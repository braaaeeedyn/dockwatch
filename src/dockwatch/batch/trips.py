"""Bay Wheels trip history (M4b, DAG trip_history_monthly). Pure rules plus the stdlib I/O they need.

Per month: find the key in the bucket listing (the file names are irregular, so never build the URL), download the
ZIP once into data/trips/zips (reused only while its size equals the listing's), stream its one CSV member through
csv.reader (never extracted to a path), map every record onto lake.bronze.trips' columns (`map_row`; times are read
as America/Los_Angeles and stored in UTC), and replace the month in one pyiceberg transaction (delete the month,
append Arrow batches of <= 100k rows: one Iceberg commit). Trino only creates the tables, counts the month back and
records lake.metrics.trip_loads; it does no write work for trips (a Hive-catalog INSERT overflowed its frozen 192M
Metaspace in Loop I).

pyarrow / pyiceberg are in the Airflow image only, so they are imported inside the functions that use them.
"""

import csv
import hashlib
import io
import itertools
import re
import shutil
import time
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dockwatch.batch.marts import lit

BUCKET_URL = "https://s3.amazonaws.com/baywheels-data/"
KEY_RE = re.compile(r"^\d{6}-[a-z]+-tripdata(\.csv)?\.zip$")
MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
SAFE_NAME = re.compile(r"^[a-z0-9_]+$")
MAX_CSV_BYTES = 2 * 1024**3
TZ = ZoneInfo("America/Los_Angeles")
# Rows are mapped 25k at a time, each batch straight into Arrow; 4 batches (100k rows) per data file. Measured
# airflow-cli peak: 1021 MiB with a whole month in one table, 727 MiB this way (limit 1 GiB).
BATCH_ROWS = 25_000
BATCHES_PER_FILE = 4
TRIPS_TABLE = "lake.bronze.trips"
LOADS_TABLE = "lake.metrics.trip_loads"

# The columns each schema must have (legacy files carry one more varying tail column, which is ignored).
LEGACY = (
    "duration_sec", "start_time", "end_time", "start_station_id", "start_station_name", "start_station_latitude",
    "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude",
    "end_station_longitude", "bike_id", "user_type",
)  # fmt: skip
RIDE = (
    "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
    "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
)  # fmt: skip

TRIPS_COLUMNS = (
    ("trip_month", "date"),
    ("schema_version", "varchar"),
    ("source_key", "varchar"),
    ("ride_id", "varchar"),
    ("rideable_type", "varchar"),
    ("started_at", "timestamp(6) with time zone"),
    ("ended_at", "timestamp(6) with time zone"),
    ("duration_s", "bigint"),
    ("start_station_code", "varchar"),
    ("start_station_name", "varchar"),
    ("end_station_code", "varchar"),
    ("end_station_name", "varchar"),
    ("start_lat", "double"),
    ("start_lon", "double"),
    ("end_lat", "double"),
    ("end_lon", "double"),
    ("member_type", "varchar"),
    ("bike_id", "varchar"),
    ("loaded_at", "timestamp(6) with time zone"),
)
LOADS_COLUMNS = (
    ("run_id", "varchar"),
    ("trip_month", "date"),
    ("source_key", "varchar"),
    ("source_bytes", "bigint"),
    ("zip_sha256", "varchar"),
    ("schema_version", "varchar"),
    ("header", "varchar"),
    ("csv_rows", "bigint"),
    ("loaded_rows", "bigint"),
    ("bad_timestamps", "bigint"),
    ("status", "varchar"),
    ("error", "varchar"),
    ("started_at", "timestamp(6) with time zone"),
    ("finished_at", "timestamp(6) with time zone"),
)


class TripError(RuntimeError):
    """Bad input data (key, ZIP, header, counts): fails the task without retry."""


# ---------------------------------------------------------------- months and the listing


def check_month(month: str) -> str:
    if not MONTH_RE.fullmatch(month or "") or not 1 <= int(month[5:]) <= 12:
        raise TripError(f"month must be YYYY-MM, got {month!r}")
    return month


def month_of(param: str | None, interval_start) -> str:
    """The `month` param when set, else the month of the run's data_interval_start."""
    return check_month(param) if param else f"{interval_start:%Y-%m}"


def first_day(month: str) -> date:
    return date(int(month[:4]), int(month[5:7]), 1)


def parse_listing(xml: bytes) -> dict[str, int]:
    """S3 ListBucket XML -> {key: size}."""
    root = ET.fromstring(xml)
    ns = {"s": root.tag.split("}")[0][1:]} if root.tag.startswith("{") else {"s": ""}
    p = "s:" if ns["s"] else ""
    if (root.findtext(f"{p}IsTruncated", "false", ns) or "").lower() == "true":
        # ponytail: one page (1000 keys) holds the ~110 files; paginate with `marker` if it ever grows past that
        raise TripError("bucket listing is truncated (more than one page)")
    return {
        c.findtext(f"{p}Key", "", ns): int(c.findtext(f"{p}Size", "0", ns)) for c in root.findall(f"{p}Contents", ns)
    }


def fetch_listing(url: str = BUCKET_URL) -> dict[str, int]:
    with urllib.request.urlopen(url, timeout=60) as r:
        return parse_listing(r.read())


def month_key(listing: dict[str, int], month: str) -> str | None:
    """The month's ZIP key, found by its YYYYMM- prefix whatever the rest of the name."""
    prefix = month.replace("-", "") + "-"
    keys = [k for k in listing if k.startswith(prefix) and KEY_RE.fullmatch(k)]
    if len(keys) > 1:
        raise TripError(f"{month}: more than one ZIP in the bucket: {keys}")
    return keys[0] if keys else None


def month_status(listing: dict[str, int], month: str) -> str:
    """published | missing (absent while a later month exists, e.g. 2020-04) | pending (not published yet)."""
    if month_key(listing, month):
        return "published"
    this = month.replace("-", "")
    later = [k[:6] for k in listing if re.match(r"^\d{6}-", k) and k[:6] > this]
    return "missing" if later else "pending"


# ---------------------------------------------------------------- download


def cached_zip(cache: Path, key: str, size: int) -> Path | None:
    p = cache / key
    return p if p.is_file() and p.stat().st_size == size else None


def download(key: str, size: int, cache: Path, opener=urllib.request.urlopen) -> Path:
    """data/trips/zips/<key>: reused when its size equals the listing's; else downloaded to a temp file + rename."""
    if not KEY_RE.fullmatch(key):
        raise TripError(f"unexpected key {key!r}")
    hit = cached_zip(cache, key, size)
    if hit:
        print(f"cached {hit} ({size} bytes)", flush=True)
        return hit
    cache.mkdir(parents=True, exist_ok=True)
    tmp = cache / f".{key}.part"
    with opener(BUCKET_URL + key, timeout=300) as r, tmp.open("wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)
    if tmp.stat().st_size != size:
        got = tmp.stat().st_size
        tmp.unlink()
        raise TripError(f"{key}: downloaded {got} bytes, the listing says {size}")
    return tmp.replace(cache / key)


def sha256(path: Path) -> str:
    d = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            d.update(chunk)
    return d.hexdigest()


def csv_member(zf: zipfile.ZipFile) -> zipfile.ZipInfo:
    members = [i for i in zf.infolist() if i.filename.lower().endswith(".csv") and "__MACOSX" not in i.filename]
    if len(members) != 1:
        raise TripError(f"expected one .csv member, found {[i.filename for i in members]}")
    if members[0].file_size > MAX_CSV_BYTES:
        raise TripError(f"{members[0].filename}: {members[0].file_size} bytes uncompressed > {MAX_CSV_BYTES}")
    return members[0]


# ---------------------------------------------------------------- schema and mapping


def schema_of(header: list[str]) -> str:
    """legacy | ride; refuses unsafe names (a trust-boundary check) and headers missing a required column."""
    bad = [h for h in header if not SAFE_NAME.fullmatch(h)]
    if bad or not header:
        raise TripError(f"unsafe or empty header column names {bad}")
    if len(set(header)) != len(header):
        raise TripError(f"duplicate header columns {header}")
    have = set(header)
    if have >= set(RIDE):
        return "ride"
    if have >= set(LEGACY):
        return "legacy"
    raise TripError(f"header lacks a required column: ride misses {sorted(set(RIDE) - have)}, legacy misses "
                    f"{sorted(set(LEGACY) - have)}")  # fmt: skip


def local_time(text: str | None) -> datetime | None:
    """Source text in Pacific local time (e.g. `2019-01-31 17:57:44.6130`) -> UTC; None if empty or unparseable.
    fold=0: an ambiguous 01:30 is the first (PDT) one; a non-existent 02:30 lands on 10:30Z (= 03:30 PDT)."""
    try:
        return datetime.fromisoformat(text).replace(tzinfo=TZ).astimezone(UTC) if text else None
    except ValueError:
        return None


def _num(text: str | None, typ=float):
    try:
        return typ(text) if text else None
    except ValueError:
        return None


MEMBER = {"Subscriber": "member", "Customer": "casual"}
# TRIPS_COLUMNS[3:-1]: what map_row returns; the other four are the month's constants (month_table adds them)
ROW_COLUMNS = tuple(n for n, _ in TRIPS_COLUMNS[3:-1])


def map_row(schema: str, index: dict[str, int], row: list[str]) -> tuple:
    """One CSV record -> the ROW_COLUMNS values of bronze.trips. Empty -> None."""

    def v(col: str) -> str | None:
        i = index[col]
        return (row[i].strip() or None) if i < len(row) else None

    if schema == "legacy":
        return (
            None, None, local_time(v("start_time")), local_time(v("end_time")), _num(v("duration_sec"), int),
            v("start_station_id"), v("start_station_name"), v("end_station_id"), v("end_station_name"),
            _num(v("start_station_latitude")), _num(v("start_station_longitude")), _num(v("end_station_latitude")),
            _num(v("end_station_longitude")), MEMBER.get(v("user_type") or ""), v("bike_id"),
        )  # fmt: skip
    if schema == "ride":
        start, end = local_time(v("started_at")), local_time(v("ended_at"))
        member = v("member_casual")
        return (
            v("ride_id"), v("rideable_type"), start, end, int((end - start).total_seconds()) if start and end else None,
            v("start_station_id"), v("start_station_name"), v("end_station_id"), v("end_station_name"),
            _num(v("start_lat")), _num(v("start_lng")), _num(v("end_lat")), _num(v("end_lng")),
            member.lower() if member else None, None,
        )  # fmt: skip
    raise TripError(f"unknown schema {schema!r}")


def month_batches(zip_path: Path, stats: dict, size: int = BATCH_ROWS) -> Iterator[list[tuple]]:
    """Stream the ZIP's one CSV member and yield mapped rows in lists of <= size. Fills `stats`: header, schema,
    csv_rows (records read; blank lines are not records) and bad_timestamps (a start or end time that did not parse)."""
    with zipfile.ZipFile(zip_path) as zf, zf.open(csv_member(zf)) as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        header = next(reader, [])
        stats.update(header=header, csv_rows=0, bad_timestamps=0)
        schema = stats["schema"] = schema_of(header)
        index = {h: i for i, h in enumerate(header)}
        batch: list[tuple] = []
        for rec in reader:
            if not rec:
                continue
            row = map_row(schema, index, rec)
            stats["csv_rows"] += 1
            if row[2] is None or row[3] is None:
                stats["bad_timestamps"] += 1
            batch.append(row)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch


# ---------------------------------------------------------------- Arrow and pyiceberg (Airflow image only)


def arrow_schema(columns):
    import pyarrow as pa

    types = {
        "date": pa.date32(),
        "varchar": pa.string(),
        "bigint": pa.int64(),
        "integer": pa.int32(),
        "double": pa.float64(),
        "timestamp(6) with time zone": pa.timestamp("us", tz="UTC"),
    }
    return pa.schema([(n, types[t]) for n, t in columns])


def month_table(month: str, schema: str, key: str, loaded_at: datetime, batches):
    """Mapped row lists -> one Arrow table with bronze.trips' columns and types (one record batch per list)."""
    import pyarrow as pa

    sch = arrow_schema(TRIPS_COLUMNS)
    head = (first_day(month), schema, key)
    out = []
    for rows in batches:
        n = len(rows)
        cols = [[x] * n for x in head] + [list(c) for c in zip(*rows, strict=True)] + [[loaded_at] * n]
        arrays = [pa.array(c, f.type) for c, f in zip(cols, sch, strict=True)]
        out.append(pa.RecordBatch.from_arrays(arrays, schema=sch))
    return pa.Table.from_batches(out, schema=sch)


def rows_table(columns, rows: list[tuple]):
    """Row tuples (in `columns` order) -> one Arrow table with those columns' types."""
    import pyarrow as pa

    sch = arrow_schema(columns)
    cols = list(zip(*rows, strict=True)) if rows else [()] * len(sch)
    return pa.Table.from_arrays([pa.array(list(c), f.type) for c, f in zip(cols, sch, strict=True)], schema=sch)


def catalog():
    """The `lake` Iceberg REST catalog through pyiceberg, data files on SeaweedFS (settings / env as in compose)."""
    from pyiceberg.catalog import load_catalog

    from dockwatch.config import get_settings

    s = get_settings()
    return load_catalog(
        "lake",
        type="rest",
        uri=s.iceberg_rest_uri,
        **{
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key,
            "s3.region": s.s3_region,
        },
    )


def overwrite(table: str, data) -> None:
    """One Iceberg commit that replaces the whole table with `data` (bronze.station_information)."""
    catalog().load_table(table.removeprefix("lake.")).overwrite(data)


def replace_month(month: str, tables, before_commit) -> None:
    """One Iceberg commit (one pyiceberg transaction) that deletes the month from bronze.trips (an identity partition,
    so metadata only) and appends `tables` one Arrow table at a time, so only one batch is in memory (a whole 600k-row
    month in one table took airflow-cli to its 1 GiB limit). before_commit() may raise: nothing is committed then
    (the data files already written stay as unreferenced orphans). Readers see the old month or the new one."""
    from pyiceberg.expressions import EqualTo

    tbl = catalog().load_table(TRIPS_TABLE.removeprefix("lake."))
    with tbl.transaction() as tx:
        tx.delete(EqualTo("trip_month", f"{check_month(month)}-01"))
        for t in tables:
            tx.append(t)
        before_commit()


def load_month(execute, run_id: str, month: str, key: str, zip_path: Path) -> dict:
    """Map the cached ZIP batch by batch, replace the month in bronze.trips (one pyiceberg commit), count it back
    through Trino and record one lake.metrics.trip_loads row (ok or error) either way. Bad timestamps or a count
    mismatch raise TripError; bad timestamps are checked before the commit, so bad data never lands."""
    t0 = time.monotonic()
    stats: dict = {}
    row = {"run_id": run_id, "trip_month": first_day(month), "source_key": key, "source_bytes": zip_path.stat().st_size,
           "zip_sha256": sha256(zip_path), "status": "error", "started_at": datetime.now(UTC)}  # fmt: skip
    for sql in create_sql():
        execute(sql)
    try:
        batches = month_batches(zip_path, stats)
        first = next(batches, None)
        if not first:
            raise TripError(f"{key}: no CSV records")
        rows = itertools.chain([first], batches)

        def tables():  # lazily: at most one Python batch and one file's Arrow table in memory
            while (t := month_table(month, stats["schema"], key, row["started_at"],
                                    itertools.islice(rows, BATCHES_PER_FILE))).num_rows:  # fmt: skip
                yield t

        def before_commit() -> None:
            print(f"{month}: {stats['csv_rows']} records written in {time.monotonic() - t0:.1f} s", flush=True)
            if stats["bad_timestamps"]:
                raise TripError(f"{month}: {stats['bad_timestamps']} records with a start or end time that won't parse")

        replace_month(month, tables(), before_commit)
        print(f"{month}: committed in {time.monotonic() - t0:.1f} s", flush=True)
        row["loaded_rows"] = int(execute(count_sql(month))[0]["n"])
        if row["loaded_rows"] != stats["csv_rows"]:
            raise TripError(f"{month}: bronze.trips has {row['loaded_rows']} rows, the CSV {stats['csv_rows']}")
        row["status"] = "ok"
    except Exception as e:
        row["error"] = f"{type(e).__name__}: {e}"[:2000]
        raise
    finally:
        row.update(schema_version=stats.get("schema"), header=",".join(stats.get("header") or []),
                   csv_rows=stats.get("csv_rows"), bad_timestamps=stats.get("bad_timestamps"),
                   finished_at=datetime.now(UTC))  # fmt: skip
        execute(insert_load_sql(row))
        print(f"{month}: {row['status']} in {time.monotonic() - t0:.1f} s", flush=True)
    return row


# ---------------------------------------------------------------- Trino SQL


def create_sql() -> list[str]:
    trips = ", ".join(f"{n} {t}" for n, t in TRIPS_COLUMNS)
    loads = ", ".join(f"{n} {t}" for n, t in LOADS_COLUMNS)
    return [
        "CREATE SCHEMA IF NOT EXISTS lake.bronze",
        "CREATE SCHEMA IF NOT EXISTS lake.metrics",
        f"CREATE TABLE IF NOT EXISTS {TRIPS_TABLE} ({trips}) WITH (partitioning = ARRAY['trip_month'])",
        f"CREATE TABLE IF NOT EXISTS {LOADS_TABLE} ({loads})",
    ]


def count_sql(month: str) -> str:
    return f"SELECT count(*) AS n FROM {TRIPS_TABLE} WHERE trip_month = DATE '{check_month(month)}-01'"


def insert_load_sql(row: dict) -> str:
    names = [n for n, _ in LOADS_COLUMNS]
    return f"INSERT INTO {LOADS_TABLE} ({', '.join(names)}) VALUES ({', '.join(lit(row.get(n)) for n in names)})"
