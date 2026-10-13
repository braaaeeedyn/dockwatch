"""marts_daily (M4b): the station dimension from the GBFS raw archive, the quality gate over dbt's run_results.json,
and the atomic insights export. Pure rules plus file I/O; Trino and boto3 come in through dag_tasks.py.

Also the one SQL literal helper every M4b SQL builder uses (values never reach SQL any other way).
"""

import json
import math
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from dockwatch.batch.maintenance import literal

MARTS = ("mart_station_availability_daily", "mart_rebalancing_need", "mart_trips_vs_availability")
# insights file name -> (mart, ORDER BY); the daily file holds the newest DAILY_DATES service dates
INSIGHTS = {
    "station_availability_daily": ("mart_station_availability_daily", "service_date DESC, station_id"),
    "rebalancing_need": ("mart_rebalancing_need", "need_rank, station_id"),
    "trips_vs_availability": ("mart_trips_vs_availability", "station_id, hour_of_day"),
}
DAILY_DATES = 30
SIZE_BUDGET = 2_000_000
FLOAT_DIGITS = 3  # full doubles put trips_vs_availability.json at 2.3 MB; 4 digits can still pass 2 MB
STATION_TABLE = "lake.bronze.station_information"
STATION_COLUMNS = (
    ("station_id", "varchar"),
    ("short_name", "varchar"),
    ("name", "varchar"),
    ("region_id", "varchar"),
    ("lat", "double"),
    ("lon", "double"),
    ("capacity", "integer"),
    ("feed_last_updated", "timestamp(6) with time zone"),
    ("loaded_at", "timestamp(6) with time zone"),
)
BAD_STATUSES = ("error", "fail", "skipped", "runtime error")


class GateError(RuntimeError):
    pass


def lit(value) -> str:
    """A Trino SQL literal: NULL, numbers, DATE / TIMESTAMP, else a quoted string with quotes doubled."""
    if isinstance(value, float):
        return "nan()" if math.isnan(value) else repr(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return f"DATE '{value.isoformat()}'"
    return literal(value)


# ---------------------------------------------------------------- station dimension


def station_rows(doc: dict, loaded_at: datetime) -> list[tuple]:
    """A GBFS station_information document -> bronze.station_information rows (STATION_COLUMNS order), one per
    station_id (the first wins); rows the shared model refuses are skipped."""
    from pydantic import ValidationError

    from dockwatch.gbfs.models import StationInformation

    fed = datetime.fromtimestamp(int(doc.get("last_updated") or 0), tz=UTC)
    out: dict[str, tuple] = {}
    for raw in (doc.get("data") or {}).get("stations") or []:
        try:
            s = StationInformation.model_validate(raw)
        except ValidationError:
            continue
        out.setdefault(s.station_id, (s.station_id, s.short_name, s.name, s.region_id, s.lat, s.lon, s.capacity,
                                      fed, loaded_at))  # fmt: skip
    return list(out.values())


def create_station_sql() -> list[str]:
    cols = ", ".join(f"{n} {t}" for n, t in STATION_COLUMNS)
    return ["CREATE SCHEMA IF NOT EXISTS lake.bronze", f"CREATE TABLE IF NOT EXISTS {STATION_TABLE} ({cols})"]


# ---------------------------------------------------------------- quality gate


def quality_gate(run_results: dict, mart_rows: dict, daily_last: date | None, silver_last: date | None) -> dict:
    """The gate document (data/dbt/quality_gate.json). `passed` is False on any dbt node error / fail / skipped, a
    mart missing or with 0 rows, or a daily mart older than silver's newest Pacific day - 1."""
    results = run_results.get("results") or []
    problems = [f"{r.get('unique_id')}: {r.get('status')}" for r in results if r.get("status") in BAD_STATUSES]
    tests = [r for r in results if str(r.get("unique_id", "")).startswith("test.")]
    for m in MARTS:
        if not mart_rows.get(m):
            problems.append(f"{m}: {mart_rows.get(m, 'missing')} rows")
    if silver_last and (daily_last is None or daily_last < silver_last - timedelta(days=1)):
        problems.append(f"mart_station_availability_daily ends {daily_last}, silver reaches {silver_last}")
    return {
        "passed": not problems,
        "problems": problems,
        "tests": len(tests),
        "tests_passed": sum(1 for r in tests if r.get("status") == "pass"),
        "warnings": sum(1 for r in results if r.get("status") == "warn"),
        "models": sum(1 for r in results if str(r.get("unique_id", "")).startswith("model.")),
        "mart_rows": mart_rows,
        "daily_last": daily_last.isoformat() if daily_last else None,
        "silver_last": silver_last.isoformat() if silver_last else None,
        "dbt_generated_at": (run_results.get("metadata") or {}).get("generated_at"),
    }


# ---------------------------------------------------------------- insights export


def plain(value):
    """A Trino result value as JSON: dates and timestamps as ISO text, decimals and floats rounded to FLOAT_DIGITS."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal | float):
        return round(float(value), FLOAT_DIGITS)
    return value


def write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def export_insights(out: Path, tables: dict[str, tuple[list[str], list[list]]], manifest: dict) -> dict:
    """Write web/data/insights/<name>.json for every (columns, rows) table, then manifest.json last, each through a
    temp file + os.replace. Every document is checked against SIZE_BUDGET before anything is written."""
    now = datetime.now(UTC).isoformat(timespec="seconds")
    docs = {}
    for name, (columns, rows) in tables.items():
        doc = {"generated_at": now, "source": f"lake.marts.{INSIGHTS[name][0]}", "columns": columns,
               "rows": [[plain(v) for v in r] for r in rows]}  # fmt: skip
        docs[name] = json.dumps(doc, separators=(",", ":")) + "\n"
        size = len(docs[name].encode("utf-8"))
        if size > SIZE_BUDGET:
            raise GateError(f"{name}.json would be {size} bytes > {SIZE_BUDGET}")
    out.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, text in docs.items():
        write_atomic(out / f"{name}.json", text)
        files[name] = {"rows": len(tables[name][1]), "bytes": len(text.encode("utf-8"))}
    man = {"generated_at": now, **manifest, "files": files}
    write_atomic(out / "manifest.json", json.dumps(man, indent=1) + "\n")
    return man
