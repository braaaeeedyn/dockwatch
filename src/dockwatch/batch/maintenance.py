"""Iceberg table maintenance through Trino (DAG iceberg_maintenance, M4a). Pure SQL builders and rules.

Per table, in order: measure, optimize (rewrite small data files), expire_snapshots, remove_orphan_files, measure
again. Retention below one day is refused here (the Trino catalog sets the same floor). A changed row count fails
the table. The rows go to lake.metrics.maintenance_runs, one per table per run.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

CATALOG = "lake"
STREAM_TABLES = ("bronze.station_status", "silver.station_status", "silver.availability_5m", "silver.station_episodes")
OPS_TABLES = ("ops.stations", "ops.docks", "ops.vans", "ops.rebalancing_jobs", "ops.maintenance_tickets")
TABLES = STREAM_TABLES + OPS_TABLES
# Loop K: the M4b and metrics tables too. They may not exist yet (a fresh install, pipeline_minutely before its first
# run): a missing batch table gets a `skipped` row. metrics.maintenance_runs is left out: the run writes it last.
BATCH_TABLES = (
    "bronze.trips",
    "bronze.station_information",
    "metrics.trip_loads",
    "marts.mart_station_availability_daily",
    "marts.mart_rebalancing_need",
    "marts.mart_trips_vs_availability",
    "metrics.pipeline_minutely",
)
ALL_TABLES = TABLES + BATCH_TABLES
# Identity-partitioned tables are optimized one partition per run, the one with the most files: one optimize of all of
# bronze.trips (13 months, 64 files, 230 MB) took Trino to 2,024 MiB of its 2 GiB limit (Loop K spike, 1,273 MiB
# without it). One partition keeps the writers small and the run at one `replace` commit; a 12-month reload is
# compacted over the next daily runs.
IDENTITY_PARTITIONS = {"bronze.trips": "trip_month"}  # table -> its DATE identity partition column
METRICS_TABLE = f"{CATALOG}.metrics.maintenance_runs"
MIN_RETENTION_S = 86400

DEFAULTS = {"file_size_threshold": "100MB", "expire_older_than": "1d", "orphan_older_than": "1d"}

METRICS_COLUMNS = (
    ("run_id", "varchar"),
    ("table_name", "varchar"),
    ("started_at", "timestamp(6) with time zone"),
    ("finished_at", "timestamp(6) with time zone"),
    ("status", "varchar"),
    ("error", "varchar"),
    ("files_before", "bigint"),
    ("files_after", "bigint"),
    ("bytes_before", "bigint"),
    ("bytes_after", "bigint"),
    ("rows_before", "bigint"),
    ("rows_after", "bigint"),
    ("snapshots_before", "bigint"),
    ("snapshots_after", "bigint"),
    ("delete_files_before", "bigint"),
    ("delete_files_after", "bigint"),
    ("orphan_files_removed", "bigint"),
    ("partitions_after", "bigint"),
)

Execute = Callable[[str], list[dict]]


class MaintenanceError(RuntimeError):
    pass


@dataclass(frozen=True)
class Measure:
    files: int
    bytes: int
    rows: int
    snapshots: int
    delete_files: int
    partitions: int


# ---------------------------------------------------------------- retention / names


def duration_seconds(text: str) -> float:
    """'1d' / '36h' / '90m' / '1.5d' (Trino duration syntax) -> seconds."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(s|m|h|d)\s*", str(text))
    if not m:
        raise MaintenanceError(f"not a duration: {text!r} (use e.g. 1d, 36h)")
    return float(m.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]


def check_retention(name: str, value: str) -> str:
    """Refuse retention below one day: Spark may hold files younger than that, and the lake is days old."""
    if duration_seconds(value) < MIN_RETENTION_S:
        raise MaintenanceError(f"{name}={value!r} is below the 1d retention floor; refusing")
    return value


def check_size(value: str) -> str:
    if not re.fullmatch(r"\d+(\.\d+)?(B|kB|MB|GB)", str(value)):
        raise MaintenanceError(f"file_size_threshold={value!r}: use e.g. 100MB")
    return value


def qualified(table: str) -> str:
    """'bronze.station_status' -> 'lake.bronze.station_status' (refuses anything but two plain identifiers)."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", table):
        raise MaintenanceError(f"bad table name {table!r}")
    return f"{CATALOG}.{table}"


def meta(table: str, kind: str) -> str:
    """Trino metadata table, e.g. lake.bronze."station_status$files"."""
    ns, name = qualified(table).split(".")[1:]
    return f'{CATALOG}.{ns}."{name}${kind}"'


# ---------------------------------------------------------------- SQL


def params_of(conf: dict | None) -> dict:
    p = {**DEFAULTS, **{k: v for k, v in (conf or {}).items() if k in DEFAULTS and v is not None}}
    check_size(p["file_size_threshold"])
    check_retention("expire_older_than", p["expire_older_than"])
    check_retention("orphan_older_than", p["orphan_older_than"])
    return p


def statements(table: str, params: dict | None = None) -> list[str]:
    """optimize, expire_snapshots, remove_orphan_files for one table, with the (checked) parameters."""
    p = params_of(params)
    t = qualified(table)
    return [
        f"ALTER TABLE {t} EXECUTE optimize(file_size_threshold => '{p['file_size_threshold']}')",
        f"ALTER TABLE {t} EXECUTE expire_snapshots(retention_threshold => '{p['expire_older_than']}')",
        f"ALTER TABLE {t} EXECUTE remove_orphan_files(retention_threshold => '{p['orphan_older_than']}')",
    ]


def measure_sql(table: str) -> str:
    return (
        "SELECT "
        f"(SELECT count(*) FROM {meta(table, 'files')}) AS files, "
        f"(SELECT coalesce(sum(file_size_in_bytes), 0) FROM {meta(table, 'files')}) AS bytes, "
        f"(SELECT count(*) FROM {meta(table, 'files')} WHERE content <> 0) AS delete_files, "
        f"(SELECT count(*) FROM {qualified(table)}) AS rows, "
        f"(SELECT count(*) FROM {meta(table, 'snapshots')}) AS snapshots, "
        f"(SELECT count(*) FROM {meta(table, 'partitions')}) AS partitions"
    )


def exists_sql(table: str) -> str:
    ns, name = qualified(table).split(".")[1:]
    return (
        f"SELECT count(*) AS n FROM {CATALOG}.information_schema.tables "
        f"WHERE table_schema = '{ns}' AND table_name = '{name}'"
    )


def busiest_partition_sql(table: str) -> str:
    """The identity partition of `table` with the most data files, if any has more than one."""
    column = IDENTITY_PARTITIONS[table]
    return (
        f"SELECT CAST(partition.{column} AS varchar) AS value, file_count FROM {meta(table, 'partitions')} "
        "WHERE file_count > 1 ORDER BY file_count DESC, 1 LIMIT 1"
    )


def optimize_partition_sql(optimize: str, table: str, value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
        raise MaintenanceError(f"{table}: unexpected partition value {value!r}")
    return f"{optimize} WHERE {IDENTITY_PARTITIONS[table]} = DATE '{value}'"


def measure(execute: Execute, table: str) -> Measure:
    r = execute(measure_sql(table))[0]
    return Measure(
        files=int(r["files"]),
        bytes=int(r["bytes"]),
        rows=int(r["rows"]),
        snapshots=int(r["snapshots"]),
        delete_files=int(r["delete_files"]),
        partitions=int(r["partitions"]),
    )


def create_metrics_sql() -> list[str]:
    cols = ", ".join(f"{n} {t}" for n, t in METRICS_COLUMNS)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.metrics",
        f"CREATE TABLE IF NOT EXISTS {METRICS_TABLE} ({cols})",
    ]


def literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        ts = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
        return f"TIMESTAMP '{ts} UTC'"
    return "'" + str(value).replace("'", "''") + "'"


def insert_sql(rows: list[dict]) -> str:
    """One INSERT for all of a run's rows (one Iceberg commit)."""
    if not rows:
        raise MaintenanceError("no maintenance rows to record")
    names = [n for n, _ in METRICS_COLUMNS]
    values = ",\n".join("(" + ", ".join(literal(r.get(n)) for n in names) + ")" for r in rows)
    return f"INSERT INTO {METRICS_TABLE} ({', '.join(names)}) VALUES\n{values}"


# ---------------------------------------------------------------- rows


def check_rows(table: str, before: Measure, after: Measure) -> None:
    if before.rows != after.rows:
        raise MaintenanceError(f"{table}: row count changed {before.rows} -> {after.rows} during maintenance")


def row(
    run_id: str,
    table: str,
    started_at: datetime,
    finished_at: datetime,
    before: Measure | None,
    after: Measure | None,
    orphan_files_removed: int | None = None,
    error: str | None = None,
) -> dict:
    """The lake.metrics.maintenance_runs row for one table."""
    b = before.__dict__ if before else {}
    a = after.__dict__ if after else {}
    return {
        "run_id": run_id,
        "table_name": table,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "error" if error else "ok",
        "error": error,
        "files_before": b.get("files"),
        "files_after": a.get("files"),
        "bytes_before": b.get("bytes"),
        "bytes_after": a.get("bytes"),
        "rows_before": b.get("rows"),
        "rows_after": a.get("rows"),
        "snapshots_before": b.get("snapshots"),
        "snapshots_after": a.get("snapshots"),
        "delete_files_before": b.get("delete_files"),
        "delete_files_after": a.get("delete_files"),
        "orphan_files_removed": orphan_files_removed,
        "partitions_after": a.get("partitions"),
    }


def orphan_count(result: list[dict]) -> int | None:
    """Files deleted by remove_orphan_files. Trino 483 returns metric rows ({metric_name: deleted_files_count,
    metric_value: n}); None if it returned none."""
    for r in result or []:
        if r.get("metric_name") == "deleted_files_count" and r.get("metric_value") is not None:
            return int(r["metric_value"])
    return None


def maintain(execute: Execute, run_id: str, table: str, params: dict | None = None, now=None) -> dict:
    """Run the four steps on one table and return its row. Raises MaintenanceError (row attached) on a changed
    row count; other errors propagate (the DAG task records them as status='error'). A BATCH_TABLES table that does
    not exist yet returns a `skipped` row; a missing core table fails as before (measure raises). An
    IDENTITY_PARTITIONS table optimizes only its partition with the most files (none if every partition has one)."""
    clock = now or (lambda: datetime.now(UTC))
    sql = statements(table, params)
    started = clock()
    if table in BATCH_TABLES and not int(execute(exists_sql(table))[0]["n"]):
        return {**row(run_id, table, started, clock(), None, None), "status": "skipped"}
    before = measure(execute, table)
    if table in IDENTITY_PARTITIONS:
        busiest = execute(busiest_partition_sql(table))
        if busiest:
            execute(optimize_partition_sql(sql[0], table, busiest[0]["value"]))
    else:
        execute(sql[0])
    execute(sql[1])
    orphans = orphan_count(execute(sql[2]))
    after = measure(execute, table)
    try:
        check_rows(table, before, after)
    except MaintenanceError as e:
        e.row = row(run_id, table, started, clock(), before, after, orphans, error=str(e))
        raise
    return row(run_id, table, started, clock(), before, after, orphans)
