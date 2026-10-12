"""Partition evolution demo on a lab copy of silver (DAG partition_evolution_demo, M4a). Pure SQL and summaries.

1. re-create lake.lab.status_partition_evolution partitioned by day(snapshot_ts);
2. load day D1 of silver.station_status as one INSERT per hour (a stream's many small commits) -> stage daily_load;
3. evolve to hour(snapshot_ts) (metadata only: old files keep spec 0, new files get spec 1);
4. load D2 the same way -> stage hourly_load;
5. optimize -> stage optimized;
6. write data/maintenance/partition_evolution.json.
Production tables keep day(); this only measures what hourly would cost in files.
"""

from datetime import UTC, date, datetime, timedelta

CATALOG = "lake"
SOURCE = "silver.station_status"
TABLE = "lab.status_partition_evolution"
STAGES = ("daily_load", "hourly_load", "optimized")
MIN_HOURS = 20


def q(table: str) -> str:
    return f"{CATALOG}.{table}"


def files_sql() -> str:
    """Per data file: spec id, partition (as text, so day and hour keys compare), rows, bytes."""
    ns, name = TABLE.split(".")
    return (
        f"SELECT spec_id, json_format(CAST(partition AS json)) AS partition, record_count, file_size_in_bytes "
        f'FROM {CATALOG}.{ns}."{name}$files" WHERE content = 0'
    )


def summarize(files: list[dict]) -> dict:
    """Group $files rows by spec: {specs: {"<spec_id>": {partitions, files, rows, bytes}}, files, rows}.

    Spec ids become string keys (JSON); only specs that still have files appear.
    """
    specs: dict[str, dict] = {}
    parts: dict[str, set] = {}
    for f in files:
        sid = str(f["spec_id"])
        s = specs.setdefault(sid, {"partitions": 0, "files": 0, "rows": 0, "bytes": 0})
        parts.setdefault(sid, set()).add(str(f.get("partition")))
        s["files"] += 1
        s["rows"] += int(f["record_count"])
        s["bytes"] += int(f["file_size_in_bytes"])
    for sid, s in specs.items():
        s["partitions"] = len(parts[sid])
    ordered = {k: specs[k] for k in sorted(specs, key=int)}
    return {
        "specs": ordered,
        "files": sum(s["files"] for s in ordered.values()),
        "rows": sum(s["rows"] for s in ordered.values()),
    }


def day_stats_sql() -> str:
    return (
        "SELECT CAST(date(snapshot_ts AT TIME ZONE 'UTC') AS varchar) AS day, "
        "count(DISTINCT hour(snapshot_ts AT TIME ZONE 'UTC')) AS hours, count(*) AS n "
        f"FROM {q(SOURCE)} GROUP BY 1"
    )


def pick_days(stats: list[dict], min_hours: int = MIN_HOURS) -> list[str]:
    """The two most recent UTC days with >= min_hours distinct hours, else the two days with the most rows; oldest
    first (D1 is loaded daily, D2 hourly)."""
    full = sorted((r["day"] for r in stats if int(r["hours"]) >= min_hours), reverse=True)
    by_rows = [r["day"] for r in sorted(stats, key=lambda r: (-int(r["n"]), r["day"]))]
    days = full[:2] if len(full) >= 2 else by_rows[:2]
    if len(days) < 2:
        raise ValueError(f"{SOURCE} has fewer than two days of data: {stats}")
    return sorted(days)


def check_days(days) -> list[str]:
    out = [date.fromisoformat(str(d)).isoformat() for d in days]
    if len(out) != 2 or out[0] == out[1]:
        raise ValueError(f"days must be two different YYYY-MM-DD dates, got {days}")
    return out


def create_sql() -> list[str]:
    ns = TABLE.split(".")[0]
    return [
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{ns}",
        f"DROP TABLE IF EXISTS {q(TABLE)}",
        f"CREATE TABLE {q(TABLE)} WITH (partitioning = ARRAY['day(snapshot_ts)']) AS "
        f"SELECT * FROM {q(SOURCE)} WITH NO DATA",
    ]


def hour_inserts(day: str) -> list[str]:
    """24 INSERTs, one per UTC hour of `day` (empty hours insert nothing and commit nothing)."""
    start = datetime.combine(date.fromisoformat(day), datetime.min.time(), tzinfo=UTC)
    out = []
    for h in range(24):
        lo, hi = start + timedelta(hours=h), start + timedelta(hours=h + 1)
        out.append(
            f"INSERT INTO {q(TABLE)} SELECT * FROM {q(SOURCE)} WHERE snapshot_ts >= TIMESTAMP "
            f"'{lo:%Y-%m-%d %H:%M:%S} UTC' AND snapshot_ts < TIMESTAMP '{hi:%Y-%m-%d %H:%M:%S} UTC'"
        )
    return out


def evolve_sql() -> str:
    return f"ALTER TABLE {q(TABLE)} SET PROPERTIES partitioning = ARRAY['hour(snapshot_ts)']"


def optimize_sql(file_size_threshold: str = "100MB") -> str:
    return f"ALTER TABLE {q(TABLE)} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')"


def report(days: list[str], stages: dict[str, dict], now: datetime | None = None) -> dict:
    return {
        "table": TABLE,
        "source": SOURCE,
        "days": list(days),
        "generated_at": (now or datetime.now(UTC)).isoformat(timespec="seconds"),
        "stages": {s: stages[s] for s in STAGES},
    }
