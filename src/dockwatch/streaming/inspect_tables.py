"""Print row counts, duplicate checks and freshness for the streaming tables. Run: python tasks.py inspect

With --check (python tasks.py verify-lake) it also asserts the lake is consistent and exits 1 on any failure:
no silver duplicates, no duplicate or missing Kafka offsets in bronze, silver keeps up with bronze, and every
table has a recent Iceberg commit (proves catalog commits succeed).
"""

import argparse
import sys

from dockwatch.config import get_settings
from dockwatch.streaming.lake import build_session

KAFKA_PARTITIONS = 6  # gbfs.station_status partitions (producer setup)
TABLES = ("bronze.station_status", "silver.station_status", "silver.availability_5m", "silver.station_episodes")


def check(spark, c: str, max_age_min: int) -> list[str]:
    """Return one message per failed consistency rule (empty list = the lake is consistent)."""
    q = spark.sql
    failures: list[str] = []

    dup = q(f"SELECT count(*) - count(DISTINCT station_id, snapshot_ts) d FROM {c}.silver.station_status").first().d
    print(f"check: silver duplicates on (station_id, snapshot_ts) = {dup}")
    if dup:
        failures.append(f"silver.station_status has {dup} duplicate (station_id, snapshot_ts) rows")

    row = q(
        f"SELECT count(*) n, count(DISTINCT kafka_partition, kafka_offset) d FROM {c}.bronze.station_status"
    ).first()
    print(f"check: bronze rows = {row.n}, distinct (partition, offset) = {row.d}")
    if row.n != row.d:
        failures.append(f"bronze.station_status has {row.n - row.d} duplicate Kafka offsets")

    parts = q(
        f"""SELECT kafka_partition p, max(kafka_offset) - min(kafka_offset) + 1 - count(DISTINCT kafka_offset) gaps
            FROM {c}.bronze.station_status GROUP BY 1 ORDER BY 1"""
    ).collect()
    gaps = {r.p: r.gaps for r in parts}
    print(f"check: bronze offset gaps per partition = {gaps}")
    if sorted(gaps) != list(range(KAFKA_PARTITIONS)):
        failures.append(f"bronze.station_status has partitions {sorted(gaps)}, expected 0..{KAFKA_PARTITIONS - 1}")
    for p, n in gaps.items():
        if n:
            failures.append(f"bronze.station_status partition {p} is missing {n} offsets")

    lag = q(
        f"""SELECT (SELECT max(snapshot_ts) FROM {c}.bronze.station_status) b,
                   (SELECT max(snapshot_ts) FROM {c}.silver.station_status) s"""
    ).first()
    print(f"check: latest snapshot bronze = {lag.b}, silver = {lag.s}")
    if lag.b is None or lag.s is None or (lag.b - lag.s).total_seconds() > 5 * 60:
        failures.append(f"silver latest snapshot {lag.s} is more than 5 min behind bronze {lag.b}")

    for table in TABLES:
        age = q(
            f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(committed_at))) / 60 age_min, "
            f"count(*) n FROM {c}.{table}.snapshots"
        ).first()
        print(f"check: {table} snapshots = {age.n}, last commit {age.age_min} min ago")
        if age.age_min is None or age.age_min > max_age_min:
            failures.append(f"{table} has no Iceberg commit in the last {max_age_min} min (last: {age.age_min} min)")
    return failures


def main() -> None:
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--check", action="store_true", help="exit 1 if any consistency rule fails")
    args.add_argument("--max-age-min", type=int, default=60, help="newest Iceberg commit must be younger than this")
    opts = args.parse_args()

    s = get_settings()
    c = s.iceberg_catalog
    spark = build_session(s, "dockwatch-inspect")
    spark.sparkContext.setLogLevel("ERROR")
    q = spark.sql

    print("== row counts and freshness")
    for table, ts in (
        ("bronze.station_status", "snapshot_ts"),
        ("silver.station_status", "snapshot_ts"),
        ("silver.availability_5m", "window_start"),
        ("silver.station_episodes", "updated_ts"),
    ):
        row = q(f"SELECT count(*) n, min({ts}) lo, max({ts}) hi FROM {c}.{table}").first()
        print(f"{table:28} {row.n:>9,}  {row.lo} -> {row.hi}")

    print("== silver duplicates on (station_id, snapshot_ts) [expect 0]")
    q(f"SELECT count(*) - count(DISTINCT station_id, snapshot_ts) AS duplicates FROM {c}.silver.station_status").show()

    print("== bronze vs Kafka: distinct (partition, offset) [equal to rows means no duplicate writes]")
    q(
        f"SELECT count(*) rows_, count(DISTINCT kafka_partition, kafka_offset) distinct_offsets "
        f"FROM {c}.bronze.station_status"
    ).show()

    print("== bronze gaps: per Kafka partition, offsets must be contiguous [gaps = 0]")
    q(
        f"""SELECT kafka_partition p, min(kafka_offset) lo, max(kafka_offset) hi, count(*) n,
                   max(kafka_offset) - min(kafka_offset) + 1 - count(DISTINCT kafka_offset) gaps
            FROM {c}.bronze.station_status GROUP BY 1 ORDER BY 1"""
    ).show()

    print("== latest snapshot: stations per state")
    q(
        f"""SELECT state, count(*) stations FROM {c}.silver.station_status
            WHERE snapshot_ts = (SELECT max(snapshot_ts) FROM {c}.silver.station_status)
            GROUP BY state ORDER BY stations DESC"""
    ).show()

    print("== episodes")
    q(
        f"""SELECT kind, status, alerted, count(*) n, round(avg(duration_s) / 60, 1) avg_min,
                   round(max(duration_s) / 60, 1) max_min
            FROM {c}.silver.station_episodes GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"""
    ).show()

    print("== files per table (small-files check)")
    for table in ("bronze.station_status", "silver.station_status"):
        n = q(f"SELECT count(*) n, round(avg(file_size_in_bytes) / 1024) avg_kb FROM {c}.{table}.files").first()
        print(f"{table:28} files={n.n} avg_kb={n.avg_kb}")

    if opts.check:
        print("== verify")
        failures = check(spark, c, opts.max_age_min)
        spark.stop()
        for f in failures:
            print("FAIL:", f)
        if failures:
            sys.exit(1)
        print("ok: lake verified")


if __name__ == "__main__":
    main()
