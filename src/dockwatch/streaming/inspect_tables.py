"""Print row counts, duplicate checks and freshness for the streaming tables. Run: python tasks.py inspect"""

from dockwatch.config import get_settings
from dockwatch.streaming.lake import build_session


def main() -> None:
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


if __name__ == "__main__":
    main()
