"""SparkSession wiring for the Iceberg catalog, and the DDL for every table the streaming jobs write."""

from pyspark.sql import SparkSession


def iceberg_conf(s) -> dict[str, str]:
    cat = f"spark.sql.catalog.{s.iceberg_catalog}"
    conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": "6",  # matches the topic's partitions; data is small
    }
    if s.target == "local":
        conf |= {
            f"{cat}.type": "rest",
            f"{cat}.uri": s.iceberg_rest_uri,
            f"{cat}.warehouse": f"s3://{s.s3_bucket}/warehouse/",
            f"{cat}.s3.endpoint": s.iceberg_s3_endpoint,
            f"{cat}.s3.path-style-access": "true",
        }
    else:
        conf |= {f"{cat}.type": "glue", f"{cat}.warehouse": f"s3://{s.s3_bucket}/warehouse/"}
    return conf


def build_session(s, app: str) -> SparkSession:
    builder = SparkSession.builder.appName(app)
    for key, value in iceberg_conf(s).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


TABLE_PROPS = (
    "TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true', "
    "'write.metadata.previous-versions-max'='100')"
)


# Schema evolution: columns added after a table was first created. Tables are created with the original columns
# (above) and evolved in place by migrate(), so a fresh install and an old one go through the same history.
EVOLUTIONS: list[tuple[str, str, str]] = [
    # (table, column, type) — 2026-10-07: scooter counts were in the feed all along but not captured.
    ("bronze.station_status", "num_scooters_available", "int"),
    ("bronze.station_status", "num_scooters_unavailable", "int"),
    ("silver.station_status", "num_scooters_available", "int"),
]


def migrate(spark: SparkSession, c: str) -> list[str]:
    """Add any missing evolved columns (metadata-only in Iceberg: no data files are rewritten)."""
    done = []
    for table, column, type_ in EVOLUTIONS:
        if column not in spark.table(f"{c}.{table}").columns:
            spark.sql(f"ALTER TABLE {c}.{table} ADD COLUMN {column} {type_}")
            done.append(f"{table}.{column}")
    return done


def ddl(c: str) -> list[str]:
    """CREATE statements, idempotent. `c` is the catalog name."""
    return [
        f"CREATE NAMESPACE IF NOT EXISTS {c}.bronze",
        f"CREATE NAMESPACE IF NOT EXISTS {c}.silver",
        f"""CREATE TABLE IF NOT EXISTS {c}.bronze.station_status (
            station_id string, num_bikes_available int, num_bikes_disabled int, num_docks_available int,
            num_docks_disabled int, num_ebikes_available int, is_installed boolean, is_renting boolean,
            is_returning boolean, last_reported_ts timestamp, snapshot_ts timestamp, fetched_ts timestamp,
            feed_version string, snapshot_id string, kafka_topic string, kafka_partition int, kafka_offset bigint,
            kafka_ts timestamp, raw_json string)
            USING iceberg PARTITIONED BY (days(snapshot_ts)) {TABLE_PROPS}""",
        f"""CREATE TABLE IF NOT EXISTS {c}.silver.station_status (
            station_id string, snapshot_ts timestamp, last_reported_ts timestamp, num_bikes_available int,
            num_bikes_disabled int, num_ebikes_available int, num_docks_available int, num_docks_disabled int,
            capacity int, is_installed boolean, is_renting boolean, is_returning boolean, state string,
            fetched_ts timestamp, kafka_ts timestamp, processed_ts timestamp)
            USING iceberg PARTITIONED BY (days(snapshot_ts)) {TABLE_PROPS}""",
        f"""CREATE TABLE IF NOT EXISTS {c}.silver.availability_5m (
            window_start timestamp, window_end timestamp, station_id string, samples bigint, min_bikes int,
            avg_bikes double, max_bikes int, avg_docks double, capacity int, share_empty double,
            share_full double, share_offline double)
            USING iceberg PARTITIONED BY (days(window_start)) {TABLE_PROPS}""",
        f"""CREATE TABLE IF NOT EXISTS {c}.silver.station_episodes (
            episode_id string, station_id string, kind string, start_ts timestamp, end_ts timestamp,
            duration_s bigint, status string, alerted boolean, updated_ts timestamp)
            USING iceberg PARTITIONED BY (days(start_ts)) {TABLE_PROPS}""",
        # Sort order for compaction (M4) and for readers; streaming appends don't re-sort.
        f"ALTER TABLE {c}.silver.station_status WRITE ORDERED BY station_id, snapshot_ts",
    ]
