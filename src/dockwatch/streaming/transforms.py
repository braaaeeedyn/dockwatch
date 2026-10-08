"""Spark transformations for station_status, as functions on DataFrames (no I/O), so they test on tiny fixtures.

Time columns:
  snapshot_ts      = feed `last_updated`: when the feed published the snapshot. This is the stream's event time:
                     it drives the watermark, windows and episodes (state is "what the station looked like then").
  last_reported_ts = when the station itself last reported; can be weeks old (used for the offline rule).
  fetched_ts       = when the producer fetched it; kafka_ts = when Kafka stored it (latency metrics).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dockwatch.streaming.episodes import DEFAULT_THRESHOLDS, Thresholds

STATUS_SCHEMA = T.StructType(
    [
        T.StructField("station_id", T.StringType()),
        T.StructField("num_bikes_available", T.IntegerType()),
        T.StructField("num_bikes_disabled", T.IntegerType()),
        T.StructField("num_docks_available", T.IntegerType()),
        T.StructField("num_docks_disabled", T.IntegerType()),
        T.StructField("num_ebikes_available", T.IntegerType()),
        T.StructField("is_installed", T.BooleanType()),
        T.StructField("is_renting", T.BooleanType()),
        T.StructField("is_returning", T.BooleanType()),
        T.StructField("last_reported", T.LongType()),
        T.StructField("feed_last_updated", T.LongType()),
        T.StructField("feed_version", T.StringType()),
        T.StructField("fetched_at", T.DoubleType()),
        T.StructField("snapshot_id", T.StringType()),
        # Added 2026-10-07 (schema evolution demo): sent by the feed from day one but dropped until now.
        T.StructField("num_scooters_available", T.IntegerType()),
        T.StructField("num_scooters_unavailable", T.IntegerType()),
    ]
)


def parse_kafka(raw: DataFrame) -> DataFrame:
    """Kafka rows (key, value, topic, partition, offset, timestamp) -> typed station_status columns + metadata."""
    parsed = raw.select(
        F.col("value").cast("string").alias("raw_json"),
        F.from_json(F.col("value").cast("string"), STATUS_SCHEMA).alias("v"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
    )
    return parsed.select(
        "v.station_id",
        "v.num_bikes_available",
        "v.num_bikes_disabled",
        "v.num_docks_available",
        "v.num_docks_disabled",
        "v.num_ebikes_available",
        "v.is_installed",
        "v.is_renting",
        "v.is_returning",
        F.timestamp_seconds("v.last_reported").alias("last_reported_ts"),
        F.timestamp_seconds("v.feed_last_updated").alias("snapshot_ts"),
        F.col("v.fetched_at").cast("timestamp").alias("fetched_ts"),
        "v.feed_version",
        "v.snapshot_id",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        "raw_json",
        # Evolved columns go last: Iceberg appends new columns at the end and Spark writes by position.
        "v.num_scooters_available",
        "v.num_scooters_unavailable",
    )


def with_state(df: DataFrame, t: Thresholds = DEFAULT_THRESHOLDS) -> DataFrame:
    """Add `capacity` and `state` using the same rule, in the same order, as episodes.classify()."""
    bikes, docks = F.col("num_bikes_available"), F.col("num_docks_available")
    capacity = bikes + F.coalesce("num_bikes_disabled", F.lit(0)) + docks + F.coalesce("num_docks_disabled", F.lit(0))
    stale = (F.unix_timestamp("snapshot_ts") - F.unix_timestamp("last_reported_ts")) > t.stale_after_s
    state = (
        F.when(~F.col("is_installed") | ~F.col("is_renting") | stale, "offline")
        .when(bikes == 0, "empty")
        .when(docks == 0, "full")
        .when((bikes <= t.low_count) | (bikes <= capacity * t.low_share), "low")
        .when((docks <= t.low_count) | (docks <= capacity * t.low_share), "high")
        .otherwise("ok")
    )
    return df.withColumn("capacity", capacity).withColumn("state", state)


def to_silver(parsed: DataFrame, t: Thresholds = DEFAULT_THRESHOLDS) -> DataFrame:
    """Valid rows only, with state; drops Kafka plumbing and the raw JSON (those stay in bronze)."""
    valid = parsed.where(F.col("station_id").isNotNull() & F.col("snapshot_ts").isNotNull())
    return with_state(valid, t).select(
        "station_id",
        "snapshot_ts",
        "last_reported_ts",
        "num_bikes_available",
        "num_bikes_disabled",
        "num_ebikes_available",
        "num_docks_available",
        "num_docks_disabled",
        "capacity",
        "is_installed",
        "is_renting",
        "is_returning",
        "state",
        "fetched_ts",
        "kafka_ts",
        F.current_timestamp().alias("processed_ts"),
        "num_scooters_available",
    )


def availability_windows(silver: DataFrame, window: str = "5 minutes") -> DataFrame:
    """Per station and tumbling window: bike counts and the share of samples in each state."""

    def share(state: str):
        return F.avg((F.col("state") == state).cast("double")).alias(f"share_{state}")

    return (
        silver.groupBy(F.window("snapshot_ts", window).alias("w"), "station_id")
        .agg(
            F.count(F.lit(1)).alias("samples"),
            F.min("num_bikes_available").alias("min_bikes"),
            F.avg("num_bikes_available").alias("avg_bikes"),
            F.max("num_bikes_available").alias("max_bikes"),
            F.avg("num_docks_available").alias("avg_docks"),
            F.max("capacity").alias("capacity"),
            share("empty"),
            share("full"),
            share("offline"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "station_id",
            "samples",
            "min_bikes",
            "avg_bikes",
            "max_bikes",
            "avg_docks",
            "capacity",
            "share_empty",
            "share_full",
            "share_offline",
        )
    )
