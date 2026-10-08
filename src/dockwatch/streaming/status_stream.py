"""Streaming job: gbfs.station_status (Kafka) -> Iceberg bronze/silver tables, 5-minute availability,
empty/full episodes (MERGE INTO), alerts and current station state (Kafka).

Run inside the Spark image: python tasks.py stream   (spark-submit -m style entrypoint below)
Each output is its own streaming query with its own checkpoint, so they recover independently.
"""

import json
import logging
from collections.abc import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from dockwatch.config import get_settings
from dockwatch.streaming.episodes import EpisodeState, Thresholds, advance
from dockwatch.streaming.lake import build_session, ddl, migrate
from dockwatch.streaming.run_mode import trigger_options
from dockwatch.streaming.transforms import availability_windows, parse_kafka, to_silver

log = logging.getLogger("dockwatch.status_stream")

OUT_SCHEMA = T.StructType(
    [
        T.StructField("row_type", T.StringType()),  # state | episode | alert
        T.StructField("key", T.StringType()),
        T.StructField("ts", T.LongType()),
        T.StructField("payload", T.StringType()),  # JSON, parsed per row_type in the sink
    ]
)
STATE_SCHEMA = T.StructType(
    [
        T.StructField("kind", T.StringType()),
        T.StructField("start_ts", T.LongType()),
        T.StructField("last_ts", T.LongType()),
        T.StructField("alerted", T.BooleanType()),
    ]
)
EPISODE_SCHEMA = T.StructType(
    [
        T.StructField("episode_id", T.StringType()),
        T.StructField("station_id", T.StringType()),
        T.StructField("kind", T.StringType()),
        T.StructField("start_ts", T.LongType()),
        T.StructField("end_ts", T.LongType()),
        T.StructField("duration_s", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("alerted", T.BooleanType()),
        T.StructField("ts", T.LongType()),
    ]
)
STATE_ROW_COLS = [
    "station_id",
    "snapshot_epoch",
    "last_reported_epoch",
    "num_bikes_available",
    "num_ebikes_available",
    "num_docks_available",
    "capacity",
    "state",
]


def thresholds(s) -> Thresholds:
    return Thresholds(
        stale_after_s=s.stale_after_min * 60,
        low_count=s.low_count,
        low_share=s.low_share,
        alert_after_s=s.alert_after_min * 60,
    )


def make_episode_fn(t: Thresholds):
    def fn(key: tuple, batches: Iterator[pd.DataFrame], gstate: GroupState) -> Iterator[pd.DataFrame]:
        (station_id,) = key
        state = EpisodeState(*gstate.get) if gstate.exists else EpisodeState()
        pdf = pd.concat(list(batches), ignore_index=True)
        rows = [{"snapshot_ts": int(r.snapshot_epoch), "state": r.state} for r in pdf.itertuples(index=False)]
        events = advance(station_id, state, rows, t)
        gstate.update((state.kind, state.start_ts, state.last_ts, state.alerted))

        out = []
        latest = pdf.sort_values("snapshot_epoch").iloc[-1]
        latest_dict = {c: (None if pd.isna(latest[c]) else latest[c]) for c in STATE_ROW_COLS}
        latest_dict = {k: (v.item() if hasattr(v, "item") else v) for k, v in latest_dict.items()}
        if state.kind is not None:
            latest_dict |= {"episode_kind": state.kind, "episode_start": state.start_ts}
        out.append(("state", station_id, int(latest["snapshot_epoch"]), json.dumps(latest_dict)))
        for e in events:
            key_ = e["episode_id"] if e["type"] == "episode" else e["alert_id"]
            out.append((e["type"], key_, e["ts"], json.dumps(e)))
        yield pd.DataFrame(out, columns=["row_type", "key", "ts", "payload"])

    return fn


def write_episode_outputs(s):
    c = s.iceberg_catalog

    def sink(batch: DataFrame, batch_id: int) -> None:
        batch.persist()
        spark: SparkSession = batch.sparkSession
        kafka_opts = {"kafka.bootstrap.servers": s.spark_kafka_bootstrap}

        # Current state per station -> compacted topic (the exporter builds live.json from it).
        states = batch.where("row_type = 'state'")
        latest = states.groupBy("key").agg(F.max_by("payload", "ts").alias("value"))
        latest.selectExpr("key", "value").write.format("kafka").options(**kafka_opts).option(
            "topic", s.topic_station_state
        ).save()

        # Alerts -> Kafka, keyed by alert_id so consumers can drop re-sent duplicates after a restart.
        alerts = batch.where("row_type = 'alert'")
        alerts.selectExpr("key", "payload AS value").write.format("kafka").options(**kafka_opts).option(
            "topic", s.topic_alerts
        ).save()

        # Episodes -> Iceberg with MERGE INTO (insert new, update open ones, close finished ones).
        eps = (
            batch.where("row_type = 'episode'")
            .select(F.from_json("payload", EPISODE_SCHEMA).alias("e"))
            .select("e.*")
            .groupBy("episode_id")
            .agg(F.max_by(F.struct("*"), "ts").alias("e"))
            .select("e.*")
            .select(
                "episode_id",
                "station_id",
                "kind",
                F.timestamp_seconds("start_ts").alias("start_ts"),
                F.timestamp_seconds("end_ts").alias("end_ts"),
                "duration_s",
                "status",
                "alerted",
                F.timestamp_seconds("ts").alias("updated_ts"),
            )
        )
        eps.createOrReplaceTempView("episode_updates")
        spark.sql(
            f"""MERGE INTO {c}.silver.station_episodes t USING episode_updates s
                ON t.episode_id = s.episode_id
                WHEN MATCHED AND s.updated_ts >= t.updated_ts THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *"""
        )
        batch.unpersist()

    return sink


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    s = get_settings()
    t = thresholds(s)
    c = s.iceberg_catalog
    spark = build_session(s, "dockwatch-status-stream")
    spark.sparkContext.setLogLevel("WARN")
    for stmt in ddl(c):
        spark.sql(stmt)
    log.info("schema migrations applied: %s", migrate(spark, c) or "none")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.spark_kafka_bootstrap)
        .option("subscribe", s.topic_station_status)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100_000)
        .load()
    )
    parsed = parse_kafka(raw)
    trigger = trigger_options(s)
    ckpt = s.checkpoint_root.rstrip("/")

    def iceberg_append(df: DataFrame, name: str, table: str):
        return (
            df.writeStream.format("iceberg")
            .outputMode("append")
            .option("fanout-enabled", "true")
            .option("checkpointLocation", f"{ckpt}/{name}")
            .trigger(**trigger)
            .queryName(name)
            .toTable(f"{c}.{table}")
        )

    iceberg_append(parsed, "bronze_status", "bronze.station_status")

    silver = (
        to_silver(parsed, t)
        .withWatermark("snapshot_ts", s.watermark)
        .dropDuplicatesWithinWatermark(["station_id", "snapshot_ts"])
    )
    iceberg_append(silver, "silver_status", "silver.station_status")
    iceberg_append(availability_windows(silver), "availability_5m", "silver.availability_5m")

    episodes_in = to_silver(parsed, t).select(
        "station_id",
        F.unix_timestamp("snapshot_ts").alias("snapshot_epoch"),
        F.unix_timestamp("last_reported_ts").alias("last_reported_epoch"),
        "num_bikes_available",
        "num_ebikes_available",
        "num_docks_available",
        "capacity",
        "state",
    )
    episodes = episodes_in.groupBy("station_id").applyInPandasWithState(
        make_episode_fn(t), OUT_SCHEMA, STATE_SCHEMA, "append", GroupStateTimeout.NoTimeout
    )
    (
        episodes.writeStream.outputMode("append")
        .option("checkpointLocation", f"{ckpt}/episodes")
        .foreachBatch(write_episode_outputs(s))
        .trigger(**trigger)
        .queryName("episodes")
        .start()
    )

    log.info("started queries (%s): %s", s.trigger_mode, [q.name for q in spark.streams.active])
    if s.trigger_mode == "available_now":
        # Catch-up: every query drains Kafka up to the offsets seen at start, then stops. awaitTermination() re-raises
        # a query's failure, so the process exits non-zero if any of them failed.
        for query in list(spark.streams.active):
            query.awaitTermination()
            log.info("query %s finished", query.name)
        spark.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
