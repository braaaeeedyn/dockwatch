"""Apply Debezium change events from Kafka (ops.public.*) to Iceberg lake.ops.* with MERGE INTO. Spark, Python 3.10.

Run through the host runner (python tasks.py cdc-catchup / cdc-reset), which starts the `cdc-apply` compose service:

    spark-submit .../cdc/apply.py             availableNow from checkpoint /checkpoints/cdc/ops, then exit
    spark-submit .../cdc/apply.py --replay    fresh checkpoint from earliest (deleted afterwards): proves idempotence
    spark-submit .../cdc/apply.py --reset     DROP TABLE lake.ops.* PURGE and delete /checkpoints/cdc

Per micro-batch and table: create lake.ops.<table> if missing (unpartitioned, format v2), add any new Postgres
column (events.plan_evolution), keep the newest event per key by (LSN, offset), and MERGE with the stale-event
guard (only a newer LSN updates or deletes). Batches are bounded (maxOffsetsPerTrigger) and collected to the driver:
the decoding is the pure events.py, the write is a Spark MERGE. Prints one line `CDC_RESULT {json}`.
"""

import argparse
import json
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dockwatch.cdc import events as ev

MAX_OFFSETS_PER_TRIGGER = 1000  # ~4 batches for the seeded run: the column add lands in a later batch


def table_columns(spark, target: str) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in spark.table(target).schema.fields]


def apply_events(spark, records, catalog: str, namespace: str, stats: dict | None = None) -> dict:
    """Apply Kafka records (rows with topic, offset, key, value) to lake.<namespace>.*; returns counts per table/op.

    This is the foreachBatch body (and what tests/spark/test_cdc_apply.py drives directly).
    """
    stats = stats if stats is not None else {}
    by_table: dict[str, list[ev.Event]] = defaultdict(list)
    for r in records:
        e = ev.parse_event(r["topic"], r["offset"], r["key"], r["value"])
        if e is not None:
            by_table[e.table].append(e)
    applied = datetime.now(ev.UTC)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    for table in sorted(by_table):
        events = by_table[table]
        target = f"{catalog}.{namespace}.{table}"
        incoming = ev.merged_columns(events)
        spark.sql(ev.create_table_sql(target, incoming))
        for column in ev.plan_evolution(table_columns(spark, target), incoming):
            spark.sql(ev.add_column_sql(target, column))
            print(f"cdc: {target} added column {column.name} {column.type}", flush=True)
        data_cols = [(n, t) for n, t in table_columns(spark, target) if not n.startswith("_")]
        names = [n for n, _ in data_cols]
        latest = ev.latest_per_key(events)
        df = spark.createDataFrame(ev.source_rows(latest, names, applied), ev.spark_schema(data_cols))
        view = f"cdc_src_{table}"
        df.createOrReplaceTempView(view)
        spark.sql(ev.merge_sql(target, view, list(events[0].key_names), names))
        spark.catalog.dropTempView(view)
        counts = stats.setdefault(table, {op: 0 for op in ev.OPS})
        for e in events:
            counts[e.op] += 1
    return stats


def make_batch_fn(spark, catalog: str, namespace: str, stats: dict, batches: dict | None = None):
    """The foreachBatch function: collect the bounded batch of Kafka records and apply it."""

    def batch_fn(batch_df, batch_id: int) -> None:
        records = batch_df.select("topic", "offset", "key", "value").collect()
        apply_events(spark, records, catalog, namespace, stats)
        if batches is not None:
            batches["n"] = batches.get("n", 0) + 1
        print(f"cdc: batch {batch_id}: {len(records)} records", flush=True)

    return batch_fn


def build_session():
    from dockwatch.config import get_settings
    from dockwatch.streaming.lake import build_session as lake_session

    s = get_settings()
    spark = lake_session(s, "dockwatch-cdc-apply")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    return s, spark


def reset(spark, catalog: str, namespace: str, checkpoint_root: str) -> dict:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    dropped = []
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{namespace}.{row.tableName} PURGE")
        dropped.append(row.tableName)
    shutil.rmtree(Path(checkpoint_root) / "cdc", ignore_errors=True)
    return {"mode": "reset", "dropped": sorted(dropped)}


def catchup(spark, s, replay: bool) -> dict:
    root = Path(s.checkpoint_root) / "cdc"
    checkpoint = root / (f"replay-{int(time.time())}" if replay else s.cdc_namespace)
    stats: dict = {}
    batches = {"n": 0}
    batch_fn = make_batch_fn(spark, s.iceberg_catalog, s.cdc_namespace, stats, batches)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.spark_kafka_bootstrap)
        .option("subscribePattern", s.cdc_topic_pattern)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    query = (
        raw.writeStream.foreachBatch(batch_fn)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .queryName("cdc_replay" if replay else "cdc_ops")
        .start()
    )
    query.awaitTermination()
    if replay:
        shutil.rmtree(checkpoint, ignore_errors=True)
    return {
        "mode": "replay" if replay else "normal",
        "batches": batches["n"],
        "events_read": sum(sum(c.values()) for c in stats.values()),
        "events": stats,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="cdc/apply.py")
    p.add_argument("--replay", action="store_true", help="fresh checkpoint from earliest, deleted afterwards")
    p.add_argument("--reset", action="store_true", help="drop lake.ops.* (PURGE) and /checkpoints/cdc")
    args = p.parse_args(argv)
    t = time.monotonic()
    s, spark = build_session()
    if args.reset:
        result = reset(spark, s.iceberg_catalog, s.cdc_namespace, s.checkpoint_root)
    else:
        result = catchup(spark, s, args.replay)
    result["seconds"] = round(time.monotonic() - t, 1)
    spark.stop()
    print("CDC_RESULT " + json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    sys.exit(main())
