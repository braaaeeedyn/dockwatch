"""CDC apply tests: the real foreachBatch function on Iceberg. Run inside the Spark image: python tasks.py test-spark
(skipped on the host). Records are the Debezium messages captured from the real connector (tests/fixtures/cdc/)."""

import json
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from dockwatch.cdc import events as ev  # noqa: E402
from dockwatch.cdc.apply import make_batch_fn  # noqa: E402
from dockwatch.cdc.checksum import table_checksum  # noqa: E402

pytestmark = pytest.mark.spark

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "cdc"
CATALOG = "cdctest"
TABLES = ("stations", "docks", "vans", "rebalancing_jobs", "maintenance_tickets")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    # spark.sql.extensions is static: this module needs its own session and must stop it, because
    # test_transforms.py runs later in the same process with a plain session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def records(*tables: str) -> list[dict]:
    out = []
    for t in tables:
        out += [json.loads(line) for line in (FIXTURES / f"{t}.jsonl").read_text(encoding="utf-8").splitlines()]
    return out


def kafka_df(spark, recs: list[dict]):
    """The shape the Kafka source hands to foreachBatch."""
    rows = [
        (r["key"].encode(), r["value"].encode(), r["topic"], r["partition"], r["offset"])
        for r in sorted(recs, key=lambda r: (r["topic"], r["offset"]))
    ]
    return spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long")


def apply(spark, namespace: str, recs: list[dict], stats: dict | None = None) -> dict:
    stats = stats if stats is not None else {}
    make_batch_fn(spark, CATALOG, namespace, stats)(kafka_df(spark, recs), 0)
    return stats


def is_priority(rec: dict) -> bool:
    return '"field":"priority"' in rec["value"]


def expected_rows(recs: list[dict], table: str) -> list[ev.Event]:
    """Pure-Python fold of the same events: newest per key, deletes removed."""
    evs = [ev.parse_event(r["topic"], r["offset"], r["key"], r["value"]) for r in recs]
    return [e for e in ev.latest_per_key([e for e in evs if e.table == table]) if e.op != "d"]


def table_rows(spark, namespace: str, table: str) -> list:
    return sorted(spark.table(f"{CATALOG}.{namespace}.{table}").collect(), key=lambda r: str(r[0]))


def checksum_of(spark, namespace: str, table: str, columns: list[str]) -> str:
    rows = spark.table(f"{CATALOG}.{namespace}.{table}").select(*columns).collect()
    return table_checksum([tuple(r) for r in rows])


def test_merge_applies_inserts_updates_and_deletes(spark):
    recs = records(*TABLES)
    jobs = [r for r in recs if r["topic"].endswith("rebalancing_jobs")]
    first_delete = next(i for i, r in enumerate(jobs) if json.loads(r["value"])["payload"]["op"] == "d")
    # batch 1: everything up to (not including) the first job delete; batch 2: the rest, so the delete and later
    # updates MERGE against rows written by an earlier batch
    batch1 = [r for r in recs if not r["topic"].endswith("rebalancing_jobs")] + jobs[:first_delete]
    stats = apply(spark, "merge", batch1)
    deleted_key = json.loads(jobs[first_delete]["key"])["payload"]["job_id"]
    assert deleted_key in {r.job_id for r in table_rows(spark, "merge", "rebalancing_jobs")}
    apply(spark, "merge", jobs[first_delete:], stats)
    assert deleted_key not in {r.job_id for r in table_rows(spark, "merge", "rebalancing_jobs")}
    for table in TABLES:
        want = expected_rows(recs, table)
        columns = [c.name for c in ev.merged_columns(want)]
        got = table_rows(spark, "merge", table)
        assert len(got) == len(want), table
        assert checksum_of(spark, "merge", table, columns) == table_checksum(
            [tuple(e.row.get(c) for c in columns) for e in want]
        ), table
    assert stats["rebalancing_jobs"]["d"] == sum(json.loads(r["value"])["payload"]["op"] == "d" for r in jobs)
    assert stats["rebalancing_jobs"]["u"] > 0
    assert stats["stations"]["c"] > 0


def test_stale_events_do_not_overwrite_newer_rows(spark):
    jobs = records("rebalancing_jobs")
    apply(spark, "stale", jobs)
    before = table_rows(spark, "stale", "rebalancing_jobs")
    # an older update of a row that has since changed again: matched, but its LSN is not newer, so nothing happens
    newest = {r.job_id: r._lsn for r in before}

    def lsn(r):
        return json.loads(r["value"])["payload"]["source"]["lsn"]

    def job(r):
        return json.loads(r["key"])["payload"]["job_id"]

    stale = next(
        r
        for r in jobs
        if json.loads(r["value"])["payload"]["op"] == "u" and job(r) in newest and lsn(r) < newest[job(r)]
    )
    apply(spark, "stale", [stale])
    assert table_rows(spark, "stale", "rebalancing_jobs") == before


def test_replaying_the_same_events_changes_nothing(spark):
    recs = records(*TABLES)
    apply(spark, "replay", recs)
    before = {t: table_rows(spark, "replay", t) for t in TABLES}
    # replay all at once, then one record per batch: every event is at or below the stored LSN (or a delete of a row
    # that is gone), so even _applied_ts stays the same
    apply(spark, "replay", recs)
    for r in records("rebalancing_jobs"):
        apply(spark, "replay", [r])
    after = {t: table_rows(spark, "replay", t) for t in TABLES}
    # (one-record batches briefly re-insert a deleted job from its old insert; its later delete removes it again)
    for t in TABLES:
        assert after[t] == before[t], t


def test_added_column_is_evolved_before_merge(spark):
    jobs = records("rebalancing_jobs")
    old = [r for r in jobs if not is_priority(r)]
    new = [r for r in jobs if is_priority(r)]
    assert old
    assert new
    apply(spark, "evolve", old)
    assert "priority" not in spark.table(f"{CATALOG}.evolve.rebalancing_jobs").columns
    apply(spark, "evolve", new)
    df = spark.table(f"{CATALOG}.evolve.rebalancing_jobs")
    assert "priority" in df.columns
    assert dict(df.dtypes)["priority"] == "int"
    data_cols = [c for c in df.columns if not c.startswith("_")]
    assert data_cols[-1] == "priority"  # appended after the original columns, like Postgres's ADD COLUMN
    want = expected_rows(jobs, "rebalancing_jobs")
    assert df.where("priority IS NOT NULL").count() == sum(e.row.get("priority") is not None for e in want)
    assert df.where("priority IS NOT NULL").count() > 0
