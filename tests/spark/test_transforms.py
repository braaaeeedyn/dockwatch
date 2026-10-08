"""Spark transform tests. Run inside the Spark image: python tasks.py test-spark (skipped on the host)."""

import json

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from dockwatch.streaming.episodes import classify  # noqa: E402
from dockwatch.streaming.transforms import availability_windows, parse_kafka, to_silver  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.codegen.wholeStage", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _msg(station, snap, bikes, docks, reported=None, renting=True, offset=0):
    value = {
        "station_id": station,
        "num_bikes_available": bikes,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "num_ebikes_available": 0,
        "is_installed": True,
        "is_renting": renting,
        "is_returning": True,
        "last_reported": reported if reported is not None else snap - 10,
        "feed_last_updated": snap,
        "feed_version": "2.3",
        "fetched_at": snap + 5.5,
        "snapshot_id": f"station_status:{snap}",
        "num_scooters_available": 0,
    }
    return (station.encode(), json.dumps(value).encode(), "gbfs.station_status", 0, offset, None)


def _kafka_df(spark, rows):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows, "key binary, value binary, topic string, partition int, offset long, ts string")
    return df.withColumn("timestamp", F.current_timestamp()).drop("ts")


def test_parse_types_and_time_columns(spark):
    out = parse_kafka(_kafka_df(spark, [_msg("a", 1_791_412_324, 3, 10)])).first()
    assert out.station_id == "a"
    assert out.snapshot_ts.isoformat() == "2026-10-07T22:32:04"
    assert out.kafka_offset == 0
    assert out.num_scooters_available == 0


def test_spark_state_rule_matches_python_rule(spark):
    cases = [
        ("empty", 1000, 0, 10, None, True),
        ("full", 1000, 10, 0, None, True),
        ("low", 1000, 2, 18, None, True),
        ("high", 1000, 18, 2, None, True),
        ("ok", 1000, 9, 9, None, True),
        ("stale", 10_000, 5, 5, 10_000 - 31 * 60, True),
        ("closed", 1000, 5, 5, None, False),
    ]
    rows = [_msg(name, snap, b, d, rep, renting, i) for i, (name, snap, b, d, rep, renting) in enumerate(cases)]
    got = {r.station_id: r.state for r in to_silver(parse_kafka(_kafka_df(spark, rows))).collect()}
    for name, snap, b, d, rep, renting in cases:
        expected = classify(
            snapshot_ts=snap,
            last_reported=rep if rep is not None else snap - 10,
            is_installed=True,
            is_renting=renting,
            bikes=b,
            docks=d,
            capacity=b + d,
        )
        assert got[name] == expected, name


def test_malformed_json_is_dropped_from_silver(spark):
    bad = (b"x", b"not json", "gbfs.station_status", 0, 1, None)
    rows = [_msg("a", 1000, 3, 10), bad]
    assert to_silver(parse_kafka(_kafka_df(spark, rows))).count() == 1


def test_availability_window_shares(spark):
    t0 = 1_791_412_200  # aligned to a 5-minute boundary
    rows = [_msg("a", t0 + 60 * i, 0 if i < 2 else 5, 10, offset=i) for i in range(5)]
    w = availability_windows(to_silver(parse_kafka(_kafka_df(spark, rows)))).first()
    assert w.samples == 5
    assert w.share_empty == pytest.approx(0.4)
    assert w.min_bikes == 0
    assert w.max_bikes == 5
