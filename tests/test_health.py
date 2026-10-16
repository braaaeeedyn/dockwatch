"""pipeline_health rules (Loop K, M5): the 1,440-minute series from silver's per-minute aggregates, consumer lag, the
tiles of the last active hour, the component statuses, and the health.json document and its budget."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from dockwatch.batch import health, maintenance

START = datetime(2026, 10, 15, 12, 0, 37, tzinfo=UTC)  # the run's start; the window ends at 12:00
W0, W1 = health.window(START)


def naive(m: datetime) -> datetime:
    return m.replace(tzinfo=None)  # Trino returns silver's timestamp(6) as naive UTC


def series(events=None, processed=None, published=None, done=None):
    return health.minutely_rows(
        W0,
        [{"m": naive(m), "n": n} for m, n in (events or {}).items()],
        [{"m": naive(m), "n": n, "p50": p50, "p95": p95} for m, (n, p50, p95) in (processed or {}).items()],
        [{"m": naive(m), "n": n} for m, n in (published or {}).items()],
        [{"m": naive(m), "n": n} for m, n in (done or {}).items()],
    )


def at(i: int) -> datetime:
    return W0 + timedelta(minutes=i)


def test_minutely_rows_cover_the_window_with_events_processed_and_latency():
    assert health.window(START) == (
        datetime(2026, 10, 14, 12, 0, tzinfo=UTC),
        datetime(2026, 10, 15, 12, 0, tzinfo=UTC),
    )
    rows = series(events={at(0): 641, at(5): 640}, processed={at(1): (641, 52.3456, 58.9)})
    assert len(rows) == health.MINUTES == 1440
    assert [r[0] for r in rows] == [W0 + timedelta(minutes=i) for i in range(1440)]  # consecutive, none missing
    assert rows[0][1:3] == [641, 0]
    assert rows[1][1:5] == [0, 641, 52.346, 58.9]  # latency rounded to 3 digits
    assert rows[5][1] == 640
    assert rows[2][3:5] == [None, None]  # nothing processed: no latency, not 0
    assert all(r[1] == 0 for i, r in enumerate(rows) if i not in (0, 5))
    sql = health.processed_sql(W0, W1)
    assert "approx_percentile(to_milliseconds(processed_ts - fetched_ts) / 1000.0, 0.95)" in sql
    assert "processed_ts >= TIMESTAMP '2026-10-14 12:00:00' AND processed_ts < TIMESTAMP '2026-10-15 12:00:00'" in sql
    assert "kafka_ts >= TIMESTAMP '2026-10-14 12:00:00'" in health.events_sql(W0, W1)


def test_lag_is_records_published_but_not_yet_in_silver():
    # 100 rows published before the window and processed in minute 2; 641 published in minute 0, processed in
    # minute 3; one row whose broker clock is ahead of Spark's (published "after" it was processed) in minute 4.
    published = {W0 - timedelta(minutes=3): 100, at(0): 641, at(4): 1}
    done = {at(2): 100, at(3): 641, at(4): 1}  # minute of greatest(kafka_ts, processed_ts)
    rows = series(published=published, done=done)
    lag = [r[5] for r in rows[:6]]
    # end of minute 0: the 100 carried in + 641 waiting; end of minute 2: 641; minute 3 on: nothing waiting
    assert lag == [741, 741, 641, 0, 0, 0]
    assert rows[-1][5] == 0
    assert "greatest(kafka_ts, processed_ts)" in health.lag_sql(W0, W1, "greatest(kafka_ts, processed_ts)")
    assert "WHERE processed_ts >= TIMESTAMP '2026-10-14 12:00:00'" in health.lag_sql(W0, W1, "kafka_ts")


def test_tiles_come_from_the_last_active_hour():
    processed = {at(i): (641, 50.0, 55.0 + (i % 7)) for i in range(100, 200)}
    processed[at(150)] = (641, 50.0, 99.5)  # the hour's worst p95
    events = {at(i): 600 for i in range(141, 201)}
    rows = series(events=events, processed=processed, published={at(1430): 5}, done={})
    t = health.tiles(rows, 42)
    # last active minute = 199; its hour = minutes 140-199: 59 minutes with 600 events
    assert t == {"latency_p95_s": 99.5, "events_per_min": round(59 * 600 / 60, 1), "lag_records": 5, "freshness_s": 42}
    quiet = health.tiles(series(), None)
    assert quiet == {"latency_p95_s": None, "events_per_min": None, "lag_records": 0, "freshness_s": None}


def test_status_is_the_worst_component_with_the_plan_thresholds():
    assert [health.stream_status(s) for s in (0, 300, 301, 86_400, 86_401, None)] == [
        "ok", "ok", "warn", "warn", "fail", "fail"]  # fmt: skip
    assert health.table_status("silver.station_status", 299) == "ok"
    assert health.table_status("silver.station_status", 90_000) == "fail"
    assert health.table_status("bronze.trips", 172_800) == "ok"
    assert health.table_status("bronze.trips", 172_801) == "warn"
    assert health.table_status("marts.mart_rebalancing_need", None) == "warn"
    assert health.quality_status(None) == "warn"
    assert health.quality_status({"passed": True}) == "ok"
    assert health.quality_status({"passed": False}) == "fail"
    now = START
    run = [{"status": "ok", "finished_at": now - timedelta(hours=47)}, {"status": "skipped", "finished_at": None}]
    assert health.maintenance_status(run, now) == "ok"
    assert health.maintenance_status([{**run[0], "finished_at": now - timedelta(hours=49)}], now) == "warn"
    assert health.maintenance_status([*run, {"status": "error", "finished_at": now}], now) == "fail"
    assert health.maintenance_status([], now) == "warn"
    assert health.worst(["ok", "warn", "ok"]) == "warn"
    assert health.worst(["warn", "fail"]) == "fail"


def document(**overrides):
    rows = series(events={at(i): 641 for i in range(1440)},
                  processed={at(i): (641, 51.123456, 58.987654) for i in range(1440)})  # fmt: skip
    args = {
        "now": START,
        "run_id": "manual__2026-10-15T12:00:37+00:00",
        "rows": rows,
        "last_processed": naive(START - timedelta(seconds=90)),
        "tables": [{"table": t, "last_commit": START - timedelta(hours=1), "files": 7, "partitions": 6}
                   for t in maintenance.ALL_TABLES],  # fmt: skip
        "gate": {"passed": True, "tests": 28, "tests_passed": 28, "warnings": 0, "problems": []},
        "tests": [{"name": f"not_null_mart_{i}", "status": "pass"} for i in range(28)],
        "run": [{"table_name": t, "status": "ok", "files_before": 9, "files_after": 1, "bytes_before": 900,
                 "bytes_after": 800, "finished_at": START - timedelta(hours=2)} for t in maintenance.ALL_TABLES],
    }  # fmt: skip
    return health.health_doc(**{**args, **overrides})


def test_health_document_has_every_section_and_fits_its_budget(tmp_path):
    doc = document()
    assert set(doc) == {"format", "generated_at", "dag_run_id", "status", "tiles", "minutely", "tables", "quality",
                        "compaction"}  # fmt: skip
    assert doc["format"] == 1
    assert doc["generated_at"] == "2026-10-15T12:00:37+00:00"
    assert [c["name"] for c in doc["status"]["components"]] == ["stream", "quality", "maintenance"]
    assert all(c["status"] == "ok" and c["detail"] for c in doc["status"]["components"])
    assert [c["detail"] for c in doc["status"]["components"]] == [
        "newest row reached silver 90 s ago",
        "28 of 28 dbt tests passed",
        "0 of 16 tables failed in the newest run, finished 2 h ago",
    ]
    assert [health.ago(s) for s in (45, 600, 10_800, 259_200)] == ["45 s", "10 min", "3 h", "3 days"]
    assert doc["status"]["overall"] == "ok"
    assert doc["tiles"] == {"latency_p95_s": 58.988, "events_per_min": 641.0, "lag_records": 0, "freshness_s": 90}
    assert doc["minutely"]["columns"] == ["minute", "events", "processed", "latency_p50_s", "latency_p95_s",
                                          "lag_records"]  # fmt: skip
    assert len(doc["minutely"]["rows"]) == 1440
    assert doc["minutely"]["rows"][0][0] == "2026-10-14T12:00:00+00:00"
    tables = {r[0]: r for r in doc["tables"]["rows"]}
    assert set(tables) == set(maintenance.ALL_TABLES)
    assert tables["silver.station_status"][2:] == [3600, 7, 6, "warn"]  # a stream table 1 h old
    assert tables["bronze.trips"][5] == "ok"
    assert doc["quality"]["gate"]["tests_passed"] == 28
    assert len(doc["quality"]["tests"]) == 28
    assert len(doc["compaction"]["rows"]) == 16
    assert doc["compaction"]["columns"][:4] == ["table", "status", "files_before", "files_after"]

    size = health.write_health(tmp_path / "health.json", doc)
    assert size == (tmp_path / "health.json").stat().st_size <= health.HEALTH_BUDGET
    assert json.loads((tmp_path / "health.json").read_text("utf-8")) == json.loads(json.dumps(doc))
    assert not list(tmp_path.glob(".*.tmp"))

    stale = document(last_processed=naive(START - timedelta(days=2)), gate=None)
    assert {c["name"]: c["status"] for c in stale["status"]["components"]} == {
        "stream": "fail", "quality": "warn", "maintenance": "ok"}  # fmt: skip
    assert stale["status"]["overall"] == "fail"
    with pytest.raises(health.HealthError, match="bytes >"):
        health.write_health(tmp_path / "big.json", {**doc, "padding": "x" * health.HEALTH_BUDGET})
