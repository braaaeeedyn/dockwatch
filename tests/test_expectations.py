"""Great Expectations on silver (Loop L, M5): the suite, its batch (the newest 24 hours of silver with a fixed `now`),
GE results -> ok / warn / fail, the quality-gate merge, and the expectations block in health.json. Pure: no GE here
(only the GE venv in the Airflow image has it); tests/fixtures/gx/raw_results.json is one real GE checkpoint result
on silver (spike, 2026-10-16), in GE's own order."""

import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from dockwatch.batch import expectations as ex
from dockwatch.batch import health, marts
from dockwatch.streaming.episodes import STATES
from tests.test_health import document

RAW = json.loads((Path(__file__).parent / "fixtures" / "gx" / "raw_results.json").read_text(encoding="utf-8"))
NEWEST = datetime(2026, 10, 16, 12, 22, 16, 250000, tzinfo=UTC)
NOW = datetime(2026, 10, 16, 12, 48, 38, 701893, tzinfo=UTC)
RUN = "manual__2026-10-16T12:40:00+00:00"
PASSED_GATE = marts.quality_gate(
    {"results": [{"unique_id": f"test.dockwatch.t{i}", "status": "pass"} for i in range(28)]},
    {m: 10 for m in marts.MARTS}, date(2026, 10, 16), date(2026, 10, 16),
)  # fmt: skip


def doc_of(raw, run_id=RUN) -> dict:
    s, e = ex.window(NEWEST)
    return ex.results_doc(ex.results(raw), dag_run_id=run_id, run_at=NOW, gx_version="1.24.0", seconds=4.1, start=s,
                          end=e, now=NOW)  # fmt: skip


def failing(kind: str, column: str | None, unexpected: int = 3) -> list[dict]:
    out = json.loads(json.dumps(RAW))
    for r in out:
        if r["type"] == kind and ex.column_key(r["kwargs"]) == column:
            r["success"] = False
            r["result"]["unexpected_count"] = unexpected
    return out


def test_suite_covers_schema_ranges_uniqueness_and_freshness_on_silver():
    keys = {(kind, key): blocking for kind, key, _, blocking, _ in ex.EXPECTATIONS}
    assert len(ex.EXPECTATIONS) == len(keys) == 10
    assert keys == {
        ("expect_table_row_count_to_be_between", None): True,
        ("expect_table_columns_to_match_set", None): True,
        ("expect_column_values_to_not_be_null", "station_id"): True,
        ("expect_column_values_to_not_be_null", "snapshot_ts"): True,
        ("expect_column_values_to_be_in_set", "state"): True,
        ("expect_column_values_to_be_between", "num_bikes_available"): True,
        ("expect_column_values_to_be_between", "num_docks_available"): True,
        ("expect_column_pair_values_a_to_be_greater_than_b", "capacity,bikes_plus_docks"): True,
        ("expect_compound_columns_to_be_unique", "station_id,snapshot_ts"): True,
        ("expect_column_min_to_be_between", "age_s"): False,  # freshness only warns
    }
    kw = {(kind, key): kwargs for kind, key, kwargs, _, _ in ex.EXPECTATIONS}
    schema = kw[("expect_table_columns_to_match_set", None)]
    assert len(schema["column_set"]) == 17
    assert schema["exact_match"] is False  # additive evolution passes
    assert {"station_id", "snapshot_ts", "capacity", "state", "processed_ts"} <= set(schema["column_set"])
    assert kw[("expect_column_values_to_be_in_set", "state")]["value_set"] == list(STATES)
    assert kw[("expect_column_min_to_be_between", "age_s")] == {"max_value": 300}
    assert kw[("expect_column_pair_values_a_to_be_greater_than_b", "capacity,bikes_plus_docks")] == {"or_equal": True}
    assert all(len(label) >= 8 for *_, label in ex.EXPECTATIONS)
    # GE constructor kwargs, and back to the column key a GE result carries
    assert ex.gx_kwargs("expect_column_pair_values_a_to_be_greater_than_b", "capacity,bikes_plus_docks",
                        {"or_equal": True}) == {"column_A": "capacity", "column_B": "bikes_plus_docks",
                                                "or_equal": True}  # fmt: skip
    assert ex.gx_kwargs("expect_compound_columns_to_be_unique", "station_id,snapshot_ts", {}) == {
        "column_list": ["station_id", "snapshot_ts"]}  # fmt: skip
    assert ex.gx_kwargs("expect_column_values_to_be_between", "num_docks_available", {"min_value": 0}) == {
        "column": "num_docks_available", "min_value": 0}  # fmt: skip
    assert ex.gx_kwargs("expect_table_row_count_to_be_between", None, {"min_value": 1}) == {"min_value": 1}
    for kind, key, kwargs, _, _ in ex.EXPECTATIONS:
        assert ex.column_key(ex.gx_kwargs(kind, key, kwargs)) == key


def test_batch_is_the_newest_24_hours_of_silver_with_a_fixed_now():
    start, end = ex.window(NEWEST)
    assert (start, end) == (NEWEST - timedelta(hours=24), NEWEST)
    assert ex.iso(start) == "2026-10-15T12:22:16.250000+00:00"
    assert ex.iso(NOW) == "2026-10-16T12:48:38.701893+00:00"
    sql = ex.query(start, end, NOW)
    assert sql == (
        "SELECT *, num_bikes_available + num_docks_available AS bikes_plus_docks, "
        "date_diff('second', processed_ts, TIMESTAMP '2026-10-16 12:48:38.701893 UTC') AS age_s "
        "FROM lake.silver.station_status WHERE snapshot_ts > TIMESTAMP '2026-10-15 12:22:16.250000 UTC' "
        "AND snapshot_ts <= TIMESTAMP '2026-10-16 12:22:16.250000 UTC'"
    )
    assert ex.query(start, end, NOW) == sql  # `now` is a literal: the same run reads the same rows and ages
    doc = doc_of(RAW)
    assert doc["batch"] == {"table": "lake.silver.station_status", "window_start": "2026-10-15T12:22:16.250000+00:00",
                            "window_end": "2026-10-16T12:22:16.250000+00:00",
                            "now": "2026-10-16T12:48:38.701893+00:00", "rows": 382143}  # fmt: skip


def test_gx_results_become_ok_warn_and_fail_whatever_their_order():
    want = ex.results(RAW)
    assert [(r["expectation"], r["column"]) for r in want] == [(k, c) for k, c, *_ in ex.EXPECTATIONS]  # suite order
    shuffled = RAW[::-1]
    random.Random(7).shuffle(shuffled)
    assert ex.results(shuffled) == want
    assert [r["status"] for r in want] == ["ok"] * 9 + ["warn"]  # real silver: only freshness warns
    rows = {(r["expectation"], r["column"]): r for r in want}
    assert rows[("expect_table_row_count_to_be_between", None)]["observed"] == 382143
    assert rows[("expect_column_min_to_be_between", "age_s")]["observed"] == 1526
    unique = rows[("expect_compound_columns_to_be_unique", "station_id,snapshot_ts")]
    assert (unique["unexpected_count"], unique["element_count"], unique["observed"]) == (0, 382143, None)
    assert unique["kwargs"] == {}
    as_text = json.loads(json.dumps(RAW))
    as_text[-1]["result"]["observed_value"] = "1526.0"  # GE may report a SQL min as text
    assert ex.results(as_text)[-1]["observed"] == 1526
    broken = ex.results(failing("expect_compound_columns_to_be_unique", "station_id,snapshot_ts", 2))
    assert [r["status"] for r in broken] == ["ok"] * 8 + ["fail", "warn"]
    assert broken[8]["unexpected_count"] == 2  # rows in duplicated groups, as GE counts them
    missing = ex.results([r for r in RAW if r["type"] != "expect_column_values_to_be_in_set"])
    assert [r["status"] for r in missing].count("fail") == 1  # a suite entry GE did not report never passes
    doc = doc_of(RAW)
    assert (doc["evaluated"], doc["passed"], doc["warnings"], doc["failed"], doc["success"]) == (10, 9, 1, 0, True)
    assert (doc["format"], doc["suite"]) == (1, "silver_station_status")
    assert doc["checkpoint"] == "marts_daily_quality"


def test_a_failed_blocking_expectation_fails_the_gate_and_freshness_only_warns():
    ok = ex.merge_gate(PASSED_GATE, doc_of(RAW), RUN)
    assert ok["passed"] is True
    assert ok["problems"] == []
    assert ok["expectations"] == {"suite": "silver_station_status", "evaluated": 10, "passed": 9, "warnings": 1,
                                  "failed": 0, "path": "data/quality/expectations.json"}  # fmt: skip
    assert (ok["tests"], ok["tests_passed"], ok["warnings"]) == (28, 28, 0)  # the dbt counts keep their meaning
    bad = ex.merge_gate(PASSED_GATE, doc_of(failing("expect_column_values_to_be_between", "num_bikes_available")), RUN)
    assert bad["passed"] is False
    assert len(bad["problems"]) == 1
    assert bad["problems"][0].startswith("gx: Bikes available is never negative")
    assert "3 unexpected rows" in bad["problems"][0]
    assert bad["expectations"]["failed"] == 1
    empty = json.loads(json.dumps(RAW))
    empty[0].update(success=False, result={"observed_value": 0})
    assert ex.merge_gate(PASSED_GATE, doc_of(empty), RUN)["problems"] == [
        "gx: The newest 24 hours of silver have rows (expect_table_row_count_to_be_between, table): 0"]  # fmt: skip
    fresh = json.loads(json.dumps(RAW))
    fresh[-1]["success"] = True
    assert ex.merge_gate(PASSED_GATE, doc_of(fresh), RUN)["expectations"]["warnings"] == 0
    dbt_failed = {**PASSED_GATE, "passed": False, "problems": ["test.dockwatch.t1: fail"]}
    both = ex.merge_gate(dbt_failed, doc_of(RAW), RUN)
    assert both["passed"] is False
    assert both["problems"] == ["test.dockwatch.t1: fail"]  # GE passed: no gx problem
    assert dbt_failed["problems"] == ["test.dockwatch.t1: fail"]  # the input gate is not mutated


def test_gate_fails_when_the_gx_run_left_no_results(tmp_path):
    gone = ex.merge_gate(PASSED_GATE, ex.read_results(tmp_path / "expectations.json"), RUN, "gx_run exited 1")
    assert gone["passed"] is False
    assert gone["problems"] == ["gx: no results (no results document; gx_run exited 1)"]
    assert gone["expectations"]["evaluated"] is None
    (tmp_path / "bad.json").write_text("{not json", encoding="utf-8")
    assert ex.read_results(tmp_path / "bad.json") is None
    old = ex.merge_gate(PASSED_GATE, doc_of(RAW, run_id="manual__2026-10-15T00:00:00+00:00"), RUN)
    assert old["passed"] is False
    assert old["problems"] == ["gx: no results (written by manual__2026-10-15T00:00:00+00:00, not this run)"]
    short = doc_of(RAW)
    short["results"] = short["results"][:-1]
    assert ex.merge_gate(PASSED_GATE, short, RUN)["problems"][0].startswith("gx: no results (its results do not")
    assert ex.merge_gate(PASSED_GATE, {"format": 2}, RUN)["passed"] is False


def test_health_json_carries_the_expectations_block_within_its_budget(tmp_path):
    path = tmp_path / "quality" / "expectations.json"
    path.parent.mkdir()
    marts.write_atomic(path, json.dumps(doc_of(RAW), indent=1) + "\n")
    doc = document()
    doc["quality"]["expectations"] = ex.read_results(path)  # what dag_tasks.health_export adds, unchanged
    size = health.write_health(tmp_path / "health.json", doc)
    assert size <= health.HEALTH_BUDGET
    back = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert back["quality"]["expectations"] == json.loads(path.read_text(encoding="utf-8"))
    assert (back["quality"]["gate"]["tests"], len(back["quality"]["tests"])) == (28, 28)
    assert len(json.dumps(doc_of(RAW), separators=(",", ":"))) < 10_000  # a few KB of the 500,000 B budget
    assert ex.read_results(tmp_path / "missing.json") is None  # health.json then says null: nothing run yet
