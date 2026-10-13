"""marts_daily rules (M4b): the quality gate, the atomic insights export, the station dimension, SQL literals, and the
host side (dag-test refuses beside another Airflow CLI run; the image rebuilds when its inputs change)."""

import json
from datetime import UTC, date, datetime

import pytest

from dockwatch.batch import host, marts

NOW = datetime(2026, 10, 12, 20, 0, tzinfo=UTC)
ROWS = {m: 10 for m in marts.MARTS}


def results(*statuses):
    rr = [{"unique_id": f"model.dockwatch.{m}", "status": "success"} for m in marts.MARTS]
    rr += [{"unique_id": f"test.dockwatch.t{i}", "status": s} for i, s in enumerate(statuses)]
    return {"metadata": {"generated_at": NOW.isoformat()}, "results": rr}


def test_quality_gate_fails_on_any_failed_or_errored_dbt_node():
    ok = marts.quality_gate(results("pass", "pass", "warn"), ROWS, date(2026, 10, 12), date(2026, 10, 12))
    assert ok["passed"] is True
    assert ok["tests"] == 3
    assert ok["warnings"] == 1
    assert ok["problems"] == []
    for bad in ("fail", "error", "skipped", "runtime error"):
        doc = marts.quality_gate(results("pass", bad), ROWS, date(2026, 10, 12), date(2026, 10, 12))
        assert doc["passed"] is False, bad
        assert doc["problems"] == [f"test.dockwatch.t1: {bad}"], bad
    errored = results("pass")
    errored["results"][0]["status"] = "error"
    assert marts.quality_gate(errored, ROWS, date(2026, 10, 12), date(2026, 10, 12))["passed"] is False


def test_quality_gate_needs_rows_in_every_mart():
    for m in marts.MARTS:
        doc = marts.quality_gate(results("pass"), {**ROWS, m: 0}, date(2026, 10, 12), date(2026, 10, 12))
        assert doc["passed"] is False, m
        assert doc["problems"] == [f"{m}: 0 rows"], m
    missing = {k: v for k, v in ROWS.items() if k != "mart_rebalancing_need"}
    no_mart = marts.quality_gate(results("pass"), missing, None, None)
    assert no_mart["problems"] == ["mart_rebalancing_need: missing rows"]
    assert marts.quality_gate(results("pass"), ROWS, date(2026, 10, 11), date(2026, 10, 12))["passed"] is True
    stale = marts.quality_gate(results("pass"), ROWS, date(2026, 10, 10), date(2026, 10, 12))
    assert stale["passed"] is False
    assert "silver reaches 2026-10-12" in stale["problems"][0]


def tables(n=2):
    return {
        name: (["station_id", "service_date", "share_empty"], [[f"s{i}", date(2026, 10, 12), 0.25] for i in range(n)])
        for name in marts.INSIGHTS
    }


def test_insights_export_writes_atomically_with_the_manifest_last(tmp_path, monkeypatch):
    out = tmp_path / "insights"
    out.mkdir()
    (out / "manifest.json").write_text('{"old": true}', encoding="utf-8")
    replaced = []
    real = marts.os.replace

    def spy(src, dst):
        assert not str(dst).endswith(".tmp")
        assert src != dst
        replaced.append(dst.name)
        real(src, dst)

    monkeypatch.setattr(marts.os, "replace", spy)
    man = marts.export_insights(out, tables(), {"dag_run_id": "manual__x", "quality_gate": {"passed": True}})
    assert replaced == [f"{n}.json" for n in marts.INSIGHTS] + ["manifest.json"]
    assert sorted(p.name for p in out.iterdir()) == sorted(replaced)  # no temp files left
    doc = json.loads((out / "rebalancing_need.json").read_text(encoding="utf-8"))
    assert doc["columns"] == ["station_id", "service_date", "share_empty"]
    assert doc["rows"][0] == ["s0", "2026-10-12", 0.25]
    saved = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert saved == man
    assert saved["dag_run_id"] == "manual__x"
    assert saved["files"]["rebalancing_need"]["rows"] == 2
    assert saved["files"]["rebalancing_need"]["bytes"] == (out / "rebalancing_need.json").stat().st_size


def test_insights_files_stay_under_the_size_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(marts, "SIZE_BUDGET", 400)
    out = tmp_path / "insights"
    with pytest.raises(marts.GateError, match="bytes >"):
        marts.export_insights(out, tables(20), {})
    assert not out.exists()  # nothing written when any file is over budget
    marts.export_insights(out, tables(1), {})
    assert all(p.stat().st_size <= 400 for p in out.glob("*.json"))
    assert marts.plain(0.34794520547945207) == 0.348  # floats are rounded to keep the files small


def test_station_information_rows_from_the_gbfs_document():
    doc = {
        "last_updated": 1791835561,
        "data": {
            "stations": [
                {"station_id": "a1", "name": "Market St", "short_name": "SF-C26-2", "region_id": "3", "lat": 37.7,
                 "lon": -122.4, "capacity": 19, "rental_uris": {}},
                {"station_id": "a1", "name": "duplicate", "lat": 1, "lon": 2},
                {"station_id": "b2", "name": "No code", "lat": 37.8, "lon": -122.3},
                {"name": "no id", "lat": 0, "lon": 0},
            ]
        },
    }  # fmt: skip
    rows = marts.station_rows(doc, NOW)
    fed = datetime.fromtimestamp(1791835561, tz=UTC)
    assert rows == [
        ("a1", "SF-C26-2", "Market St", "3", 37.7, -122.4, 19, fed, NOW),
        ("b2", None, "No code", None, 37.8, -122.3, None, fed, NOW),
    ]
    assert len(rows[0]) == len(marts.STATION_COLUMNS)
    assert (
        "CREATE TABLE IF NOT EXISTS lake.bronze.station_information (station_id varchar"
        in (marts.create_station_sql()[1])
    )


def test_sql_literals_escape_quotes():
    assert marts.lit("O'Farrell St") == "'O''Farrell St'"
    assert marts.lit("'; DROP TABLE x; --") == "'''; DROP TABLE x; --'"
    assert marts.lit(None) == "NULL"
    assert marts.lit(3) == "3"
    assert marts.lit(0.5) == "0.5"
    assert marts.lit(float("nan")) == "nan()"
    assert marts.lit(date(2026, 9, 1)) == "DATE '2026-09-01'"
    assert marts.lit(NOW) == "TIMESTAMP '2026-10-12 20:00:00.000000 UTC'"
    from dockwatch.batch import trips

    sql = trips.insert_load_sql({"run_id": "it's", "trip_month": date(2026, 9, 1), "csv_rows": 5})
    assert "'it''s', DATE '2026-09-01'" in sql
    assert sql.count("NULL") == len(trips.LOADS_COLUMNS) - 3


def test_dag_test_refuses_while_another_airflow_cli_runs(monkeypatch):
    started = []
    monkeypatch.setattr(host, "running_services", lambda: {"trino", "airflow-db", "airflow-cli"})
    monkeypatch.setattr(host, "airflow_db_up", lambda: started.append("db"))
    with pytest.raises(host.BatchError, match="another Airflow CLI run"):
        host.dag_test("trip_history_monthly", ["month=2019-01"])
    assert started == []  # refused before touching airflow-db or starting a container


def test_image_is_rebuilt_when_its_build_inputs_change(tmp_path, monkeypatch):
    dockerfile, reqs = tmp_path / "Dockerfile", tmp_path / "dbt-requirements.txt"
    dockerfile.write_bytes(b"FROM x\r\n")
    reqs.write_text("dbt-core==1.10.23\n", encoding="utf-8")
    monkeypatch.setattr(host, "BUILD_INPUTS", (dockerfile, reqs))
    built = []
    monkeypatch.setattr(host, "compose", lambda *a, **k: built.append(a))
    label = {"value": None}
    monkeypatch.setattr(host, "image_hash", lambda: label["value"])

    first = host.build_hash()
    host.ensure_image()  # no image yet
    assert built[-1] == ("--profile", "airflow-cli", "build", "--build-arg", f"DOCKWATCH_BUILD_HASH={first}",
                         "airflow-cli")  # fmt: skip
    label["value"] = first
    host.ensure_image()
    assert len(built) == 1  # same inputs: no rebuild
    dockerfile.write_bytes(b"FROM x\n")
    assert host.build_hash() == first  # line endings do not count
    reqs.write_text("dbt-core==1.10.23\ndbt-trino==1.10.6\n", encoding="utf-8")
    assert host.build_hash() != first
    host.ensure_image()
    assert len(built) == 2
    assert built[-1][4] == f"DOCKWATCH_BUILD_HASH={host.build_hash()}"
