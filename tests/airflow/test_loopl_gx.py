"""Loop L tests that need the Airflow image (python tasks.py test-airflow): Great Expectations lives only in
/opt/gx-venv with the pinned versions, the silver suite flags each kind of bad row on a small pandas frame in that
venv, and marts_daily's quality_gate task runs the checkpoint (a GE venv subprocess) and merges it into
quality_gate.json."""

import importlib.util
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.exceptions import AirflowFailException  # noqa: E402
from airflow.models import DagBag  # noqa: E402

from dockwatch.batch import dag_tasks, expectations, marts  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
GX_PYTHON = "/opt/gx-venv/bin/python"
RAW = json.loads((ROOT / "tests" / "fixtures" / "gx" / "raw_results.json").read_text(encoding="utf-8"))
NEWEST = datetime(2026, 10, 16, 12, 22, 16, tzinfo=UTC)


def pins(path: str) -> dict[str, str]:
    out = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip() and not line.startswith("#"):
            name, _, version = line.strip().partition("==")
            out[name.lower().replace("_", "-")] = version
    return out


def test_gx_venv_has_the_pinned_great_expectations_and_airflow_does_not():
    want = pins("/opt/gx-requirements.txt")  # the image's copy of infra/airflow/gx-requirements.txt
    assert want["great-expectations"] == "1.24.0"
    probe = ("import json, great_expectations, sqlalchemy, trino, pandas; print(json.dumps({'gx': great_expectations"
             ".__version__, 'sqlalchemy': sqlalchemy.__version__, 'trino': trino.__version__, 'pandas': pandas."
             "__version__}))")  # fmt: skip
    r = subprocess.run([GX_PYTHON, "-c", probe], capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr[-2000:]
    got = json.loads(r.stdout.strip().splitlines()[-1])
    assert got == {"gx": want["great-expectations"], "sqlalchemy": want["sqlalchemy"], "trino": want["trino"],
                   "pandas": want["pandas"]}  # fmt: skip
    freeze = subprocess.run([GX_PYTHON, "-m", "pip", "freeze"], capture_output=True, text=True, timeout=300)
    have = {n.lower().replace("_", "-"): v for n, _, v in (x.partition("==") for x in freeze.stdout.split())}
    assert have == want  # exactly the pinned set, nothing more
    assert importlib.util.find_spec("great_expectations") is None  # Airflow's python: constraints untouched
    import sqlalchemy

    assert sqlalchemy.__version__.startswith("1.4.")
    assert sys.executable != GX_PYTHON
    assert os.environ.get("GX_ANALYTICS_ENABLED") == "False"  # no usage analytics, no network at run time


def row(**changes) -> dict:
    r = {
        "station_id": "st-1", "snapshot_ts": "2026-10-16T12:00:00Z", "last_reported_ts": "2026-10-16T11:59:00Z",
        "num_bikes_available": 3, "num_bikes_disabled": 1, "num_ebikes_available": 1, "num_docks_available": 6,
        "num_docks_disabled": 0, "capacity": 10, "is_installed": True, "is_renting": True, "is_returning": True,
        "state": "ok", "fetched_ts": "2026-10-16T12:00:01Z", "kafka_ts": "2026-10-16T12:00:02Z",
        "processed_ts": "2026-10-16T12:00:40Z", "num_scooters_available": 0, "age_s": 40,
    }  # fmt: skip
    r.update(changes)
    r["bikes_plus_docks"] = r["num_bikes_available"] + r["num_docks_available"]  # as the query asset derives it
    return r


def test_gx_suite_flags_each_violation_on_a_small_frame_in_the_gx_venv(tmp_path):
    clean = [row(), row(station_id="st-2", state="empty"), row(snapshot_ts="2026-10-16T12:01:00Z", state="full")]
    frames = {
        "clean": clean,
        "duplicate": [*clean, row()],
        "negative": [*clean, row(station_id="st-3", num_docks_available=-1)],
        "unknown_state": [*clean, row(station_id="st-4", state="closed")],
        "over_capacity": [*clean, row(station_id="st-5", num_bikes_available=8, num_docks_available=4)],
        "old": [{**r, "age_s": 7200} for r in clean],  # the newest row is 2 h old
    }
    (tmp_path / "frames.json").write_text(json.dumps(frames), encoding="utf-8")
    cmd = [GX_PYTHON, "-m", "dockwatch.batch.gx_run", "frame", "--frames", str(tmp_path / "frames.json"), "--out",
           str(tmp_path / "out.json")]  # fmt: skip
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-3000:]
    out = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
    bad = {name: [(x["expectation"], x["column"], x["status"]) for x in rows if not x["success"]]
           for name, rows in out.items()}  # fmt: skip
    assert bad == {
        "clean": [],
        "duplicate": [("expect_compound_columns_to_be_unique", "station_id,snapshot_ts", "fail")],
        "negative": [("expect_column_values_to_be_between", "num_docks_available", "fail")],
        "unknown_state": [("expect_column_values_to_be_in_set", "state", "fail")],
        "over_capacity": [("expect_column_pair_values_a_to_be_greater_than_b", "capacity,bikes_plus_docks", "fail")],
        "old": [("expect_column_min_to_be_between", "age_s", "warn")],  # freshness only warns
    }
    found = {name: {x["column"]: x for x in rows} for name, rows in out.items()}
    assert found["duplicate"]["station_id,snapshot_ts"]["unexpected_count"] == 2  # both rows of the duplicated pair
    assert found["old"]["age_s"]["observed"] == 7200
    assert out["clean"][0]["expectation"] == "expect_table_row_count_to_be_between"
    assert out["clean"][0]["observed"] == 3


class Run:
    run_id = "manual__2026-10-16T12:40:00+00:00"


def fake_trino(sql: str) -> list[dict]:
    if "max(snapshot_ts)" in sql:
        return [{"t": NEWEST.replace(tzinfo=None)}]
    if "max(service_date)" in sql:
        return [{"d": date(2026, 10, 16), "s": date(2026, 10, 16)}]
    return [{"n": 10}]


def gate_with(monkeypatch, tmp_path, raw, code=0):
    """Run marts_quality_gate with a fake Trino and a fake GE subprocess that writes `raw` (None: writes nothing)."""
    monkeypatch.setenv("DOCKWATCH_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(dag_tasks, "trino_execute", lambda: fake_trino)
    calls = []

    def run(cmd, timeout=None, **_):
        calls.append((cmd, timeout))
        if raw is not None:
            out = Path(cmd[cmd.index("--out") + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            s, e = expectations.window(datetime.fromisoformat(cmd[cmd.index("--window-end") + 1]))
            doc = expectations.results_doc(expectations.results(raw), dag_run_id=cmd[cmd.index("--run-id") + 1],
                                           run_at=NEWEST, gx_version="1.24.0", seconds=4.1, start=s, end=e,
                                           now=NEWEST)  # fmt: skip
            marts.write_atomic(out, json.dumps(doc))
        return subprocess.CompletedProcess(cmd, code)

    monkeypatch.setattr(subprocess, "run", run)
    return calls


def gate_file(tmp_path) -> dict:
    return json.loads((tmp_path / "dbt" / "quality_gate.json").read_text(encoding="utf-8"))


def test_marts_daily_runs_the_checkpoint_inside_its_quality_gate(monkeypatch, tmp_path):
    dag = DagBag(dag_folder=str(ROOT / "airflow" / "dags"), include_examples=False).get_dag("marts_daily")
    assert [t.task_id for t in dag.topological_sort()] == ["guard", "stations", "dbt_build", "quality_gate", "export"]
    assert dag.get_task("quality_gate").python_callable is dag_tasks.marts_quality_gate

    calls = gate_with(monkeypatch, tmp_path, RAW)
    assert dag_tasks.marts_quality_gate(dag_run=Run())["passed"] is True
    [(cmd, timeout)] = calls
    assert cmd[:4] == [GX_PYTHON, "-m", "dockwatch.batch.gx_run", "trino"]
    assert cmd[cmd.index("--window-end") + 1] == "2026-10-16T12:22:16.000000+00:00"  # silver's newest snapshot_ts
    assert cmd[cmd.index("--out") + 1] == str(tmp_path / "quality" / "expectations.json")
    assert timeout == 900
    gate = gate_file(tmp_path)
    assert gate["dag_run_id"] == Run.run_id
    assert gate["expectations"] == {"suite": "silver_station_status", "evaluated": 10, "passed": 9, "warnings": 1,
                                    "failed": 0, "path": "data/quality/expectations.json"}  # fmt: skip

    broken = json.loads(json.dumps(RAW))
    for r in broken:
        if r["type"] == "expect_compound_columns_to_be_unique":
            r.update(success=False, result={**r["result"], "unexpected_count": 2})
    gate_with(monkeypatch, tmp_path, broken)
    with pytest.raises(AirflowFailException, match="gx: One row per station per snapshot"):
        dag_tasks.marts_quality_gate(dag_run=Run())  # a failed blocking expectation: no retry
    assert gate_file(tmp_path)["passed"] is False

    (tmp_path / "quality" / "expectations.json").unlink()
    gate_with(monkeypatch, tmp_path, None, code=1)
    with pytest.raises(RuntimeError, match="exited 1"):
        dag_tasks.marts_quality_gate(dag_run=Run())  # a crashed GE run raises, so Airflow retries
    gate = gate_file(tmp_path)
    assert gate["passed"] is False
    assert gate["problems"] == ["gx: no results (no results document; gx_run exited 1)"]
