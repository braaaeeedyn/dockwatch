"""DAG tests that need real Airflow: run with `python tasks.py test-airflow` (Airflow image, SQLite metadata).

On the host the repo's top-level airflow/ folder is importable as a namespace package, so the skip guard imports a
real Airflow submodule.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.models import DagBag, DagRun, SlaMiss, TaskInstance  # noqa: E402
from airflow.models.dag import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.utils.session import create_session  # noqa: E402

from dockwatch.batch import airflow_defaults, alerts, runstate  # noqa: E402

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags"
DAG_IDS = {"iceberg_maintenance", "partition_evolution_demo", "cdc_slot_keepalive"}
START = datetime(2026, 10, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == DAG_IDS
    maint = dagbag.get_dag("iceberg_maintenance")
    assert [t.task_id for t in maint.topological_sort()] == ["guard", "maintain", "record"]
    keep = dagbag.get_dag("cdc_slot_keepalive")
    assert keep.params["catch_up_above_mb"] == 256


def test_every_dag_retries_with_exponential_backoff_and_alerts(dagbag):
    for dag in dagbag.dags.values():
        assert dag.sla_miss_callback is alerts.sla_missed, dag.dag_id
        for task in dag.tasks:
            where = f"{dag.dag_id}.{task.task_id}"
            assert task.retries == 3, where
            assert task.retry_exponential_backoff is True, where
            assert task.retry_delay == timedelta(minutes=1), where
            assert task.max_retry_delay == timedelta(minutes=30), where
            assert task.execution_timeout is not None, where
            callbacks = task.on_failure_callback
            callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
            assert alerts.task_failed in callbacks, where
        for leaf in dag.leaves:
            assert leaf.sla is not None, f"{dag.dag_id}.{leaf.task_id} has no sla"


def test_schedules_and_catchup(dagbag):
    want = {"iceberg_maintenance": "@daily", "partition_evolution_demo": None, "cdc_slot_keepalive": "@hourly"}
    for dag_id, schedule in want.items():
        dag = dagbag.get_dag(dag_id)
        assert dag.schedule_interval == schedule, dag_id
        assert dag.catchup is False, dag_id
        assert dag.max_active_runs == 1, dag_id


def test_retry_delays_grow_exponentially_up_to_the_cap(dagbag):
    dag = dagbag.get_dag("iceberg_maintenance")
    task = dag.get_task("record")
    ti = TaskInstance(task=task, run_id="manual__retry-test")
    ti.dag_run = DagRun(dag_id=dag.dag_id, run_id="manual__retry-test", execution_date=START)
    ti.end_date = START
    delays = []
    for attempt in range(1, 9):
        ti.try_number = attempt
        delays.append((ti.next_retry_datetime() - ti.end_date).total_seconds())
    for attempt, seconds in enumerate(delays, start=1):
        floor = min(60 * 2 ** (attempt - 1), 1800)
        ceiling = min(120 * 2 ** (attempt - 1), 1800)
        assert floor <= seconds <= ceiling, (attempt, seconds)
    assert delays[0] < delays[2] < delays[4]
    assert delays[-1] == 1800  # capped at max_retry_delay (30 min)


def test_failed_task_writes_an_alert(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKWATCH_DATA_DIR", str(tmp_path))

    def explode():
        raise ValueError("boom from the test DAG")

    with DAG(
        dag_id="dockwatch_failure_alert_test",
        schedule=None,
        start_date=START,
        default_args=airflow_defaults.default_args(retries=0),
        **airflow_defaults.dag_kwargs(),
    ) as dag:
        PythonOperator(task_id="explode", python_callable=explode)
    run = dag.test()
    assert str(getattr(run.state, "value", run.state)) == "failed"
    lines = (tmp_path / "airflow" / "alerts.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["kind"] == "task_failed"
    assert rec["dag_id"] == "dockwatch_failure_alert_test"
    assert rec["task_id"] == "explode"
    assert rec["run_id"] == run.run_id
    assert rec["try_number"] == 1
    assert rec["max_tries"] == 0
    assert "boom from the test DAG" in rec["exception"]


def test_sla_miss_writes_an_alert(tmp_path, monkeypatch, dagbag):
    monkeypatch.setenv("DOCKWATCH_DATA_DIR", str(tmp_path))
    dag = dagbag.get_dag("iceberg_maintenance")
    miss = SlaMiss(task_id="record", dag_id=dag.dag_id, execution_date=START, timestamp=START + timedelta(hours=3))
    dag.sla_miss_callback(dag, "record", "maintain", [miss], [])
    rec = json.loads((tmp_path / "airflow" / "alerts.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert rec["kind"] == "sla_miss"
    assert rec["dag_id"] == "iceberg_maintenance"
    assert rec["task_id"] == "record"
    assert "maintain" in rec["exception"]


def test_dag_test_fails_stale_running_runs(dagbag):
    dag_id, other = "cdc_slot_keepalive", "iceberg_maintenance"
    with create_session() as s:
        for d in (dag_id, other):
            s.query(TaskInstance).filter(TaskInstance.dag_id == d).delete()
            s.query(DagRun).filter(DagRun.dag_id == d).delete()
        rows = [
            (dag_id, "manual__stale", "running", START),
            (dag_id, "manual__done", "success", START + timedelta(hours=1)),
            (other, "manual__other", "running", START),
        ]
        for d, run_id, state, when in rows:
            s.add(DagRun(dag_id=d, run_id=run_id, execution_date=when, run_type="manual", state=state))
            s.flush()
            ti = TaskInstance(task=dagbag.get_dag(d).tasks[0], run_id=run_id, state=state)
            s.add(ti)
    with create_session() as s:
        assert runstate.fail_stale(dag_id, s) == ["manual__stale"]
    with create_session() as s:
        runs = {(r.dag_id, r.run_id): runstate.plain(r.state) for r in s.query(DagRun)}
        tis = {(t.dag_id, t.run_id): runstate.plain(t.state) for t in s.query(TaskInstance)}
    assert runs[(dag_id, "manual__stale")] == tis[(dag_id, "manual__stale")] == "failed"
    assert runs[(dag_id, "manual__done")] == tis[(dag_id, "manual__done")] == "success"
    assert runs[(other, "manual__other")] == tis[(other, "manual__other")] == "running"
