"""Loop K DAG tests that need the Airflow image (python tasks.py test-airflow): iceberg_maintenance maps over the 9 core
and the 7 M4b / metrics tables; pipeline_health's shape; the minute series as the Arrow table pyiceberg writes."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

from dockwatch.batch import guard, health, maintenance, trips  # noqa: E402

DAGS_FOLDER = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_iceberg_maintenance_maps_over_all_sixteen_tables(dagbag):
    dag = dagbag.get_dag("iceberg_maintenance")
    maintain = dag.get_task("maintain")
    tables = [kw["table"] for kw in maintain.expand_input.value["op_kwargs"]]
    assert tables == list(maintenance.ALL_TABLES)
    assert len(tables) == 16
    assert set(maintenance.BATCH_TABLES) <= set(tables)
    assert maintain.partial_kwargs["max_active_tis_per_dag"] == 1  # one table at a time on Trino
    assert dag.get_task("record").trigger_rule == "all_done"


def test_pipeline_health_runs_guard_minutely_export_and_is_paused(dagbag):
    dag = dagbag.get_dag("pipeline_health")
    assert dag.schedule_interval == "@hourly"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert [t.task_id for t in dag.topological_sort()] == ["guard", "minutely", "export"]
    assert dag.get_task("export").sla is not None
    assert "pipeline_health" in guard.TRINO_TASKS  # refused beside a Spark JVM or Kafka Connect


def test_minutely_rows_become_an_arrow_table_with_the_table_types():
    w0, _ = health.window(datetime(2026, 10, 15, 12, 0, 37, tzinfo=UTC))
    m = w0.replace(tzinfo=None)  # Trino returns silver's timestamps as naive UTC
    rows = health.minutely_rows(w0, [{"m": m, "n": 641}], [{"m": m, "n": 640, "p50": 51.25, "p95": 58.9}], [], [])
    computed = datetime(2026, 10, 15, 12, 1, tzinfo=UTC)
    table = health.minutely_arrow(rows, "manual__2026-10-15T12:00:37+00:00", computed)
    assert table.num_rows == 1440
    assert table.schema == trips.arrow_schema(health.MINUTELY_COLUMNS)
    assert str(table.schema.field("minute").type) == "timestamp[us, tz=UTC]"
    first = table.slice(0, 1).to_pylist()[0]
    assert first["minute"] == w0
    assert (first["events"], first["processed"], first["latency_p50_s"], first["lag_records"]) == (641, 640, 51.25, 0)
    assert first["run_id"] == "manual__2026-10-15T12:00:37+00:00"
    assert first["computed_at"] == computed
    assert table.column("latency_p95_s").null_count == 1439  # minutes with nothing processed have no latency
