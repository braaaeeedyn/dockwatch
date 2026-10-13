"""M4b DAG tests that need the Airflow image (python tasks.py test-airflow): the two DAGs' shape, the trip month rule,
the dbt project parse with the image's /opt/dbt-venv, and the Arrow table pyiceberg receives."""

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.exceptions import AirflowFailException  # noqa: E402
from airflow.models import DagBag  # noqa: E402
from airflow.sensors.python import PythonSensor  # noqa: E402

from dockwatch.batch import dag_tasks, trips  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = ROOT / "airflow" / "dags"
DBT_PROJECT = ROOT / "dbt" / "dockwatch"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_trip_history_monthly_backfills_monthly_from_2019(dagbag):
    dag = dagbag.get_dag("trip_history_monthly")
    assert dag.schedule_interval == "@monthly"
    assert dag.start_date == datetime(2019, 1, 1, tzinfo=UTC)
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    assert [t.task_id for t in dag.topological_sort()] == ["guard", "published", "download", "load"]
    sensor = dag.get_task("published")
    assert isinstance(sensor, PythonSensor)
    assert sensor.mode == "reschedule"
    assert sensor.poke_interval == 6 * 3600
    assert sensor.timeout == 20 * 24 * 3600
    assert dag.params["month"] == ""


class Run:
    def __init__(self, conf):
        self.conf = conf


def test_trip_month_comes_from_the_param_or_the_data_interval():
    interval = datetime(2025, 10, 1, tzinfo=UTC)
    assert dag_tasks.trips_month({"params": {"month": ""}, "data_interval_start": interval}) == "2025-10"
    ctx = {"params": {"month": ""}, "dag_run": Run({"month": "2019-01"}), "data_interval_start": interval}
    assert dag_tasks.trips_month(ctx) == "2019-01"
    with pytest.raises(AirflowFailException):
        dag_tasks.trips_month({"params": {"month": "2019-1"}, "data_interval_start": interval})


def test_marts_daily_exports_only_after_the_quality_gate(dagbag):
    dag = dagbag.get_dag("marts_daily")
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is True
    order = [t.task_id for t in dag.topological_sort()]
    assert order == ["guard", "stations", "dbt_build", "quality_gate", "export"]
    export = dag.get_task("export")
    assert export.upstream_task_ids == {"quality_gate"}
    assert str(export.trigger_rule) in ("all_success", "TriggerRule.ALL_SUCCESS")
    assert dag.get_task("quality_gate").upstream_task_ids == {"dbt_build"}


def test_dbt_project_parses_with_three_marts_and_their_tests(tmp_path):
    paths = ["--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs")]
    project = ["--project-dir", str(DBT_PROJECT), "--profiles-dir", str(DBT_PROJECT), "--target", "local"]
    cmd = ["/opt/dbt-venv/bin/dbt", "parse", *project, *paths]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    manifest = json.loads((tmp_path / "target" / "manifest.json").read_text(encoding="utf-8"))
    models = {n["name"]: n for n in manifest["nodes"].values() if n["resource_type"] == "model"}
    marts = {k for k, n in models.items() if n["config"]["materialized"] == "table"}
    assert marts == {"mart_station_availability_daily", "mart_rebalancing_need", "mart_trips_vs_availability"}
    assert all(n["config"]["materialized"] == "ephemeral" for k, n in models.items() if k.startswith("stg_"))
    tests = [n for n in manifest["nodes"].values() if n["resource_type"] == "test"]
    assert len(tests) >= 10
    kinds = {(n.get("test_metadata") or {}).get("name") for n in tests}
    assert {"unique", "not_null", "accepted_values", "relationships"} <= kinds
    states = [n for n in tests if (n.get("test_metadata") or {}).get("name") == "accepted_values"
              and n["column_name"] == "state"]  # fmt: skip
    assert sorted(states[0]["test_metadata"]["kwargs"]["values"]) == sorted(
        ["offline", "empty", "full", "low", "high", "ok"]
    )
    assert {n["name"] for n in manifest["sources"].values()} >= {"trips", "station_information", "availability_5m"}


def test_month_rows_become_an_arrow_table_with_the_bronze_trips_types():
    import pyarrow as pa

    header = list(trips.RIDE)
    rec = ["A1", "classic_bike", "2026-09-01 08:00:00", "2026-09-01 08:10:00", "Market St", "SF-C26-2", "", "",
           "37.7", "-122.4", "37.8", "-122.5", "member"]  # fmt: skip
    row = trips.map_row("ride", {h: i for i, h in enumerate(header)}, rec)
    loaded = datetime(2026, 10, 12, 20, 0, tzinfo=UTC)
    table = trips.month_table("2026-09", "ride", "202609-lyftbikes-tripdata.zip", loaded, [[row, row], [row]])
    assert table.num_rows == 3
    assert table.column_names == [n for n, _ in trips.TRIPS_COLUMNS]
    types = dict(zip(table.column_names, table.schema.types, strict=True))
    assert types["trip_month"] == pa.date32()
    assert types["started_at"] == pa.timestamp("us", tz="UTC")
    assert types["loaded_at"] == pa.timestamp("us", tz="UTC")
    assert types["duration_s"] == pa.int64()
    assert types["start_lat"] == pa.float64()
    assert types["ride_id"] == pa.string()
    first = table.slice(0, 1).to_pylist()[0]
    assert first["trip_month"] == date(2026, 9, 1)
    assert first["schema_version"] == "ride"
    assert first["source_key"] == "202609-lyftbikes-tripdata.zip"
    assert first["started_at"] == datetime(2026, 9, 1, 15, 0, tzinfo=UTC)
    assert first["duration_s"] == 600
    assert first["end_station_code"] is None
    assert first["bike_id"] is None
    assert first["loaded_at"] == loaded
    stations = trips.rows_table((("station_id", "varchar"), ("capacity", "integer")), [("a", 3), ("b", None)])
    assert stations.schema.types == [pa.string(), pa.int32()]
    assert stations.column("capacity").to_pylist() == [3, None]
