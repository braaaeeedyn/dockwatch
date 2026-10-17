"""Bay Wheels trip history into lake.bronze.trips, one month per run (M4b). Logic: dockwatch.batch.trips.

guard -> published (sensor: waits until the month's ZIP is in the bucket; a month missing while later ones exist,
e.g. 2020-04, is recorded and skipped) -> download (cached in data/trips/zips) -> load (map the CSV, one pyiceberg
overwrite of the month, Trino count-back, lake.metrics.trip_loads).

The month is the `month` param (YYYY-MM) if set, else the run's data interval. catchup=True makes
`airflow dags backfill -s ... -e ...` load history; the DAG stays paused (unpaused, the scheduler would load every month
since 2019-01, ~0.9 GB of downloads).

Inlets / outlets (OpenLineage datasets, Loop M): dockwatch.batch.lineage.annotate.
"""

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from dockwatch.batch import airflow_defaults, dag_tasks, lineage

with DAG(
    dag_id="trip_history_monthly",
    schedule="@monthly",
    start_date=pendulum.datetime(2019, 1, 1, tz="UTC"),
    default_args=airflow_defaults.default_args(execution_timeout=timedelta(minutes=30)),
    params={"month": Param("", type="string", description="YYYY-MM; empty = the run's data interval month")},
    tags=["dockwatch", "m4b", "trino", "pyiceberg"],
    doc_md=__doc__,
    is_paused_upon_creation=True,  # never runs by surprise when the scheduler starts; unpause on purpose
    **{**airflow_defaults.dag_kwargs(), "catchup": True},
) as dag:
    guard = PythonOperator(task_id="guard", python_callable=dag_tasks.trips_guard)
    published = PythonSensor(
        task_id="published",
        python_callable=dag_tasks.trips_published,
        mode="reschedule",
        poke_interval=timedelta(hours=6),
        timeout=timedelta(days=20),
    )
    download = PythonOperator(task_id="download", python_callable=dag_tasks.trips_download)
    load = PythonOperator(task_id="load", python_callable=dag_tasks.trips_load, sla=timedelta(hours=1))
    guard >> published >> download >> load

lineage.annotate(dag)
