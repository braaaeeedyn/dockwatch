"""The dbt marts on Trino and the insights export (M4b). Logic: dockwatch.batch.marts, dbt/dockwatch.

guard -> stations (lake.bronze.station_information from the newest GBFS station_information snapshot, pyiceberg)
      -> dbt_build (models + tests; a failed test fails without retry) -> quality_gate (data/dbt/quality_gate.json)
      -> export (web/data/insights/*.json, manifest.json last; only after the gate passed)
"""

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from dockwatch.batch import airflow_defaults, dag_tasks

with DAG(
    dag_id="marts_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    default_args=airflow_defaults.default_args(execution_timeout=timedelta(minutes=30)),
    tags=["dockwatch", "m4b", "trino", "dbt"],
    doc_md=__doc__,
    is_paused_upon_creation=True,  # never runs by surprise when the scheduler starts; unpause on purpose
    **airflow_defaults.dag_kwargs(),
) as dag:
    guard = PythonOperator(task_id="guard", python_callable=dag_tasks.marts_guard)
    stations = PythonOperator(task_id="stations", python_callable=dag_tasks.marts_stations)
    dbt_build = PythonOperator(task_id="dbt_build", python_callable=dag_tasks.marts_dbt_build)
    gate = PythonOperator(task_id="quality_gate", python_callable=dag_tasks.marts_quality_gate)
    export = PythonOperator(
        task_id="export", python_callable=dag_tasks.marts_export, trigger_rule="all_success", sla=timedelta(hours=1)
    )
    guard >> stations >> dbt_build >> gate >> export
