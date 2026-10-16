"""Pipeline health every hour (M5, Loop K). Logic: dockwatch.batch.health.

guard -> minutely (lake.metrics.pipeline_minutely: the 1,440 minutes before the run from lake.silver.station_status,
                   one pyiceberg commit)
      -> export (web/data/health.json: status, tiles, the minute series, table freshness, quality gate, compaction)
"""

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from dockwatch.batch import airflow_defaults, dag_tasks

with DAG(
    dag_id="pipeline_health",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    default_args=airflow_defaults.default_args(execution_timeout=timedelta(minutes=20)),
    tags=["dockwatch", "m5", "trino"],
    doc_md=__doc__,
    is_paused_upon_creation=True,  # never runs by surprise when the scheduler starts; unpause on purpose
    **airflow_defaults.dag_kwargs(),
) as dag:
    guard = PythonOperator(task_id="guard", python_callable=dag_tasks.health_guard)
    minutely = PythonOperator(task_id="minutely", python_callable=dag_tasks.health_minutely)
    export = PythonOperator(task_id="export", python_callable=dag_tasks.health_export, sla=timedelta(minutes=30))
    guard >> minutely >> export
