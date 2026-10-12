"""Partition evolution demo on a lab copy of silver (M4a, manual). Logic: dockwatch.batch.evolution.

day(snapshot_ts) table, D1 loaded one INSERT per hour -> evolve to hour(snapshot_ts) -> D2 loaded the same way ->
optimize; files per spec at each stage go to data/maintenance/partition_evolution.json. Production stays day().
"""

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dockwatch.batch import airflow_defaults, dag_tasks

with DAG(
    dag_id="partition_evolution_demo",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    default_args=airflow_defaults.default_args(execution_timeout=timedelta(minutes=20)),
    params={
        "days": Param(None, type=["null", "array"], description="two YYYY-MM-DD UTC days; default: picked from silver"),
        "file_size_threshold": Param("100MB", type="string"),
    },
    tags=["dockwatch", "m4a", "trino"],
    doc_md=__doc__,
    is_paused_upon_creation=True,  # never runs by surprise when the scheduler starts; unpause on purpose
    **airflow_defaults.dag_kwargs(),
) as dag:
    guard = PythonOperator(task_id="guard", python_callable=dag_tasks.evolution_guard)
    prepare = PythonOperator(task_id="prepare", python_callable=dag_tasks.evolution_prepare)
    daily = PythonOperator(task_id="daily_load", python_callable=dag_tasks.evolution_daily)
    evolve = PythonOperator(task_id="evolve", python_callable=dag_tasks.evolution_evolve)
    hourly = PythonOperator(task_id="hourly_load", python_callable=dag_tasks.evolution_hourly)
    optimize = PythonOperator(task_id="optimize", python_callable=dag_tasks.evolution_optimize)
    report = PythonOperator(task_id="report", python_callable=dag_tasks.evolution_report, sla=timedelta(hours=1))
    guard >> prepare >> daily >> evolve >> hourly >> optimize >> report
