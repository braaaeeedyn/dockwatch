"""Daily Iceberg maintenance on the 9 lake tables through Trino (M4a). Logic: dockwatch.batch.maintenance.

guard -> maintain (one table at a time: measure, optimize, expire_snapshots, remove_orphan_files, measure)
      -> record (one INSERT into lake.metrics.maintenance_runs; fails if any table failed)
"""

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dockwatch.batch import airflow_defaults, dag_tasks, maintenance

with DAG(
    dag_id="iceberg_maintenance",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    default_args=airflow_defaults.default_args(execution_timeout=timedelta(minutes=30)),
    params={
        "file_size_threshold": Param(maintenance.DEFAULTS["file_size_threshold"], type="string"),
        "expire_older_than": Param(maintenance.DEFAULTS["expire_older_than"], type="string"),
        "orphan_older_than": Param(maintenance.DEFAULTS["orphan_older_than"], type="string"),
    },
    tags=["dockwatch", "m4a", "trino"],
    doc_md=__doc__,
    is_paused_upon_creation=True,  # never runs by surprise when the scheduler starts; unpause on purpose
    **airflow_defaults.dag_kwargs(),
) as dag:
    guard = PythonOperator(task_id="guard", python_callable=dag_tasks.maintenance_guard)
    maintain = PythonOperator.partial(
        task_id="maintain", python_callable=dag_tasks.maintain_table, max_active_tis_per_dag=1
    ).expand(op_kwargs=[{"table": t} for t in maintenance.TABLES])
    record = PythonOperator(
        task_id="record",
        python_callable=dag_tasks.record_maintenance,
        trigger_rule="all_done",
        sla=timedelta(hours=2),
    )
    guard >> maintain >> record
