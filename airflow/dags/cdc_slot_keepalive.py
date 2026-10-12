"""Hourly CDC slot keepalive (M4a). Logic: dockwatch.batch.slot.

inspect_slot -> keepalive: lost -> fail + alert (never resets); retained WAL >= catch_up_above_mb or no catch-up for
max_hours_between_catchups -> docker start the existing Kafka Connect container until the slot is caught up, then
docker stop; else skip. One line per run in data/metrics/slot_health.jsonl; alert if safe_wal_size stays low.
"""

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dockwatch.batch import airflow_defaults, dag_tasks, slot

with DAG(
    dag_id="cdc_slot_keepalive",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    default_args=airflow_defaults.default_args(execution_timeout=timedelta(minutes=15)),
    params={k: Param(v, type="integer", minimum=0) for k, v in slot.DEFAULTS.items()},
    tags=["dockwatch", "m4a", "cdc"],
    doc_md=__doc__,
    is_paused_upon_creation=True,  # never runs by surprise when the scheduler starts; unpause on purpose
    **airflow_defaults.dag_kwargs(),
) as dag:
    inspect = PythonOperator(task_id="inspect_slot", python_callable=dag_tasks.keepalive_inspect)
    keep = PythonOperator(task_id="keepalive", python_callable=dag_tasks.keepalive, sla=timedelta(minutes=30))
    inspect >> keep
