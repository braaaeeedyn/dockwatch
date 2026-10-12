"""The one `default_args` every DockWatch DAG uses (M4a): exponential retries and failure alerts.

Importable without Airflow (plain dict of stdlib values and callbacks), so host tests can check it.
"""

from datetime import timedelta

from dockwatch.batch import alerts

RETRIES = 3
RETRY_DELAY = timedelta(minutes=1)
MAX_RETRY_DELAY = timedelta(minutes=30)


def default_args(execution_timeout: timedelta = timedelta(minutes=30), **overrides) -> dict:
    """retries=3, 1 min doubling up to 30 min, a per-task timeout, and alerts.task_failed on every failure."""
    args = {
        "owner": "dockwatch",
        "retries": RETRIES,
        "retry_delay": RETRY_DELAY,
        "retry_exponential_backoff": True,
        "max_retry_delay": MAX_RETRY_DELAY,
        "execution_timeout": execution_timeout,
        "on_failure_callback": alerts.task_failed,
        "depends_on_past": False,
    }
    args.update(overrides)
    return args


def dag_kwargs() -> dict:
    """Shared DAG arguments: no backfill of missed intervals, one run at a time, SLA misses alert."""
    return {"catchup": False, "max_active_runs": 1, "sla_miss_callback": alerts.sla_missed}
