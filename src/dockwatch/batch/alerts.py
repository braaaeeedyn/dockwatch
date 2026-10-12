"""Failure, SLA-miss and slot alerts for the Airflow DAGs (M4a). Importable without Airflow.

Every alert is one JSON line in data/airflow/alerts.jsonl (`at`, `kind`, `dag_id`, `task_id`, `run_id`, `try_number`,
`max_tries`, `exception`, `log_url`), an ERROR log line, and, when DOCKWATCH_ALERTS_TOPIC_ARN is set, an SNS message.
The SNS publisher is injected (`publisher=`) in tests; the real one is boto3.
"""

import json
import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger("dockwatch.batch.alerts")

KINDS = ("task_failed", "sla_miss", "slot_unhealthy")
Publisher = Callable[[str, str, str], None]  # (topic_arn, subject, message)


def data_dir() -> Path:
    """data/ on the host, /opt/dockwatch/data in the Airflow containers (DOCKWATCH_DATA_DIR)."""
    return Path(os.environ.get("DOCKWATCH_DATA_DIR") or "data")


def alerts_path() -> Path:
    return data_dir() / "airflow" / "alerts.jsonl"


def sns_publisher(topic_arn: str, subject: str, message: str) -> None:
    import boto3

    boto3.client("sns").publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)


def alert(
    kind: str,
    *,
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    try_number: int | None = None,
    max_tries: int | None = None,
    exception: str | None = None,
    log_url: str | None = None,
    path: Path | None = None,
    topic_arn: str | None = None,
    publisher: Publisher | None = None,
    now: datetime | None = None,
) -> dict:
    """Write one alert line (and publish it to SNS when a topic is set). Returns the record."""
    if kind not in KINDS:
        raise ValueError(f"unknown alert kind {kind!r}")
    rec = {
        "at": (now or datetime.now(UTC)).isoformat(timespec="seconds"),
        "kind": kind,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "max_tries": max_tries,
        "exception": exception,
        "log_url": log_url,
    }
    line = json.dumps(rec, default=str)
    path = path or alerts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    log.error("dockwatch alert: %s", line)
    if topic_arn is None:
        from dockwatch.config import get_settings

        topic_arn = get_settings().alerts_topic_arn
    topic = topic_arn
    if topic:
        (publisher or sns_publisher)(topic, f"dockwatch {kind}: {dag_id}.{task_id}", line)
    return rec


def task_failed(context: dict) -> dict:
    """Airflow on_failure_callback (shared default_args)."""
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    exc = context.get("exception")
    return alert(
        "task_failed",
        dag_id=getattr(ti, "dag_id", None),
        task_id=getattr(ti, "task_id", None),
        run_id=getattr(dag_run, "run_id", None) or getattr(ti, "run_id", None),
        try_number=getattr(ti, "try_number", None),
        max_tries=getattr(ti, "max_tries", None),
        exception=f"{type(exc).__name__}: {exc}" if exc is not None else None,
        log_url=getattr(ti, "log_url", None),
    )


def sla_missed(dag, task_list, blocking_task_list, slas, blocking_tis) -> list[dict]:
    """Airflow DAG sla_miss_callback: one alert per missed task (fires only under the scheduler)."""
    out = []
    for sla in slas or []:
        out.append(
            alert(
                "sla_miss",
                dag_id=getattr(sla, "dag_id", None) or getattr(dag, "dag_id", None),
                task_id=getattr(sla, "task_id", None),
                run_id=str(getattr(sla, "execution_date", "")) or None,
                exception=f"SLA missed; blocking: {str(blocking_task_list or '').strip() or 'none'}",
            )
        )
    return out


def slot_unhealthy(dag_id: str, task_id: str, run_id: str | None, message: str) -> dict:
    return alert("slot_unhealthy", dag_id=dag_id, task_id=task_id, run_id=run_id, exception=message)
