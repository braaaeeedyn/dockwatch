"""Pure memory rules for the batch side (M4a). Stdlib only: tasks.py imports this module by path.

Trino (1.5 GiB) never runs beside a Spark JVM (2 GiB) or Kafka Connect (1 GiB) on this host. `refusal()` says why a
task must not start, given a `running(service)` probe (docker ps by compose labels in real use, a dict in tests).
"""

import subprocess
from collections.abc import Callable

PROJECT = "dockwatch"

# Batch work (Trino, the Airflow scheduler): refused while a Spark JVM or Kafka Connect runs.
TRINO_TASKS = (
    "batch-up",
    "scheduler-up",
    "iceberg_maintenance",
    "partition_evolution_demo",
    "trip_history_monthly",
    "marts_daily",
)
TRINO_BLOCKERS = ("status-stream", "cdc-apply", "connect")
# Spark and Connect work: refused while Trino runs.
SPARK_CONNECT_TASKS = (
    "stream",
    "catchup",
    "inspect",
    "verify-lake",
    "sql",
    "connect-up",
    "cdc-catchup",
    "cdc-reset",
    "cdc-verify",
    "cdc-e2e",
)
# cdc_slot_keepalive starts the existing `connect` container: never beside Spark or Trino.
KEEPALIVE_TASKS = ("cdc_slot_keepalive",)
KEEPALIVE_BLOCKERS = ("status-stream", "cdc-apply", "trino")

STOP_HINT = {
    "status-stream": "python tasks.py stream-stop",
    "cdc-apply": "wait for the CDC apply to finish",
    "connect": "python tasks.py connect-stop",
    "trino": "python tasks.py batch-stop",
}


def refusal(task: str, running: Callable[[str], bool]) -> str | None:
    """None if `task` may start now, else the reason it may not (the reason names the running service)."""
    if task in TRINO_TASKS:
        blockers = TRINO_BLOCKERS
        why = "Trino and the Airflow scheduler never run beside a Spark JVM or Kafka Connect on this host"
    elif task in SPARK_CONNECT_TASKS:
        blockers = ("trino",)
        why = "Spark and Kafka Connect never run beside Trino on this host"
    elif task in KEEPALIVE_TASKS:
        blockers = KEEPALIVE_BLOCKERS
        why = "the keepalive starts Kafka Connect, which never runs beside Spark or Trino on this host"
    else:
        return None
    busy = [s for s in blockers if running(s)]
    if not busy:
        return None
    names = " and ".join("Trino" if s == "trino" else s for s in busy)
    hints = " / ".join(STOP_HINT[s] for s in busy)
    return f"{task}: {names} running; stop it first ({hints}). {why[0].upper()}{why[1:]}."


def running_services(timeout: float = 30) -> set[str]:
    """Compose services of this project with a running container, from the docker CLI (empty if Docker is down)."""
    try:
        out = subprocess.run(
            [
                "docker",
                "ps",
                "--filter",
                f"label=com.docker.compose.project={PROJECT}",
                "--format",
                '{{.Label "com.docker.compose.service"}}',
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        return set()
    if out.returncode:
        return set()
    return {line.strip() for line in out.stdout.splitlines() if line.strip()}
