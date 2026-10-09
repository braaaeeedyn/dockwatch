"""Pure safety rules for the CDC tasks (host side; Python 3.10-safe like the rest of the package).

Memory: Kafka Connect (1 GiB) and a Spark JVM (2 GiB) never run at the same time on this host. `refusal()` says
why a task must not start, given a `running(service)` probe that tasks inject (docker compose ps in real use).
`cdc_topics()` is the only list of Kafka topics a CDC reset may delete.
"""

from collections.abc import Callable, Iterable

SPARK_TASKS = ("cdc-catchup", "cdc-reset", "cdc-verify", "catchup")
CONNECT_TASKS = ("connect-up",)
BLOCKERS = {
    **{t: ("connect", "status-stream") for t in SPARK_TASKS},
    **{t: ("cdc-apply", "status-stream") for t in CONNECT_TASKS},
}
CDC_TOPIC_PREFIXES = ("ops.public.", "dockwatch-connect-")


def refusal(task: str, running: Callable[[str], bool]) -> str | None:
    """None if `task` may start now, else the reason it may not."""
    busy = [s for s in BLOCKERS.get(task, ()) if running(s)]
    if not busy:
        return None
    return (
        f"{task}: {' and '.join(busy)} running; stop it first (python tasks.py connect-stop / stream-stop). "
        "Kafka Connect and a Spark JVM never run together on this host."
    )


def cdc_topics(names: Iterable[str]) -> list[str]:
    """The topics a CDC reset deletes: Debezium's ops.public.* and Connect's own dockwatch-connect-* topics."""
    return sorted(n for n in names if n.startswith(CDC_TOPIC_PREFIXES))
