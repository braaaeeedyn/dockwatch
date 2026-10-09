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


RESET_HINT = (
    "python tasks.py cdc-reset rebuilds CDC from scratch: it drops lake.ops.* and its checkpoints, the slot, the five "
    "ops tables (re-created empty from the base schema) and the ops.public.* / dockwatch-connect-* topics; the next "
    "connect-up re-snapshots."
)


def slot_refusal(slot: str, wal_status: str | None) -> str | None:
    """None if Kafka Connect may start on replication slot `slot` (absent, reserved or extended), else why not.

    `wal_status` comes from pg_replication_slots (None = no such slot; the connector creates it). A `lost` slot was
    invalidated by max_slot_wal_keep_size: the WAL it needed is gone, so CDC cannot resume from it.
    """
    if wal_status != "lost":
        return None
    return (
        f"replication slot {slot} is lost (wal_status = 'lost'): more WAL than max_slot_wal_keep_size was written "
        f"while Connect was stopped, so Postgres removed the WAL the slot needed and changes since the last catch-up "
        f"cannot be streamed. Refusing to start Kafka Connect. {RESET_HINT}"
    )


def slot_warning(slot: str, wal_status: str | None) -> str | None:
    """A warning (Connect may still start) when the slot is `unreserved`: it will be lost at the next checkpoint
    unless Connect consumes it first."""
    if wal_status != "unreserved":
        return None
    return (
        f"warning: replication slot {slot} is unreserved (past max_slot_wal_keep_size); it may be lost at the next "
        f"checkpoint. Starting Connect now may still save it; if it is lost, run python tasks.py cdc-reset."
    )
