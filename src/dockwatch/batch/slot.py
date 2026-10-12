"""CDC replication slot keepalive (DAG cdc_slot_keepalive, M4a).

An idle slot pins WAL until Kafka Connect confirms it; past max_slot_wal_keep_size (1 GB) Postgres drops the slot
(`lost`) and CDC needs `python tasks.py cdc-reset`. Hourly, this module reads the slot and decides:
  lost      -> fail and alert; never reset
  catch_up  -> retained WAL >= catch_up_above_mb, or the last catch-up is older than max_hours_between_catchups:
               `docker start` the existing compose `connect` container, wait until the connector is RUNNING, touch
               one captured row (see TOUCH_SQL), wait until the slot has confirmed that change and the WAL before
               it (retained < 16 MiB), `docker stop` it again (always; a timeout fails the run without retries)
  skip      -> otherwise
and appends one line to data/metrics/slot_health.jsonl. The Iceberg apply is not part of this: Kafka keeps
ops.public.* forever, so `python tasks.py cdc-catchup` applies later.

Why the touch: after a restart Debezium does not confirm (flush) the slot until it has streamed a captured change
newer than its stored offset, and WAL that holds no captured change (another database, empty transactions, logical
messages) never reaches it. So an idle slot would stay pinned however long Connect runs. One `UPDATE vans` per
catch-up is that change; it costs one `u` event on ops.public.vans and bumps one van's updated_at.

Why sometimes a second touch: Postgres moves restart_lsn (what "retained" measures) only when a confirmed LSN passes
a pending restart candidate, and it keeps one candidate at a time, taken from a running-xacts record. Decoding the
backlog leaves a candidate near its start; the first confirmed touch applies that one, and if no newer running-xacts
record is decoded after it (an idle database logs none), retained stays high although confirmed_flush_lsn passed the
target. So once a touch is confirmed but retained is still >= 16 MiB, the touch is repeated (at most every
RETOUCH_AFTER_S, after the bgwriter's 15 s running-xacts interval); measured: two touches (DEVLOG, M4a).
"""

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dockwatch.batch import alerts

SLOT = "dockwatch_ops"
CONNECTOR = "ops-postgres"
PROJECT = "dockwatch"
MIB = 1024 * 1024
CAUGHT_UP_BELOW = 16 * MIB
RETOUCH_AFTER_S = 20
DEFAULTS = {
    "catch_up_above_mb": 256,
    "max_hours_between_catchups": 24,
    "timeout_s": 600,
    "alert_below_safe_mb": 512,
}
RESET_HINT = (
    "python tasks.py cdc-reset rebuilds CDC from scratch (drops lake.ops.*, the slot, the ops tables and the "
    "ops.public.* topics; the next connect-up re-snapshots). This DAG never resets on its own."
)


class SlotLost(RuntimeError):
    pass


class KeepaliveError(RuntimeError):
    pass


@dataclass(frozen=True)
class SlotStatus:
    wal_status: str | None  # None = no such slot
    retained_bytes: int | None
    safe_wal_size: int | None
    confirmed_flush_lsn: str | None
    current_lsn: str | None
    active: bool = False


@dataclass(frozen=True)
class Decision:
    action: str  # catch_up | skip | fail
    reason: str


def params_of(conf: dict | None) -> dict:
    p = dict(DEFAULTS)
    for k, v in (conf or {}).items():
        if k in DEFAULTS and v is not None:
            p[k] = type(DEFAULTS[k])(v)
    return p


def decide(
    status: SlotStatus,
    last_catchup_at: datetime | None,
    now: datetime,
    catch_up_above_mb: float = DEFAULTS["catch_up_above_mb"],
    max_hours_between_catchups: float = DEFAULTS["max_hours_between_catchups"],
) -> Decision:
    """The pure rule: lost -> fail; enough retained WAL or a stale last catch-up -> catch_up; else skip."""
    if status.wal_status is None:
        return Decision("skip", f"no slot {SLOT} yet (python tasks.py connect-up creates it)")
    if status.wal_status == "lost":
        return Decision("fail", f"replication slot {SLOT} is lost (wal_status = 'lost'). {RESET_HINT}")
    retained = status.retained_bytes or 0
    if retained >= catch_up_above_mb * MIB:
        return Decision("catch_up", f"retained WAL {retained / MIB:.1f} MiB >= {catch_up_above_mb} MiB")
    if status.wal_status == "unreserved":
        return Decision("catch_up", "slot is unreserved (past max_slot_wal_keep_size)")
    if last_catchup_at is None:
        return Decision("catch_up", "no catch-up recorded in slot_health.jsonl")
    age = now - last_catchup_at
    if age >= timedelta(hours=max_hours_between_catchups):
        return Decision("catch_up", f"last catch-up {age.total_seconds() / 3600:.1f} h ago")
    return Decision("skip", f"retained WAL {retained / MIB:.1f} MiB < {catch_up_above_mb} MiB, last catch-up {age} ago")


def caught_up(start_lsn: str | None, status: SlotStatus) -> bool:
    """The slot confirmed at least the WAL that existed when the catch-up started and retains < 16 MiB."""
    if status.wal_status in (None, "lost"):
        return False
    if start_lsn and (status.confirmed_flush_lsn is None or lsn_int(status.confirmed_flush_lsn) < lsn_int(start_lsn)):
        return False
    return (status.retained_bytes or 0) < CAUGHT_UP_BELOW


def confirmed_past(target: str | None, status: SlotStatus) -> bool:
    return bool(target and status.confirmed_flush_lsn and lsn_int(status.confirmed_flush_lsn) >= lsn_int(target))


def target_lsn(start_lsn: str | None, touch_lsn: str | None) -> str | None:
    """The LSN the slot must confirm: the later of the WAL position at the start and the touch."""
    lsns = [x for x in (start_lsn, touch_lsn) if x]
    return max(lsns, key=lsn_int) if lsns else None


def lsn_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


# ---------------------------------------------------------------- slot_health.jsonl


def health_path() -> Path:
    return alerts.data_dir() / "metrics" / "slot_health.jsonl"


def last_catchup(path: Path | None = None) -> datetime | None:
    path = path or health_path()
    if not path.is_file():
        return None
    last = None
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("action") == "caught_up":
            last = datetime.fromisoformat(rec["at"])
    return last


def health_line(
    dag_run_id: str | None,
    action: str,
    reason: str,
    before: SlotStatus,
    after: SlotStatus | None,
    connect_seconds: float | None,
    now: datetime | None = None,
    touch_lsn: str | None = None,
) -> dict:
    after = after or before
    line = {
        "at": (now or datetime.now(UTC)).isoformat(timespec="seconds"),
        "dag_run_id": dag_run_id,
        "action": action,
        "reason": reason,
        "wal_status_before": before.wal_status,
        "retained_before_bytes": before.retained_bytes,
        "safe_wal_size_before": before.safe_wal_size,
        "wal_status_after": after.wal_status,
        "retained_after_bytes": after.retained_bytes,
        "safe_wal_size_after": after.safe_wal_size,
        "connect_seconds": connect_seconds,
    }
    if touch_lsn:
        line["touch_lsn"] = touch_lsn
    return line


def append_health(line: dict, path: Path | None = None) -> None:
    path = path or health_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, default=str) + "\n")


# ---------------------------------------------------------------- Postgres


SLOT_SQL = """SELECT wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, safe_wal_size,
       confirmed_flush_lsn::text, pg_current_wal_lsn()::text, active
FROM pg_replication_slots WHERE slot_name = %s"""


def inspect_slot(dsn: str, slot: str = SLOT) -> SlotStatus:
    import psycopg

    with psycopg.connect(dsn, connect_timeout=10, autocommit=True) as conn:
        r = conn.execute(SLOT_SQL, (slot,)).fetchone()
        if r is None:
            lsn = conn.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
            return SlotStatus(None, None, None, None, lsn)
    return SlotStatus(r[0], r[1], r[2], r[3], r[4], bool(r[5]))


# One real change to a captured table (the publication lists exactly the five ops tables), so Debezium has an event
# past its stored offset and confirms the slot. Returns the WAL position of the change.
TOUCH_SQL = (
    "UPDATE vans SET updated_at = now() WHERE van_id = (SELECT min(van_id) FROM vans) "
    "RETURNING pg_current_wal_lsn()::text"
)


def touch_captured_row(dsn: str) -> str:
    """Autocommit TOUCH_SQL on ops-db; the LSN of the touch. Fails clearly when vans is empty."""
    import psycopg

    with psycopg.connect(dsn, connect_timeout=10, autocommit=True) as conn:
        r = conn.execute(TOUCH_SQL).fetchone()
    if r is None:
        raise KeepaliveError(
            "ops.vans is empty: the keepalive needs one van row to touch (a captured change is the only thing that "
            "makes Debezium confirm the slot); seed the ops DB first"
        )
    return r[0]


# ---------------------------------------------------------------- Kafka Connect (existing container only)


def connect_container(client):
    """The existing compose container of service `connect` (by labels). Never created here."""
    found = client.containers.list(
        all=True,
        filters={"label": [f"com.docker.compose.project={PROJECT}", "com.docker.compose.service=connect"]},
    )
    if not found:
        raise KeepaliveError(
            "no Kafka Connect container exists: run python tasks.py connect-up once (it creates the container and "
            "registers the connector); the keepalive only starts and stops that container"
        )
    return found[0]


def connector_running(connect_url: str, timeout: float = 10) -> bool:
    try:
        with urllib.request.urlopen(f"{connect_url.rstrip('/')}/connectors/{CONNECTOR}/status", timeout=timeout) as r:
            st = json.loads(r.read())
    except (urllib.error.URLError, OSError, ValueError):
        return False
    tasks = st.get("tasks") or []
    failed = [t for t in tasks if t.get("state") == "FAILED"]
    if failed:
        raise KeepaliveError(f"connector task failed: {str(failed[0].get('trace', ''))[:1500]}")
    return (st.get("connector") or {}).get("state") == "RUNNING" and bool(tasks)


def catch_up(
    client,
    inspect: Callable[[], SlotStatus],
    connect_url: str,
    timeout_s: float = DEFAULTS["timeout_s"],
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    touch: Callable[[], str] | None = None,
) -> tuple[SlotStatus, float]:
    """Start the existing `connect` container, wait RUNNING, `touch()` one captured row (returns its LSN), wait until
    the slot confirmed max(start LSN, touch LSN) and retains < 16 MiB (touching again while a confirmed touch leaves
    retained >= 16 MiB, see the module doc), always stop it. Everything counts against timeout_s. Returns (after,
    seconds); raises KeepaliveError on timeout."""
    start = inspect()
    box = connect_container(client)
    t0 = clock()
    deadline = t0 + timeout_s
    status = start
    try:
        print(f"docker start {box.name}", flush=True)
        box.start()
        while not connector_running(connect_url):
            if clock() > deadline:
                raise KeepaliveError(f"connector {CONNECTOR} not RUNNING after {timeout_s:.0f} s")
            sleep(3)
        touch_lsn = touch() if touch else None
        touches, touched_at = (1, clock()) if touch else (0, None)
        target = target_lsn(start.current_lsn, touch_lsn)
        print(
            f"connector {CONNECTOR} RUNNING; touched a captured row at {touch_lsn}; waiting for the slot to confirm "
            f"{target}",
            flush=True,
        )
        while True:
            status = inspect()
            if caught_up(target, status):
                break
            if clock() > deadline:
                raise KeepaliveError(
                    f"slot {SLOT} still retains {(status.retained_bytes or 0) / MIB:.1f} MiB after {timeout_s:.0f} s: "
                    f"confirmed_flush_lsn stuck at {status.confirmed_flush_lsn}, target {target} "
                    f"(start {start.current_lsn}, touch {touch_lsn}, {touches} touches)"
                )
            if touch and confirmed_past(target, status) and clock() - touched_at >= RETOUCH_AFTER_S:
                touch_lsn = touch()
                touches, touched_at = touches + 1, clock()
                target = target_lsn(target, touch_lsn)
                print(
                    f"confirmed {status.confirmed_flush_lsn} but restart_lsn still retains "
                    f"{(status.retained_bytes or 0) / MIB:.1f} MiB; touch {touches} at {touch_lsn}",
                    flush=True,
                )
            sleep(5)
        print(
            f"caught up: confirmed {status.confirmed_flush_lsn}, retains {status.retained_bytes} B, {touches} touches",
            flush=True,
        )
    finally:
        print(f"docker stop {box.name}", flush=True)
        box.stop(timeout=60)
    return status, round(clock() - t0, 1)


def to_dict(status: SlotStatus) -> dict:
    return asdict(status)


def from_dict(d: dict) -> SlotStatus:
    return SlotStatus(**d)
