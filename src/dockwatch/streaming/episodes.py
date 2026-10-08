"""Station state rule and the empty/full episode state machine, in plain Python.

Spark calls these from applyInPandasWithState (one call per station per micro-batch); keeping them free of Spark
means they are unit-tested directly. Must stay Python 3.10 compatible (the Spark image runs 3.10).
"""

from dataclasses import dataclass

# Order matters: the first rule that matches wins. The site's legend uses the same words (DESIGN.md §2).
STATES = ("offline", "empty", "full", "low", "high", "ok")
PROBLEM_KINDS = ("empty", "full")  # states that open an episode


@dataclass(frozen=True)
class Thresholds:
    stale_after_s: int = 30 * 60  # no report for 30 min -> offline
    low_count: int = 2  # <= 2 bikes (or docks) -> low (high)
    low_share: float = 0.10  # or <= 10% of capacity
    alert_after_s: int = 15 * 60  # an episode this long raises an alert


DEFAULT_THRESHOLDS = Thresholds()


def capacity_of(bikes: int, bikes_disabled: int, docks: int, docks_disabled: int) -> int:
    """station_status has no capacity field; every dock holds a bike or is free, working or not."""
    return bikes + (bikes_disabled or 0) + docks + (docks_disabled or 0)


def classify(
    *,
    snapshot_ts: int,
    last_reported: int,
    is_installed: bool,
    is_renting: bool,
    bikes: int,
    docks: int,
    capacity: int,
    t: Thresholds = DEFAULT_THRESHOLDS,
) -> str:
    if not is_installed or not is_renting or snapshot_ts - last_reported > t.stale_after_s:
        return "offline"
    if bikes == 0:
        return "empty"
    if docks == 0:
        return "full"
    if bikes <= t.low_count or bikes <= t.low_share * capacity:
        return "low"
    if docks <= t.low_count or docks <= t.low_share * capacity:
        return "high"
    return "ok"


@dataclass
class EpisodeState:
    """What Spark keeps per station between micro-batches."""

    kind: str | None = None  # "empty" / "full" while an episode is open
    start_ts: int | None = None
    last_ts: int = -1  # newest snapshot already processed (older or equal rows are duplicates / late)
    alerted: bool = False


def episode_id(station_id: str, kind: str, start_ts: int) -> str:
    return f"{station_id}:{kind}:{start_ts}"


def advance(station_id: str, state: EpisodeState, rows: list[dict], t: Thresholds = DEFAULT_THRESHOLDS) -> list[dict]:
    """Feed one station's new rows (any order) through the state machine; returns episode and alert events.

    Each row needs `snapshot_ts` and `state`. Rows at or before `state.last_ts` are skipped, which also makes
    re-delivered duplicates harmless. Events:
      {"type": "episode", status open|closed, ...}  one per change, plus an update per batch while open
      {"type": "alert", action raised|resolved, ...}
    """
    events: list[dict] = []

    def episode_event(status: str, end_ts: int | None, now: int) -> dict:
        return {
            "type": "episode",
            "episode_id": episode_id(station_id, state.kind, state.start_ts),
            "station_id": station_id,
            "kind": state.kind,
            "start_ts": state.start_ts,
            "end_ts": end_ts,
            "duration_s": (end_ts or now) - state.start_ts,
            "status": status,
            "alerted": state.alerted,
            "ts": now,
        }

    def alert_event(action: str, now: int) -> dict:
        return {
            "type": "alert",
            "alert_id": f"{episode_id(station_id, state.kind, state.start_ts)}:{action}",
            "episode_id": episode_id(station_id, state.kind, state.start_ts),
            "station_id": station_id,
            "kind": state.kind,
            "action": action,
            "start_ts": state.start_ts,
            "duration_s": now - state.start_ts,
            "ts": now,
        }

    touched = False
    for row in sorted(rows, key=lambda r: r["snapshot_ts"]):
        ts = int(row["snapshot_ts"])
        if ts <= state.last_ts:
            continue
        kind = row["state"] if row["state"] in PROBLEM_KINDS else None
        if state.kind is not None and kind != state.kind:
            events.append(episode_event("closed", ts, ts))
            if state.alerted:
                events.append(alert_event("resolved", ts))
            state.kind, state.start_ts, state.alerted = None, None, False
            touched = False
        if kind is not None and state.kind is None:
            state.kind, state.start_ts, state.alerted = kind, ts, False
            touched = True
        if state.kind is not None:
            touched = True
            if not state.alerted and ts - state.start_ts >= t.alert_after_s:
                state.alerted = True
                events.append(alert_event("raised", ts))
        state.last_ts = ts

    if state.kind is not None and touched:
        events.append(episode_event("open", None, state.last_ts))
    return events
