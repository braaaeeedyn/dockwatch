"""The simulator's rules, as pure functions of alerts and a simulated clock (no I/O, no wall clock).

`Model.step(tick, now_ts, alerts)` returns the `Op`s an operator would make in that minute:
- an *empty* alert episode that has been open for >= 20 min, with no open job at the station, opens a rebalancing
  job (assigned to the first idle van if there is one; otherwise it waits for a van);
- the episode's resolved alert closes its job (`done`, with `bikes_moved`) and releases the van;
- *full* alerts open nothing;
- each minute an unfinished job may be cancelled (seeded); a cancelled job is **deleted** on a later tick;
- maintenance tickets open on random docks (dock -> out_of_service), progress, and close (dock -> ok);
- now and then a station is switched off / on (is_active).

All randomness comes from one `random.Random` seeded from the seed, and every loop runs over sorted keys, so the
same seed and the same alerts give the same operations. Times are integer epoch seconds (UTC).
"""

import json
import random
from dataclasses import dataclass, field

JOB_AFTER_S = 20 * 60  # an empty episode must be this old before a job opens
TICKET_ISSUES = ("jammed", "broken_lock", "no_power", "damaged", "other")
TABLES = ("stations", "docks", "vans", "rebalancing_jobs", "maintenance_tickets")

# action -> (table, Debezium op letter: c = insert, u = update, d = delete)
ACTIONS = {
    "insert_station": ("stations", "c"),
    "upsert_station": ("stations", "c"),  # live mode: insert or update, counted from the database's answer
    "update_station": ("stations", "u"),
    "insert_docks": ("docks", "c"),
    "dock_status": ("docks", "u"),
    "insert_van": ("vans", "c"),
    "van_status": ("vans", "u"),
    "open_job": ("rebalancing_jobs", "c"),
    "update_job": ("rebalancing_jobs", "u"),
    "delete_job": ("rebalancing_jobs", "d"),
    "open_ticket": ("maintenance_tickets", "c"),
    "update_ticket": ("maintenance_tickets", "u"),
}


@dataclass(frozen=True)
class Op:
    tick: int
    action: str
    values: dict
    rows: int = 1  # rows this op changes (insert_docks inserts one row per dock)

    @property
    def table(self) -> str:
        return ACTIONS[self.action][0]

    @property
    def kind(self) -> str:
        return ACTIONS[self.action][1]


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    short_name: str | None
    lat: float
    lon: float
    capacity: int
    region_id: str | None = None
    is_active: bool = True

    def values(self) -> dict:
        return {
            "station_id": self.station_id,
            "name": self.name,
            "short_name": self.short_name,
            "lat": self.lat,
            "lon": self.lon,
            "capacity": self.capacity,
            "region_id": self.region_id,
            "is_active": self.is_active,
        }


@dataclass(frozen=True)
class Alert:
    """One `gbfs.alerts` message (streaming/episodes.py alert_event)."""

    alert_id: str
    episode_id: str
    station_id: str
    kind: str  # empty / full
    action: str  # raised / resolved
    start_ts: int
    duration_s: int
    ts: int


def parse_alert(raw: bytes | str | dict) -> Alert:
    """Parse a gbfs.alerts value; raises ValueError on anything that is not a well-formed alert."""
    try:
        doc = json.loads(raw) if isinstance(raw, bytes | str) else raw
        if not isinstance(doc, dict):
            raise ValueError("alert is not a JSON object")
        alert = Alert(
            alert_id=str(doc["alert_id"]),
            episode_id=str(doc["episode_id"]),
            station_id=str(doc["station_id"]),
            kind=str(doc["kind"]),
            action=str(doc["action"]),
            start_ts=int(doc["start_ts"]),
            duration_s=int(doc["duration_s"]),
            ts=int(doc["ts"]),
        )
    except (KeyError, TypeError, json.JSONDecodeError) as e:
        raise ValueError(f"not a gbfs.alerts message: {e!r}") from e
    if alert.action not in ("raised", "resolved"):
        raise ValueError(f"unknown alert action {alert.action!r}")
    return alert


@dataclass
class Params:
    vans: int = 4
    van_capacity: int = 20
    cancel_per_tick: float = 0.004  # chance per minute that an unfinished job is cancelled
    delete_after: tuple[int, int] = (5, 60)  # a cancelled job is deleted this many minutes later
    ticket_per_tick: float = 0.09  # chance per minute that a new maintenance ticket opens
    progress_per_tick: float = 0.08  # open -> in_progress
    close_per_tick: float = 0.06  # in_progress -> closed
    station_toggle_per_tick: float = 0.01  # a station is switched off / on


@dataclass
class Job:
    station_id: str
    episode_id: str
    status: str  # open / assigned
    plate: str | None
    opened_at: int
    priority: int | None = None

    def values(self, **changes) -> dict:
        out = {
            "station_id": self.station_id,
            "alert_episode_id": self.episode_id,
            "status": self.status,
            "plate": self.plate,
            "opened_at": self.opened_at,
            "closed_at": None,
            "bikes_moved": None,
        }
        out.update(changes)
        return out


@dataclass
class Model:
    seed: int
    params: Params = field(default_factory=Params)
    with_priority: bool = False  # set once migration 001 (rebalancing_jobs.priority) has been applied

    def __post_init__(self) -> None:
        self.rng = random.Random(f"dockwatch-ops-model:{self.seed}")
        self.stations: dict[str, Station] = {}
        self.station_ids: list[str] = []  # sorted, for seeded choices
        self.vans: dict[str, str] = {}  # plate -> idle / busy
        self.open_empty: dict[str, Alert] = {}  # station -> raised empty alert of the open episode
        self.jobs: dict[str, Job] = {}  # station -> its unfinished job
        self.handled: set[str] = set()  # episode ids that already had a job (membership only)
        self.to_delete: list[tuple[int, str]] = []  # (tick, episode id) of cancelled jobs
        self.tickets: dict[tuple[str, int], str] = {}  # (station, dock number) -> open / in_progress

    # ------------------------------------------------------------------ setup

    def setup(self, stations: list[Station], plates: list[str], tick: int = 0) -> list[Op]:
        ops = [op for s in stations for op in self.add_station(s, tick, action="insert_station")]
        for plate in plates:
            self.vans[plate] = "idle"
            ops.append(Op(tick, "insert_van", {"plate": plate, "capacity": self.params.van_capacity, "status": "idle"}))
        return ops

    def add_station(self, station: Station, tick: int, action: str = "upsert_station") -> list[Op]:
        known = self.stations.get(station.station_id)
        self.stations[station.station_id] = station
        self.station_ids = sorted(self.stations)
        ops = [Op(tick, action, station.values())]
        if known is None or station.capacity > known.capacity:
            ops.append(
                Op(
                    tick,
                    "insert_docks",
                    {"station_id": station.station_id, "count": station.capacity},
                    rows=station.capacity - (known.capacity if known else 0),
                )
            )
        return ops

    # ------------------------------------------------------------------ one simulated minute

    def step(self, tick: int, now_ts: int, alerts: list[Alert]) -> list[Op]:
        ops: list[Op] = []
        self._deletes(tick, ops)
        for alert in sorted(alerts, key=lambda a: (a.ts, a.alert_id)):
            self._alert(tick, now_ts, alert, ops)
        self._open_jobs(tick, now_ts, ops)
        self._assign_waiting(tick, ops)
        self._cancel(tick, now_ts, ops)
        self._tickets(tick, now_ts, ops)
        self._toggle_station(tick, ops)
        return ops

    def _priority(self, job: Job) -> dict:
        if not self.with_priority:
            return {}
        if job.priority is None:
            job.priority = self.rng.randint(1, 3)
        return {"priority": job.priority}

    def _idle_van(self) -> str | None:
        return next((p for p in sorted(self.vans) if self.vans[p] == "idle"), None)

    def _set_van(self, tick: int, plate: str, status: str, ops: list[Op]) -> None:
        self.vans[plate] = status
        ops.append(Op(tick, "van_status", {"plate": plate, "status": status}))

    def _deletes(self, tick: int, ops: list[Op]) -> None:
        due = [e for t, e in self.to_delete if t <= tick]
        self.to_delete = [(t, e) for t, e in self.to_delete if t > tick]
        for episode in due:
            ops.append(Op(tick, "delete_job", {"alert_episode_id": episode}))

    def _alert(self, tick: int, now_ts: int, alert: Alert, ops: list[Op]) -> None:
        if alert.kind != "empty":
            return  # full stations need no van: riders return bikes elsewhere
        if alert.action == "raised":
            self.open_empty[alert.station_id] = alert
            return
        open_alert = self.open_empty.get(alert.station_id)
        if open_alert is not None and open_alert.episode_id == alert.episode_id:
            del self.open_empty[alert.station_id]
        job = self.jobs.get(alert.station_id)
        if job is None or job.episode_id != alert.episode_id:
            return
        del self.jobs[alert.station_id]
        bikes = self.rng.randint(3, 15) if job.plate else 0
        job.status = "done"
        ops.append(Op(tick, "update_job", {**job.values(closed_at=now_ts, bikes_moved=bikes), **self._priority(job)}))
        if job.plate:
            self._set_van(tick, job.plate, "idle", ops)

    def _open_jobs(self, tick: int, now_ts: int, ops: list[Op]) -> None:
        for station_id in sorted(self.open_empty):
            alert = self.open_empty[station_id]
            if station_id in self.jobs or alert.episode_id in self.handled or now_ts - alert.start_ts < JOB_AFTER_S:
                continue
            plate = self._idle_van()
            job = Job(station_id, alert.episode_id, "assigned" if plate else "open", plate, now_ts)
            self.jobs[station_id] = job
            self.handled.add(alert.episode_id)
            ops.append(Op(tick, "open_job", {**job.values(), **self._priority(job)}))
            if plate:
                self._set_van(tick, plate, "busy", ops)

    def _assign_waiting(self, tick: int, ops: list[Op]) -> None:
        waiting = sorted(
            (j for j in self.jobs.values() if j.status == "open"), key=lambda j: (j.opened_at, j.station_id)
        )
        for job in waiting:
            plate = self._idle_van()
            if plate is None:
                return
            job.status, job.plate = "assigned", plate
            ops.append(Op(tick, "update_job", {**job.values(), **self._priority(job)}))
            self._set_van(tick, plate, "busy", ops)

    def _cancel(self, tick: int, now_ts: int, ops: list[Op]) -> None:
        for station_id in sorted(self.jobs):
            if self.rng.random() >= self.params.cancel_per_tick:
                continue
            job = self.jobs.pop(station_id)
            job.status = "cancelled"
            ops.append(Op(tick, "update_job", {**job.values(closed_at=now_ts), **self._priority(job)}))
            if job.plate:
                self._set_van(tick, job.plate, "idle", ops)
            lo, hi = self.params.delete_after
            self.to_delete.append((tick + self.rng.randint(lo, hi), job.episode_id))

    def _tickets(self, tick: int, now_ts: int, ops: list[Op]) -> None:
        p = self.params
        for dock in sorted(self.tickets):
            station_id, number = dock
            status = self.tickets[dock]
            if status == "open" and self.rng.random() < p.progress_per_tick:
                self.tickets[dock] = "in_progress"
                ops.append(
                    Op(
                        tick,
                        "update_ticket",
                        {"station_id": station_id, "dock_number": number, "status": "in_progress", "closed_at": None},
                    )
                )
            elif status == "in_progress" and self.rng.random() < p.close_per_tick:
                del self.tickets[dock]
                ops.append(
                    Op(
                        tick,
                        "update_ticket",
                        {"station_id": station_id, "dock_number": number, "status": "closed", "closed_at": now_ts},
                    )
                )
                ops.append(Op(tick, "dock_status", {"station_id": station_id, "dock_number": number, "status": "ok"}))
        if not self.station_ids or self.rng.random() >= p.ticket_per_tick:
            return
        station_id = self.rng.choice(self.station_ids)
        number = self.rng.randint(1, self.stations[station_id].capacity)
        issue = self.rng.choice(TICKET_ISSUES)
        if (station_id, number) in self.tickets:
            return
        self.tickets[(station_id, number)] = "open"
        ops.append(
            Op(
                tick,
                "open_ticket",
                {"station_id": station_id, "dock_number": number, "issue": issue, "opened_at": now_ts},
            )
        )
        ops.append(
            Op(tick, "dock_status", {"station_id": station_id, "dock_number": number, "status": "out_of_service"})
        )

    def _toggle_station(self, tick: int, ops: list[Op]) -> None:
        if not self.station_ids or self.rng.random() >= self.params.station_toggle_per_tick:
            return
        station_id = self.rng.choice(self.station_ids)
        s = self.stations[station_id]
        self.stations[station_id] = s = Station(**{**s.values(), "is_active": not s.is_active})
        ops.append(Op(tick, "update_station", {"station_id": station_id, "is_active": s.is_active}))


def expected_events(ops: list[Op]) -> dict[str, dict[str, int]]:
    """Change events Debezium should emit for these ops, per table and op letter (inserts and updates of one row)."""
    out = {t: {"c": 0, "u": 0, "d": 0} for t in TABLES}
    for op in ops:
        out[op.table][op.kind] += op.rows
    return out
