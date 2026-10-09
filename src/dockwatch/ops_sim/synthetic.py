"""Seeded synthetic inputs for the bounded simulation: stations, vans, and a `gbfs.alerts` stream on a simulated clock.

One tick = one simulated minute starting at T0. The alert stream follows the M2 episode rules
(streaming/episodes.py): an empty / full episode raises its alert after 15 min and resolves it when it ends;
episodes shorter than that never alert. Messages have exactly the `gbfs.alerts` payload.
"""

import random
from dataclasses import dataclass, field

from dockwatch.ops_sim.model import Alert, Model, Op, Params, Station, expected_events, parse_alert
from dockwatch.streaming.episodes import episode_id

T0 = 1_790_812_800  # 2026-10-01T00:00:00Z
TICK_S = 60
ALERT_AFTER_S = 15 * 60
EPISODE_START_PER_MIN = 0.0022  # chance per station-minute that a free station starts an episode
EMPTY_SHARE = 0.65
EPISODE_MIN = (5, 120)  # episode length in minutes


def tick_ts(tick: int) -> int:
    return T0 + tick * TICK_S


def stations(seed: int, n: int = 80) -> list[Station]:
    rng = random.Random(f"dockwatch-ops-stations:{seed}")
    out = []
    for i in range(1, n + 1):
        out.append(
            Station(
                station_id=f"sim-{i:04d}",
                name=f"Sim Station {i}",
                short_name=f"S{i:04d}",
                lat=round(rng.uniform(37.70, 37.81), 6),
                lon=round(rng.uniform(-122.52, -122.38), 6),
                capacity=rng.randint(11, 35),
                region_id=rng.choice(("3", "5", "12")),
            )
        )
    return out


def plates(n: int) -> list[str]:
    return [f"DW-{i:03d}" for i in range(1, n + 1)]


def alert_stream(seed: int, station_list: list[Station], ticks: int) -> dict[int, list[dict]]:
    """tick -> the gbfs.alerts messages published in that minute (as dicts)."""
    rng = random.Random(f"dockwatch-ops-alerts:{seed}")
    busy_until: dict[str, int] = {}
    out: dict[int, list[dict]] = {}

    def emit(at: int, sid: str, kind: str, start: int, action: str) -> None:
        eid = episode_id(sid, kind, tick_ts(start))
        ts = tick_ts(at)
        out.setdefault(at, []).append(
            {
                "type": "alert",
                "alert_id": f"{eid}:{action}",
                "episode_id": eid,
                "station_id": sid,
                "kind": kind,
                "action": action,
                "start_ts": tick_ts(start),
                "duration_s": ts - tick_ts(start),
                "ts": ts,
            }
        )

    for tick in range(ticks):
        for s in station_list:
            if busy_until.get(s.station_id, -1) > tick or rng.random() >= EPISODE_START_PER_MIN:
                continue
            kind = "empty" if rng.random() < EMPTY_SHARE else "full"
            minutes = rng.randint(*EPISODE_MIN)
            busy_until[s.station_id] = tick + minutes + 1
            raise_at = tick + ALERT_AFTER_S // TICK_S
            if minutes > ALERT_AFTER_S // TICK_S:
                emit(raise_at, s.station_id, kind, tick, "raised")
                emit(tick + minutes, s.station_id, kind, tick, "resolved")
    return out


@dataclass
class SeededSimulation:
    """The bounded, deterministic run: setup ops, then `ops_for(tick)` for ticks 0..events-1 in order."""

    seed: int = 42
    events: int = 1500
    n_stations: int = 80
    params: Params = field(default_factory=Params)

    def __post_init__(self) -> None:
        self.stations = stations(self.seed, self.n_stations)
        self.model = Model(self.seed, self.params)
        self.alerts = alert_stream(self.seed, self.stations, self.events)

    @property
    def migrate_at(self) -> int:
        """Tick at which migration 001 (rebalancing_jobs.priority) is applied."""
        return self.events // 2

    def setup_ops(self) -> list[Op]:
        return self.model.setup(self.stations, plates(self.params.vans))

    def alerts_at(self, tick: int) -> list[Alert]:
        return [parse_alert(a) for a in self.alerts.get(tick, [])]

    def ops_for(self, tick: int) -> list[Op]:
        return self.model.step(tick, tick_ts(tick), self.alerts_at(tick))

    def all_ops(self) -> list[Op]:
        """Every op of the run without a database (the model switches to priority at migrate_at)."""
        ops = self.setup_ops()
        for tick in range(self.events):
            if tick == self.migrate_at:
                self.model.with_priority = True
            ops += self.ops_for(tick)
        return ops


def summary(ops: list[Op]) -> dict:
    return {"ops": len(ops), "expected_events": expected_events(ops)}
