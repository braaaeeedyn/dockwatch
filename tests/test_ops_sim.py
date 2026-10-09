import json

import pytest

from dockwatch.ops_sim.model import Alert, Model, Params, Station, expected_events, parse_alert
from dockwatch.ops_sim.synthetic import SeededSimulation
from dockwatch.streaming.episodes import EpisodeState, Thresholds, advance, episode_id

T = 1_790_812_800
QUIET = dict(cancel_per_tick=0.0, ticket_per_tick=0.0, station_toggle_per_tick=0.0)


def _station(sid="s1"):
    return Station(sid, f"Station {sid}", None, 37.77, -122.42, 15)


def _alert(action, kind="empty", sid="s1", start=T, ts=None):
    ts = ts if ts is not None else start + 15 * 60
    eid = episode_id(sid, kind, start)
    return Alert(f"{eid}:{action}", eid, sid, kind, action, start, ts - start, ts)


def _model(**params):
    m = Model(seed=1, params=Params(vans=1, **{**QUIET, **params}))
    m.setup([_station()], ["V1"])
    return m


def _minute(m, minute, alerts=()):
    """Step the model to `minute` minutes after the episode start T."""
    return m.step(minute, T + minute * 60, list(alerts))


def _actions(ops):
    return [op.action for op in ops]


def test_same_seed_gives_the_same_operations():
    a = SeededSimulation(seed=42, events=300).all_ops()
    b = SeededSimulation(seed=42, events=300).all_ops()
    c = SeededSimulation(seed=7, events=300).all_ops()
    assert a == b
    assert len(a) > 100
    assert a != c


def test_job_opens_after_20_minutes_empty_not_before():
    m = _model()
    assert "open_job" not in _actions(_minute(m, 15, [_alert("raised")]))
    for minute in range(16, 20):
        assert "open_job" not in _actions(_minute(m, minute)), minute
    ops = _minute(m, 20)
    assert _actions(ops) == ["open_job", "van_status"]
    job = ops[0].values
    assert (job["station_id"], job["status"], job["plate"]) == ("s1", "assigned", "V1")
    assert job["alert_episode_id"] == episode_id("s1", "empty", T)
    assert job["opened_at"] == T + 20 * 60
    assert ops[1].values == {"plate": "V1", "status": "busy"}
    assert "open_job" not in _actions(_minute(m, 21))  # one job per episode


def test_resolved_alert_closes_the_open_job():
    m = _model()
    _minute(m, 15, [_alert("raised")])
    _minute(m, 20)
    ops = _minute(m, 47, [_alert("resolved", ts=T + 47 * 60)])
    assert _actions(ops) == ["update_job", "van_status"]
    job = ops[0].values
    assert (job["status"], job["closed_at"]) == ("done", T + 47 * 60)
    assert 3 <= job["bikes_moved"] <= 15
    assert ops[1].values == {"plate": "V1", "status": "idle"}
    assert m.jobs == {}


def test_cancelled_jobs_are_deleted():
    m = _model(cancel_per_tick=1.0, delete_after=(5, 5))
    _minute(m, 15, [_alert("raised")])
    ops = _minute(m, 20)
    assert _actions(ops) == ["open_job", "van_status", "update_job", "van_status"]
    assert (ops[2].values["status"], ops[2].values["closed_at"]) == ("cancelled", T + 20 * 60)
    for minute in range(21, 25):
        assert "delete_job" not in _actions(_minute(m, minute))
    ops = _minute(m, 25)
    assert _actions(ops) == ["delete_job"]
    assert ops[0].values == {"alert_episode_id": episode_id("s1", "empty", T)}
    assert (ops[0].table, ops[0].kind) == ("rebalancing_jobs", "d")
    assert _minute(m, 40, [_alert("resolved", ts=T + 40 * 60)]) == []  # nothing left to close, no new job


def test_full_station_does_not_open_a_job():
    m = _model()
    _minute(m, 15, [_alert("raised", kind="full")])
    for minute in range(16, 120):
        assert _minute(m, minute) == [], minute
    assert _minute(m, 120, [_alert("resolved", kind="full", ts=T + 120 * 60)]) == []


def test_seeded_run_changes_every_table_and_deletes_jobs():
    sim = SeededSimulation(seed=42, events=1500)
    ops = sim.all_ops()
    events = expected_events(ops)
    for table, counts in events.items():
        assert counts["c"] > 0, table
        assert counts["u"] > 0, table
    assert events["rebalancing_jobs"]["d"] >= 10
    assert sum(sum(c.values()) for c in events.values()) <= 15_000
    jobs = [op for op in ops if op.table == "rebalancing_jobs" and op.kind != "d"]
    assert all("priority" not in op.values for op in jobs if op.tick < sim.migrate_at)
    assert all(op.values.get("priority") in (1, 2, 3) for op in jobs if op.tick >= sim.migrate_at)
    assert any(op.tick >= sim.migrate_at for op in jobs)


def test_live_alert_messages_are_parsed():
    # A real alert from the M2 episode state machine, as the Spark job publishes it to gbfs.alerts.
    rows = [{"snapshot_ts": T + i * 60, "state": "empty"} for i in range(17)]
    events = advance("sf-123", EpisodeState(), rows, Thresholds(alert_after_s=15 * 60))
    raised = next(e for e in events if e["type"] == "alert")
    alert = parse_alert(json.dumps(raised).encode("utf-8"))
    assert alert == Alert(
        alert_id=f"sf-123:empty:{T}:raised",
        episode_id=f"sf-123:empty:{T}",
        station_id="sf-123",
        kind="empty",
        action="raised",
        start_ts=T,
        duration_s=15 * 60,
        ts=T + 15 * 60,
    )
    # The synthetic stream publishes exactly the same payload shape.
    sim = SeededSimulation(seed=42, events=300)
    synthetic = next(a for tick in sorted(sim.alerts) for a in sim.alerts[tick])
    assert set(synthetic) == set(raised)
    for bad in (b"not json", b"[]", json.dumps({**raised, "action": "maybe"}), json.dumps({"alert_id": "x"})):
        with pytest.raises(ValueError, match="alert"):
            parse_alert(bad)
