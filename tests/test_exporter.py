from dockwatch.exporter.__main__ import read_previous, write_json
from dockwatch.exporter.build import build_alerts, build_live, carry_generated_at, has_new_data, view_for

INFOS = {
    "a": {"name": "Market St", "short_name": "SF-1", "lat": 37.79, "lon": -122.40, "region_id": "3", "capacity": 20},
    "b": {"name": "Lake Merritt", "short_name": "OK-1", "lat": 37.80, "lon": -122.26, "region_id": "12"},
}


def _state(sid, state, bikes, snap=1000, **kw):
    return {
        "station_id": sid,
        "state": state,
        "num_bikes_available": bikes,
        "num_docks_available": 5,
        "snapshot_epoch": snap,
        "last_reported_epoch": snap - 30,
        **kw,
    }


def test_view_uses_region_then_coordinates():
    assert view_for("12", None, None) == "eastbay"
    assert view_for(None, 37.33, -121.89) == "sj"
    assert view_for(None, 37.87, -122.27) == "eastbay"
    assert view_for(None, 37.77, -122.42) == "sf"


def test_live_counts_sorting_and_missing_information():
    states = {
        "a": _state("a", "ok", 8),
        "b": _state("b", "empty", 0, snap=1060, episode_start=900),
        "c": _state("c", "full", 12),  # no station_information row, like some live stations
    }
    live = build_live(states, INFOS, now=1100)
    assert [s["id"] for s in live["stations"]] == ["b", "c", "a"]  # problems first
    assert live["counts"]["all"]["empty"] == 1
    assert live["counts"]["eastbay"]["empty"] == 1
    assert live["data_as_of"] == "1970-01-01T00:17:40Z"
    assert live["bikes_available"] == 20
    unknown = next(s for s in live["stations"] if s["id"] == "c")
    assert unknown["name"] == "Unknown station"
    assert next(s for s in live["stations"] if s["id"] == "b")["state_since"] == "1970-01-01T00:15:00Z"


def _alert(sid, action, ts, start=0):
    ep = f"{sid}:empty:{start}"
    return {
        "alert_id": f"{ep}:{action}",
        "episode_id": ep,
        "station_id": sid,
        "kind": "empty",
        "action": action,
        "start_ts": start,
        "duration_s": ts - start,
        "ts": ts,
    }


def test_alerts_split_open_and_resolved():
    alerts = {
        a["alert_id"]: a
        for a in (_alert("a", "raised", 900), _alert("a", "resolved", 1500), _alert("b", "raised", 1000, 100))
    }
    doc = build_alerts(alerts, INFOS, now=2000)
    assert [r["station_id"] for r in doc["open"]] == ["b"]
    assert doc["resolved"][0]["duration_s"] == 1500
    assert doc["open"][0]["name"] == "Lake Merritt"


def test_generated_at_does_not_advance_without_new_data():
    states = {"a": _state("a", "ok", 8), "b": _state("b", "empty", 0, snap=1060)}
    first = carry_generated_at(build_live(states, INFOS, now=1100), None)
    second = carry_generated_at(build_live(states, INFOS, now=1400), first)
    assert first["generated_at"] == "1970-01-01T00:18:20Z"
    assert second["generated_at"] == first["generated_at"]
    assert not has_new_data(second, first)  # nothing new: the exporter does not rewrite the files
    alerts = build_alerts({}, INFOS, now=1400, generated_at=second["generated_at"])
    assert alerts["generated_at"] == first["generated_at"]  # live.json and alerts.json never disagree


def test_generated_at_advances_when_data_as_of_moves():
    first = carry_generated_at(build_live({"a": _state("a", "ok", 8)}, INFOS, now=1100), None)
    moved = {"a": _state("a", "ok", 7, snap=1360)}
    second = carry_generated_at(build_live(moved, INFOS, now=1400), first)
    assert second["data_as_of"] == "1970-01-01T00:22:40Z"
    assert second["generated_at"] == "1970-01-01T00:23:20Z"
    assert has_new_data(second, first)
    assert has_new_data(first, None)  # first export after a clean start is always written


def test_has_new_data_ignores_older_or_missing_data():
    newer = build_live({"a": _state("a", "ok", 8, snap=2000)}, INFOS, now=2100)
    older = build_live({"a": _state("a", "ok", 8, snap=1000)}, INFOS, now=2200)
    assert not has_new_data(older, newer)  # still catching up after a restart: do not go backwards
    assert not has_new_data(build_live({}, INFOS, now=2200), newer)
    assert has_new_data(newer, {"generated_at": "x", "data_as_of": "garbage"})


def test_generated_at_kept_from_previous_export_after_restart(tmp_path):
    states = {"a": _state("a", "ok", 8)}
    path = tmp_path / "live.json"
    write_json(path, build_live(states, INFOS, now=1100))
    previous = read_previous(path)
    after_restart = carry_generated_at(build_live(states, INFOS, now=5000), previous)
    assert after_restart["generated_at"] == "1970-01-01T00:18:20Z"
    assert not has_new_data(after_restart, previous)

    assert read_previous(tmp_path / "missing.json") is None
    garbage = tmp_path / "garbage.json"
    garbage.write_text("{not json", encoding="utf-8")
    assert read_previous(garbage) is None
    assert carry_generated_at(build_live(states, INFOS, now=5000), None)["generated_at"] == "1970-01-01T01:23:20Z"
