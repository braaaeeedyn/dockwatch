from dockwatch.exporter.build import build_alerts, build_live, view_for

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
