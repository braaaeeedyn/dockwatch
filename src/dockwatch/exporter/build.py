"""Build the site's live.json and alerts.json from the latest Kafka records (pure functions, no I/O)."""

from datetime import UTC, datetime

# Bay Wheels region ids (system_regions, 2026-10-07) -> the site's three map views (DESIGN.md §6).
REGION_NAMES = {"3": "San Francisco", "5": "San Jose", "12": "Oakland", "13": "Emeryville", "14": "Berkeley"}
REGION_VIEW = {"3": "sf", "5": "sj", "12": "eastbay", "13": "eastbay", "14": "eastbay"}
STATE_ORDER = ("empty", "full", "low", "high", "offline", "ok")
RESOLVED_KEPT = 20


def view_for(region_id: str | None, lat: float | None, lon: float | None) -> str:
    """Map view for a station. 35 stations have no region_id, so fall back to coordinates."""
    if region_id in REGION_VIEW:
        return REGION_VIEW[region_id]
    if lat is not None and lat < 37.5:
        return "sj"
    if lon is not None and lon > -122.36:
        return "eastbay"
    return "sf"


def iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat().replace("+00:00", "Z")


def build_live(states: dict[str, dict], infos: dict[str, dict], now: float) -> dict:
    """states: station_id -> latest dockwatch.station_state value; infos: station_id -> station_information."""
    stations = []
    for sid, st in states.items():
        info = infos.get(sid, {})  # some stations have status but no information row
        lat, lon = info.get("lat"), info.get("lon")
        stations.append(
            {
                "id": sid,
                "name": info.get("name") or "Unknown station",
                "code": info.get("short_name"),
                "lat": lat,
                "lon": lon,
                "region": REGION_NAMES.get(info.get("region_id")),
                "view": view_for(info.get("region_id"), lat, lon),
                "capacity": info.get("capacity") or st.get("capacity"),
                "bikes": st.get("num_bikes_available"),
                "ebikes": st.get("num_ebikes_available"),
                "docks": st.get("num_docks_available"),
                "state": st.get("state"),
                "state_since": iso(st.get("episode_start")),
                "last_reported": iso(st.get("last_reported_epoch")),
                "snapshot": iso(st.get("snapshot_epoch")),
            }
        )
    stations.sort(key=lambda s: (STATE_ORDER.index(s["state"]) if s["state"] in STATE_ORDER else 99, s["name"]))

    counts: dict[str, dict[str, int]] = {"all": dict.fromkeys(STATE_ORDER, 0)}
    for s in stations:
        for key in ("all", s["view"]):
            counts.setdefault(key, dict.fromkeys(STATE_ORDER, 0))
            if s["state"] in STATE_ORDER:
                counts[key][s["state"]] += 1

    snapshots = [st.get("snapshot_epoch") for st in states.values() if st.get("snapshot_epoch")]
    return {
        "generated_at": iso(now),
        "data_as_of": iso(max(snapshots)) if snapshots else None,
        "station_count": len(stations),
        "bikes_available": sum(s["bikes"] or 0 for s in stations),
        "counts": counts,
        "stations": stations,
        "attribution": "Bay Wheels station data, processed by DockWatch. Not affiliated with Lyft or Bay Wheels.",
    }


def carry_generated_at(live: dict, previous: dict | None) -> dict:
    """`generated_at` means "time of the last export that had new data". If the previous export has the same
    non-null `data_as_of`, keep its `generated_at`, so a stopped pipeline makes the site go Delayed -> Paused."""
    old = previous.get("data_as_of") if previous else None
    if old is not None and old == live.get("data_as_of") and previous.get("generated_at"):
        return live | {"generated_at": previous["generated_at"]}
    return live


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def has_new_data(live: dict, previous: dict | None) -> bool:
    """True if `live` should be written: there is no previous export, or `data_as_of` moved forward."""
    if previous is None:
        return True
    new, old = live.get("data_as_of"), previous.get("data_as_of")
    if new is None:
        return False
    try:
        return old is None or _parse(new) > _parse(old)
    except (TypeError, ValueError):  # a hand-edited or corrupt previous file: write a fresh one
        return True


def build_alerts(alerts: dict[str, dict], infos: dict[str, dict], now: float, generated_at: str | None = None) -> dict:
    """alerts: alert_id -> gbfs.alerts value (duplicates already collapsed by key). Pass the live doc's
    `generated_at` so live.json and alerts.json never disagree."""
    by_episode: dict[str, dict] = {}
    for a in sorted(alerts.values(), key=lambda a: a["ts"]):
        ep = by_episode.setdefault(a["episode_id"], {"episode_id": a["episode_id"], "station_id": a["station_id"]})
        ep["kind"] = a["kind"]
        ep["started"] = iso(a["start_ts"])
        if a["action"] == "raised":
            ep["raised"] = iso(a["ts"])
        else:
            ep["resolved"] = iso(a["ts"])
            ep["duration_s"] = a["duration_s"]

    rows = []
    for ep in by_episode.values():
        info = infos.get(ep["station_id"], {})
        ep |= {
            "name": info.get("name") or "Unknown station",
            "code": info.get("short_name"),
            "view": view_for(info.get("region_id"), info.get("lat"), info.get("lon")),
        }
        rows.append(ep)
    open_ = sorted((r for r in rows if "resolved" not in r), key=lambda r: r["started"])  # longest-running first
    resolved = sorted((r for r in rows if "resolved" in r), key=lambda r: r["resolved"], reverse=True)
    return {"generated_at": generated_at or iso(now), "open": open_, "resolved": resolved[:RESOLVED_KEPT]}
