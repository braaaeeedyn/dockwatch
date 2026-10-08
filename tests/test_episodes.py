from dockwatch.streaming.episodes import EpisodeState, Thresholds, advance, capacity_of, classify

T = Thresholds(alert_after_s=15 * 60)
S = "station-1"


def _cls(**kw):
    base = dict(snapshot_ts=10_000, last_reported=9_990, is_installed=True, is_renting=True, bikes=8, docks=8)
    base.update(kw)
    base.setdefault("capacity", base["bikes"] + base["docks"])
    return classify(**base)


def test_classify_rules_in_priority_order():
    assert _cls(last_reported=10_000 - 31 * 60) == "offline"  # stale beats everything
    assert _cls(is_renting=False, bikes=0) == "offline"
    assert _cls(bikes=0) == "empty"
    assert _cls(docks=0) == "full"
    assert _cls(bikes=2) == "low"
    assert _cls(bikes=3, docks=30) == "low"  # 3 <= 10% of 33
    assert _cls(docks=1) == "high"
    assert _cls() == "ok"


def test_capacity_counts_disabled_bikes_and_docks():
    assert capacity_of(5, 1, 10, 2) == 18


def _rows(*pairs):
    return [{"snapshot_ts": ts, "state": st} for ts, st in pairs]


def test_episode_opens_updates_and_closes():
    st = EpisodeState()
    ev = advance(S, st, _rows((0, "ok"), (60, "empty"), (120, "empty")), T)
    assert [(e["type"], e["status"]) for e in ev] == [("episode", "open")]
    assert ev[0]["start_ts"] == 60
    assert ev[0]["duration_s"] == 60

    ev = advance(S, st, _rows((180, "low")), T)
    assert ev[0]["status"] == "closed"
    assert ev[0]["end_ts"] == 180
    assert ev[0]["duration_s"] == 120
    assert st.kind is None


def test_alert_raised_once_then_resolved():
    st = EpisodeState()
    rows = _rows(*[(m * 60, "full") for m in range(0, 20)])
    ev = advance(S, st, rows, T)
    alerts = [e for e in ev if e["type"] == "alert"]
    assert len(alerts) == 1
    assert alerts[0]["action"] == "raised"
    assert alerts[0]["duration_s"] == 15 * 60
    assert not [e for e in advance(S, st, _rows((20 * 60, "full")), T) if e["type"] == "alert"]
    ev = advance(S, st, _rows((21 * 60, "ok")), T)
    assert [e.get("action") for e in ev if e["type"] == "alert"] == ["resolved"]


def test_switch_from_empty_to_full_closes_one_and_opens_other():
    st = EpisodeState()
    ev = advance(S, st, _rows((0, "empty"), (60, "full")), T)
    statuses = [(e["kind"], e["status"]) for e in ev]
    assert statuses == [("empty", "closed"), ("full", "open")]


def test_duplicates_and_late_rows_are_ignored():
    st = EpisodeState()
    advance(S, st, _rows((60, "empty"), (120, "empty")), T)
    assert advance(S, st, _rows((120, "ok"), (60, "ok")), T) == []
    assert st.kind == "empty"


def test_out_of_order_rows_within_a_batch_are_sorted():
    st = EpisodeState()
    ev = advance(S, st, _rows((120, "ok"), (60, "empty")), T)
    assert [e["status"] for e in ev] == ["closed"]
    assert ev[0]["start_ts"] == 60


def test_offline_closes_an_episode():
    st = EpisodeState()
    advance(S, st, _rows((0, "empty")), T)
    ev = advance(S, st, _rows((60, "offline")), T)
    assert ev[0]["status"] == "closed"
