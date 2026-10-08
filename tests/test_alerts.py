from dockwatch.alerts.handler import format_alert, lambda_handler

ALERT = {
    "station_id": "s1",
    "kind": "empty",
    "action": "raised",
    "start_ts": 1791412324,  # 2026-10-07 15:32:04 Pacific
    "duration_s": 900,
}


def test_raised_alert_text_is_plain_english_in_pacific_time():
    subject, body = format_alert(ALERT, "Market St at 4th St")
    assert subject == "DockWatch: Market St at 4th St has no bikes (15 min)"
    assert "since 3:32 PM Pacific time" in body


def test_resolved_alert_and_unknown_station_name():
    subject, body = format_alert({**ALERT, "kind": "full", "action": "resolved", "duration_s": 2520}, None)
    assert subject == "DockWatch: resolved, s1"
    assert "after 42 minutes" in body


def test_handler_prints_without_a_topic(capsys, monkeypatch):
    monkeypatch.delenv("DOCKWATCH_ALERTS_TOPIC_ARN", raising=False)
    assert lambda_handler({"alerts": [ALERT], "stations": {"s1": "Market St"}}) == {"sent": 1}
    assert "Market St has no bikes" in capsys.readouterr().out
