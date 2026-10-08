from dockwatch.config import Settings
from dockwatch.streaming.run_mode import trigger_options


def test_processing_time_is_default(monkeypatch):
    monkeypatch.delenv("DOCKWATCH_TRIGGER_MODE", raising=False)
    s = Settings(_env_file=None, trigger_seconds=45)
    assert s.trigger_mode == "processing_time"
    assert trigger_options(s) == {"processingTime": "45 seconds"}


def test_available_now_trigger(monkeypatch):
    monkeypatch.setenv("DOCKWATCH_TRIGGER_MODE", "available_now")
    s = Settings(_env_file=None)
    assert s.trigger_mode == "available_now"
    assert trigger_options(s) == {"availableNow": True}
