import json
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from dockwatch.cdc import __main__ as cdc_cli
from dockwatch.cdc import events as ev
from dockwatch.cdc import runner
from dockwatch.cdc.checksum import canonical, data_columns, project, row_hash, table_checksum
from dockwatch.cdc.guard import cdc_topics, refusal, slot_refusal, slot_warning

# Records captured from the real Debezium connector (ops.public.*, JsonConverter with schemas), trimmed to a few keys.
FIXTURES = Path(__file__).parent / "fixtures" / "cdc"


def records(table: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURES / f"{table}.jsonl").read_text(encoding="utf-8").splitlines()]


def parsed(table: str) -> list[ev.Event]:
    return [ev.parse_event(r["topic"], r["offset"], r["key"], r["value"]) for r in records(table)]


def by_key(events: list[ev.Event]) -> dict[tuple, list[ev.Event]]:
    out: dict[tuple, list[ev.Event]] = {}
    for e in events:
        out.setdefault(e.key, []).append(e)
    return out


def test_connect_schema_maps_to_iceberg_types():
    station = parsed("stations")[0]
    assert station.table == "stations"
    assert station.key_names == ("station_id",)
    assert [(c.name, c.type) for c in station.columns] == [
        ("station_id", "string"),
        ("name", "string"),
        ("short_name", "string"),
        ("lat", "double"),
        ("lon", "double"),
        ("capacity", "int"),
        ("region_id", "string"),
        ("is_active", "boolean"),
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
    ]
    assert isinstance(station.row["lat"], float)
    assert isinstance(station.row["is_active"], bool)
    assert station.row["created_at"].tzinfo is not None
    jobs = [e for e in parsed("rebalancing_jobs") if "priority" in e.row]
    types = {c.name: c.type for c in jobs[0].columns}
    assert types["job_id"] == "bigint"
    assert types["bikes_moved"] == "int"
    assert types["priority"] == "int"  # Postgres smallint arrives as Connect int16
    assert types["opened_at"] == "timestamp"
    # ZonedTimestamp strings trim trailing zeros of the fraction; MicroTimestamp is microseconds since the epoch.
    zoned = ev.Column("t", "timestamp", "io.debezium.time.ZonedTimestamp")
    micro = ev.Column("t", "timestamp", "io.debezium.time.MicroTimestamp")
    assert ev.decode("2026-10-01T00:20:00.12Z", zoned) == datetime(2026, 10, 1, 0, 20, 0, 120000, tzinfo=UTC)
    assert ev.decode("2026-10-01T00:20:00Z", zoned) == datetime(2026, 10, 1, 0, 20, tzinfo=UTC)
    assert ev.decode("2026-10-01T02:20:00+02:00", zoned) == datetime(2026, 10, 1, 0, 20, tzinfo=UTC)
    assert ev.decode(1_790_814_000_000_001, micro) == datetime(2026, 10, 1, 0, 20, 0, 1, tzinfo=UTC)
    with pytest.raises(ev.SchemaError, match="not supported"):
        ev.iceberg_type({"type": "string", "name": "io.debezium.data.VariableScaleDecimal", "field": "x"})
    with pytest.raises(ev.SchemaError, match="not supported"):
        ev.iceberg_type({"type": "bytes", "field": "x"})


def test_new_column_plans_an_add_column():
    jobs = parsed("rebalancing_jobs")
    before = next(e for e in jobs if "priority" not in e.row)
    after = next(e for e in jobs if "priority" in e.row)
    existing = [(c.name, c.type) for c in before.columns] + list(ev.METADATA)
    assert ev.plan_evolution(existing, after.columns) == [ev.Column("priority", "int")]
    assert ev.plan_evolution(existing, before.columns) == []
    # the new column comes last whatever order a batch's events arrive in
    assert [c.name for c in ev.merged_columns([after, before])][-1] == "priority"
    sql = ev.add_column_sql("lake.ops.rebalancing_jobs", ev.Column("priority", "int"))
    assert sql == "ALTER TABLE lake.ops.rebalancing_jobs ADD COLUMN `priority` int"


def test_type_change_is_refused():
    after = next(e for e in parsed("rebalancing_jobs") if "priority" in e.row)
    existing = [(c.name, "string" if c.name == "priority" else c.type) for c in after.columns]
    with pytest.raises(ev.SchemaError, match="priority"):
        ev.plan_evolution(existing, after.columns)
    changed = replace(
        after, columns=[ev.Column(c.name, "bigint") if c.name == "priority" else c for c in after.columns]
    )
    with pytest.raises(ev.SchemaError, match="type changed"):
        ev.merged_columns([after, replace(changed, lsn=after.lsn + 1)])


def test_latest_event_per_key_wins_by_lsn():
    jobs = parsed("rebalancing_jobs")
    keys = by_key(jobs)
    latest = {e.key: e for e in ev.latest_per_key(jobs)}
    assert sorted(latest) == sorted(keys)
    for key, history in keys.items():
        assert latest[key] is max(history, key=lambda e: e.lsn)
    assert ev.latest_per_key(list(reversed(jobs))) == ev.latest_per_key(jobs)
    # a replayed (higher-offset) copy of an older event never beats a newer LSN
    key, history = next((k, h) for k, h in keys.items() if len(h) >= 2)
    stale = replace(history[0], offset=10_000)
    assert ev.latest_per_key([*history, stale])[0] is history[-1]
    # same LSN: the later Kafka offset wins
    twin = replace(history[-1], offset=history[-1].offset + 1)
    assert ev.latest_per_key([history[-1], twin])[0] is twin
    sql = ev.merge_sql("lake.ops.rebalancing_jobs", "src", ["job_id"], ["job_id", "status"])
    assert "WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN DELETE" in sql
    assert "WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE" in sql
    assert "WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT" in sql


def test_delete_event_uses_the_before_image_key():
    raw = next(r for r in records("rebalancing_jobs") if json.loads(r["value"])["payload"]["op"] == "d")
    payload = json.loads(raw["value"])["payload"]
    assert payload["after"] is None
    assert payload["before"] is not None
    e = ev.parse_event(raw["topic"], raw["offset"], raw["key"], raw["value"])
    assert e.op == "d"
    assert e.key == (json.loads(raw["key"])["payload"]["job_id"],) == (payload["before"]["job_id"],)
    # REPLICA IDENTITY FULL: the before image is the whole old row, not just the key
    assert e.row["status"] == "cancelled"
    assert e.row["station_id"] == payload["before"]["station_id"]
    assert e.row["opened_at"] is not None
    assert e.row["closed_at"] is not None
    assert e.lsn == payload["source"]["lsn"]


def test_checksum_ignores_row_order():
    rows = [(1, "sim-0001", 37.7712, True), (2, "sim-0002", 37.7801, False), (3, "sim-0003", None, True)]
    assert table_checksum(rows) == table_checksum(list(reversed(rows)))
    assert table_checksum(rows) != table_checksum(rows[:2])
    assert table_checksum(rows) != table_checksum([(1, "sim-0001", 37.7712, True), *rows[1:2], (3, "x", None, True)])
    # Iceberg rows carry CDC metadata columns; only Postgres's columns, in Postgres's order, are hashed.
    iceberg = [{"_lsn": 99, "id": r[0], "station_id": r[1], "lat": r[2], "ok": r[3], "_op": "u"} for r in rows]
    columns = data_columns(["id", "station_id", "lat", "ok", "_lsn", "_op"])
    assert columns == ["id", "station_id", "lat", "ok"]
    assert table_checksum(project(reversed(iceberg), columns)) == table_checksum(rows)


def test_checksum_canonicalises_timestamps_and_floats():
    aware = datetime(2026, 10, 1, 0, 20, 0, 123456, tzinfo=UTC)
    pacific = aware.astimezone(timezone(timedelta(hours=-7)))
    naive_utc = datetime(2026, 10, 1, 0, 20, 0, 123456)  # how Spark collects a timestamp with TZ=UTC
    assert canonical(aware) == canonical(pacific) == canonical(naive_utc) == "2026-10-01T00:20:00.123456+00:00"
    assert canonical(datetime(2026, 10, 1, tzinfo=UTC)) == "2026-10-01T00:00:00.000000+00:00"
    assert canonical(0.1 + 0.2) == "0.30000000000000004"  # repr: exact round trip, no rounding
    assert canonical(37.7712) == "37.7712"
    assert canonical(Decimal("1.50")) == canonical(Decimal("1.5")) == "1.5"
    assert [canonical(v) for v in (None, True, False, 3, "open")] == ["\\N", "t", "f", "3", "open"]
    assert row_hash((1, aware, 0.5)) == row_hash((1, naive_utc, 0.5))
    assert row_hash((1, None)) != row_hash((1, "\\N "))
    assert row_hash((1, 2)) != row_hash((2, 1))


def test_spark_and_connect_never_run_together():
    def probe(*up):
        return lambda service: service in up

    for task in ("cdc-catchup", "cdc-reset", "cdc-verify", "catchup"):
        assert refusal(task, probe()) is None
        assert refusal(task, probe("cdc-apply", "redpanda")) is None
        assert "connect" in refusal(task, probe("connect"))
        assert "status-stream" in refusal(task, probe("status-stream"))
    assert refusal("connect-up", probe("redpanda", "ops-db")) is None
    assert "cdc-apply" in refusal("connect-up", probe("cdc-apply"))
    assert "status-stream" in refusal("connect-up", probe("status-stream"))
    assert refusal("ops-schema", probe("connect", "cdc-apply")) is None  # Postgres-only tasks are never blocked


def test_reset_only_deletes_cdc_topics():
    topics = [
        "gbfs.station_status",
        "gbfs.station_information",
        "gbfs.alerts",
        "gbfs.dlq",
        "gbfs.station_status.replay",
        "dockwatch.station_state",
        "ops.public.rebalancing_jobs",
        "ops.public.stations",
        "dockwatch-connect-offsets",
        "dockwatch-connect-configs",
        "dockwatch-connect-status",
        "__debezium-heartbeat.ops",
        "_schemas",
        "ops",
    ]
    assert cdc_topics(topics) == [
        "dockwatch-connect-configs",
        "dockwatch-connect-offsets",
        "dockwatch-connect-status",
        "ops.public.rebalancing_jobs",
        "ops.public.stations",
    ]


OPS_TOPICS = [f"ops.public.{t}" for t in runner.TABLES]
OTHER_TOPICS = [
    "gbfs.station_status",
    "gbfs.alerts",
    "dockwatch-connect-offsets",
    "dockwatch-connect-configs",
    "dockwatch-connect-status",
    "__debezium-heartbeat.ops",
]


def test_lost_slot_refuses_connect_up_and_points_to_cdc_reset():
    msg = slot_refusal("dockwatch_ops", "lost")
    assert msg is not None
    assert "lost" in msg
    assert "python tasks.py cdc-reset" in msg
    assert "max_slot_wal_keep_size" in msg
    assert "dockwatch_ops" in msg
    for dropped in ("lake.ops.*", "slot", "ops tables", "ops.public.*"):
        assert dropped in msg


def test_missing_or_healthy_slot_allows_connect_up():
    for status in (None, "reserved", "extended"):
        assert slot_refusal("dockwatch_ops", status) is None
        assert slot_warning("dockwatch_ops", status) is None


def test_unreserved_slot_warns_but_allows_connect_up():
    assert slot_refusal("dockwatch_ops", "unreserved") is None
    warning = slot_warning("dockwatch_ops", "unreserved")
    assert warning is not None
    assert "unreserved" in warning
    assert "cdc-reset" in warning
    assert slot_warning("dockwatch_ops", "lost") is None  # lost is a refusal, not a warning


def _no_connect(monkeypatch) -> list:
    """Fakes for every side effect of connect_up; returns the list of calls that would start or alter something."""
    calls: list = []
    monkeypatch.setattr(runner, "guard", lambda task: None)
    monkeypatch.setattr(runner, "slot_status", lambda slot, dsn=None: "lost")
    monkeypatch.setattr(runner, "sh", lambda *cmd, **kw: calls.append(("sh", cmd)))
    monkeypatch.setattr(runner, "admin", lambda: calls.append(("admin",)))
    monkeypatch.setattr(runner, "ensure_cdc_topic_retention", lambda client: calls.append(("retention",)))
    monkeypatch.setattr(runner, "http", lambda *a, **kw: calls.append(("http", a)))
    return calls


def test_connect_up_checks_the_slot_before_starting_connect(monkeypatch):
    calls = _no_connect(monkeypatch)
    with pytest.raises(runner.CdcError, match="cdc-reset"):
        runner.connect_up()
    assert calls == []  # no compose up, no topic alter, no connector registration


def test_cli_connect_up_exits_1_on_a_lost_slot(monkeypatch, capsys):
    calls = _no_connect(monkeypatch)
    with pytest.raises(SystemExit) as exit_info:
        cdc_cli.main(["connect-up"])
    assert exit_info.value.code == 1
    err = capsys.readouterr().err
    assert err.startswith("cdc connect-up: ")
    assert "cdc-reset" in err
    assert calls == []


class FakeFuture:
    def result(self):
        return None


class FakeAdmin:
    def __init__(self, topics):
        self.topics = topics
        self.altered: dict[str, dict] = {}

    def list_topics(self, timeout=None):
        return type("Metadata", (), {"topics": {t: object() for t in self.topics}})()

    def incremental_alter_configs(self, resources):
        for r in resources:
            self.altered[r.name] = {e.name: e.value for e in r.incremental_configs}
        return {r: FakeFuture() for r in resources}


def test_cdc_topic_retention_is_set_only_on_ops_topics():
    client = FakeAdmin(OTHER_TOPICS + OPS_TOPICS)
    assert runner.ensure_cdc_topic_retention(client) == sorted(OPS_TOPICS)
    assert sorted(client.altered) == sorted(OPS_TOPICS)
    for configs in client.altered.values():
        assert configs == {"retention.ms": "-1", "cleanup.policy": "delete"}
    assert runner.ensure_cdc_topic_retention(FakeAdmin(OTHER_TOPICS)) == []


def test_connector_creates_ops_topics_with_unlimited_retention():
    cfg = json.loads(runner.CONNECTOR_FILE.read_text(encoding="utf-8"))["config"]
    assert not any(k.startswith("topic.creation.default.") and ("retention" in k or "cleanup" in k) for k in cfg)
    covering = []
    for group in (g.strip() for g in cfg["topic.creation.groups"].split(",")):
        patterns = cfg[f"topic.creation.{group}.include"].split(",")

        def match(name, patterns=patterns):
            return any(re.fullmatch(p.strip(), name) for p in patterns)

        if all(match(t) for t in OPS_TOPICS):
            covering.append(group)
            for other in OTHER_TOPICS + ["opsXpublic.vans"]:
                assert not match(other), other
            assert cfg[f"topic.creation.{group}.retention.ms"] == "-1"
            assert cfg.get(f"topic.creation.{group}.cleanup.policy", "delete") == "delete"
    assert len(covering) == 1
