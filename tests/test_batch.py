import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dockwatch.batch import airflow_defaults, alerts, evolution, guard, host, maintenance, slot
from dockwatch.cdc import runner
from dockwatch.config import get_settings

ROOT = Path(__file__).resolve().parents[1]
MIB = 1024 * 1024


def load_tasks():
    spec = importlib.util.spec_from_file_location("dockwatch_tasks_py", ROOT / "tasks.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeTrino:
    """Answers maintenance.measure_sql with a scripted sequence of measurements; records every statement."""

    def __init__(self, *measures: dict, orphans: list[dict] | None = None) -> None:
        self.measures = list(measures)
        self.orphans = orphans or []
        self.sql: list[str] = []

    def __call__(self, sql: str) -> list[dict]:
        self.sql.append(sql)
        if sql.startswith("SELECT (SELECT count(*)"):
            return [self.measures.pop(0)]
        if "remove_orphan_files" in sql:
            return self.orphans
        return []


def measured(files, rows, snapshots, bytes_=1000, deletes=0, partitions=3) -> dict:
    return {
        "files": files,
        "bytes": bytes_,
        "rows": rows,
        "snapshots": snapshots,
        "delete_files": deletes,
        "partitions": partitions,
    }


def clock():
    t = [datetime(2026, 10, 10, 6, 0, tzinfo=UTC)]

    def now():
        t[0] += timedelta(seconds=10)
        return t[0]

    return now


# ---------------------------------------------------------------- maintenance


def test_maintenance_covers_the_four_streaming_and_five_ops_tables():
    assert maintenance.TABLES == (
        "bronze.station_status",
        "silver.station_status",
        "silver.availability_5m",
        "silver.station_episodes",
        "ops.stations",
        "ops.docks",
        "ops.vans",
        "ops.rebalancing_jobs",
        "ops.maintenance_tickets",
    )
    assert [n for n, _ in maintenance.METRICS_COLUMNS][:6] == [
        "run_id",
        "table_name",
        "started_at",
        "finished_at",
        "status",
        "error",
    ]


def test_maintenance_statements_use_the_retention_parameters():
    sql = maintenance.statements(
        "silver.station_status", {"file_size_threshold": "64MB", "expire_older_than": "2d", "orphan_older_than": "36h"}
    )
    assert sql == [
        "ALTER TABLE lake.silver.station_status EXECUTE optimize(file_size_threshold => '64MB')",
        "ALTER TABLE lake.silver.station_status EXECUTE expire_snapshots(retention_threshold => '2d')",
        "ALTER TABLE lake.silver.station_status EXECUTE remove_orphan_files(retention_threshold => '36h')",
    ]
    defaults = maintenance.statements("ops.vans")
    assert "file_size_threshold => '100MB'" in defaults[0]
    assert "retention_threshold => '1d'" in defaults[1]
    assert "retention_threshold => '1d'" in defaults[2]
    with pytest.raises(maintenance.MaintenanceError):
        maintenance.statements("ops.vans; DROP TABLE x")


def test_retention_below_one_day_is_refused():
    for bad in ({"expire_older_than": "12h"}, {"orphan_older_than": "0d"}, {"expire_older_than": "1439m"}):
        with pytest.raises(maintenance.MaintenanceError, match="1d retention floor"):
            maintenance.statements("bronze.station_status", bad)
    with pytest.raises(maintenance.MaintenanceError):
        maintenance.params_of({"expire_older_than": "soon"})
    assert maintenance.params_of({"expire_older_than": "24h"})["expire_older_than"] == "24h"
    # the Trino catalog sets the same floor
    props = (ROOT / "infra/trino/etc/catalog/lake.properties").read_text(encoding="utf-8")
    assert "iceberg.expire-snapshots.min-retention=1d" in props
    assert "iceberg.remove-orphan-files.min-retention=1d" in props


def test_maintenance_row_records_before_and_after():
    trino = FakeTrino(
        measured(240, 5000, 300, 9_000_000, 2, 3),
        measured(3, 5000, 2, 7_000_000, 0, 3),
        orphans=[
            {"metric_name": "scanned_files_count", "metric_value": 40},
            {"metric_name": "deleted_files_count", "metric_value": 7},
        ],
    )
    r = maintenance.maintain(trino, "run-1", "bronze.station_status", now=clock())
    assert [s.split(" EXECUTE ")[-1].split("(")[0] for s in trino.sql if "EXECUTE" in s] == [
        "optimize",
        "expire_snapshots",
        "remove_orphan_files",
    ]
    assert r["status"] == "ok"
    assert r["error"] is None
    assert (r["files_before"], r["files_after"]) == (240, 3)
    assert (r["rows_before"], r["rows_after"]) == (5000, 5000)
    assert (r["snapshots_before"], r["snapshots_after"]) == (300, 2)
    assert (r["bytes_before"], r["bytes_after"]) == (9_000_000, 7_000_000)
    assert (r["delete_files_before"], r["delete_files_after"]) == (2, 0)
    assert r["partitions_after"] == 3
    assert r["orphan_files_removed"] == 7
    assert r["finished_at"] > r["started_at"]
    assert set(r) == {n for n, _ in maintenance.METRICS_COLUMNS}
    sql = maintenance.insert_sql([r])
    assert sql.startswith("INSERT INTO lake.metrics.maintenance_runs (run_id, table_name,")
    assert "'bronze.station_status'" in sql
    assert "TIMESTAMP '2026-10-10 06:00:10.000000 UTC'" in sql


def test_maintenance_refuses_a_changed_row_count():
    trino = FakeTrino(measured(10, 5000, 4), measured(2, 4990, 2))
    with pytest.raises(maintenance.MaintenanceError, match="row count changed 5000 -> 4990") as e:
        maintenance.maintain(trino, "run-2", "silver.station_status", now=clock())
    assert e.value.row["status"] == "error"
    assert (e.value.row["rows_before"], e.value.row["rows_after"]) == (5000, 4990)


# ---------------------------------------------------------------- guards


def test_trino_refuses_beside_spark_or_connect():
    for task in ("batch-up", "iceberg_maintenance", "partition_evolution_demo", "scheduler-up"):
        for busy in ("status-stream", "cdc-apply", "connect"):
            reason = guard.refusal(task, lambda s, b=busy: s == b)
            assert busy in (reason or ""), (task, busy)
        assert guard.refusal(task, lambda s: s in {"redpanda", "s3", "ops-db", "iceberg-rest"}) is None
    up = {"status-stream", "connect"}
    assert "status-stream and connect" in guard.refusal("batch-up", lambda s: s in up)


def test_spark_and_connect_refuse_beside_trino(monkeypatch, capsys):
    for task in ("stream", "catchup", "verify-lake", "connect-up", "cdc-catchup", "cdc-reset", "cdc-verify"):
        reason = guard.refusal(task, lambda s: s == "trino")
        assert "Trino" in (reason or ""), task
        assert "batch-stop" in reason, task
        assert guard.refusal(task, lambda s: False) is None
    # the keepalive starts Connect: never beside Trino or Spark
    assert "Trino" in guard.refusal("cdc_slot_keepalive", lambda s: s == "trino")
    # the CDC runner's guard consults the batch guard
    monkeypatch.setattr(runner, "running", lambda s: s == "trino")
    with pytest.raises(runner.CdcError, match="Trino"):
        runner.guard("connect-up")
    # tasks.py catchup refuses before any compose call
    tasks = load_tasks()
    calls = []
    monkeypatch.setattr(tasks.batch_guard, "running_services", lambda: {"trino", "redpanda"})
    monkeypatch.setattr(tasks, "sh", lambda *cmd: calls.append(cmd))
    with pytest.raises(SystemExit) as e:
        tasks.catchup()
    assert e.value.code == 1
    assert calls == []
    assert "Trino" in capsys.readouterr().out


def test_compose_refuses_a_different_path_casing():
    tasks = load_tasks()
    reason = tasks.compose_cwd_refusal(
        r"C:\Users\brady\coding\DockWatch", [r"C:\Users\brady\coding\dockwatch", r"C:\Users\brady\coding\dockwatch"]
    )
    assert reason is not None
    assert r"C:\Users\brady\coding\dockwatch" in reason
    # the CURRENT_STATE recovery text
    assert "--no-deps ops-db" in reason
    assert "restart iceberg-rest" in reason
    mixed = tasks.compose_cwd_refusal(r"C:\a\dockwatch", [r"C:\a\dockwatch", r"C:\a\DockWatch"])
    assert mixed is not None


def test_compose_allows_the_creating_path_or_no_containers():
    tasks = load_tasks()
    assert tasks.compose_cwd_refusal(r"C:\Users\brady\coding\dockwatch", [r"C:\Users\brady\coding\dockwatch"]) is None
    assert tasks.compose_cwd_refusal("/home/runner/work/dockwatch", []) is None
    assert tasks.compose_cwd_refusal("/x", ["", ""]) is None


# ---------------------------------------------------------------- slot keepalive


def status(
    retained_mib: float, wal_status: str = "reserved", safe_mib: float = 900, confirmed: str = "0/5000000"
) -> slot.SlotStatus:
    return slot.SlotStatus(wal_status, int(retained_mib * MIB), int(safe_mib * MIB), confirmed, "0/9000000")


NOW = datetime(2026, 10, 10, 6, 0, tzinfo=UTC)


def test_slot_catch_up_when_retained_wal_passes_the_threshold():
    recent = NOW - timedelta(hours=1)
    assert slot.decide(status(300), recent, NOW).action == "catch_up"
    assert slot.decide(status(255), recent, NOW).action == "skip"
    d = slot.decide(status(78), recent, NOW, catch_up_above_mb=16)
    assert d.action == "catch_up"
    assert "78.0 MiB" in d.reason
    assert slot.decide(status(1), NOW - timedelta(hours=25), NOW).action == "catch_up"
    assert slot.decide(status(1), None, NOW).action == "catch_up"
    assert slot.params_of({"catch_up_above_mb": "16"})["catch_up_above_mb"] == 16


def test_slot_lost_fails_and_never_resets():
    d = slot.decide(status(1100, "lost", 0), NOW, NOW)
    assert d.action == "fail"
    assert "python tasks.py cdc-reset" in d.reason
    assert "never resets" in d.reason
    src = (ROOT / "src/dockwatch/batch/slot.py").read_text(encoding="utf-8")
    src += (ROOT / "src/dockwatch/batch/dag_tasks.py").read_text(encoding="utf-8")
    for reset_call in ("pg_drop_replication_slot", "runner.reset", "delete_topics", 'cdc-reset"'):
        assert reset_call not in src
    assert not slot.caught_up("0/1000", status(1, "lost"))


def test_slot_healthy_needs_no_catch_up():
    d = slot.decide(status(3), NOW - timedelta(hours=2), NOW)
    assert d.action == "skip"
    assert slot.caught_up("0/5000000", status(3))
    assert not slot.caught_up("0/6000000", status(3))  # confirmed_flush_lsn behind the start LSN
    assert not slot.caught_up("0/5000000", status(40))


def test_slot_health_line_and_last_catchup(tmp_path):
    path = tmp_path / "slot_health.jsonl"
    slot.append_health(slot.health_line("r1", "skipped", "fine", status(1), None, None, now=NOW), path)
    slot.append_health(slot.health_line("r2", "caught_up", "big", status(80), status(2), 41.5, now=NOW), path)
    lines = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]
    assert list(lines[1]) == [
        "at",
        "dag_run_id",
        "action",
        "reason",
        "wal_status_before",
        "retained_before_bytes",
        "safe_wal_size_before",
        "wal_status_after",
        "retained_after_bytes",
        "safe_wal_size_after",
        "connect_seconds",
    ]
    assert lines[1]["retained_before_bytes"] == 80 * MIB
    assert lines[1]["retained_after_bytes"] == 2 * MIB
    assert slot.last_catchup(path) == NOW


class FakeBox:
    name = "dockwatch-connect-1"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def start(self):
        self.calls.append("start")

    def stop(self, timeout=None):
        self.calls.append("stop")


class FakeDocker:
    def __init__(self, boxes) -> None:
        self.boxes = boxes
        self.containers = self

    def list(self, all=False, filters=None):  # noqa: A002 - docker SDK signature
        assert all
        assert "com.docker.compose.service=connect" in filters["label"]
        return self.boxes


def test_keepalive_starts_and_always_stops_the_existing_connect(monkeypatch):
    box = FakeBox()
    seq = iter([status(80), status(40), status(2), status(2, confirmed="0/9000000")])
    monkeypatch.setattr(slot, "connector_running", lambda url: True)
    after, seconds = slot.catch_up(FakeDocker([box]), lambda: next(seq), "http://connect:8083", sleep=lambda s: None)
    assert after.retained_bytes == 2 * MIB
    assert box.calls == ["start", "stop"]
    with pytest.raises(slot.KeepaliveError, match="connect-up once"):
        slot.catch_up(FakeDocker([]), lambda: status(80), "http://connect:8083")
    box2, t = FakeBox(), iter(range(0, 10_000, 100))
    with pytest.raises(slot.KeepaliveError, match="still retains"):
        slot.catch_up(
            FakeDocker([box2]), lambda: status(80), "u", timeout_s=300, sleep=lambda s: None, clock=lambda: next(t)
        )
    assert box2.calls == ["start", "stop"]


def test_keepalive_touches_one_captured_row_after_connect_runs(monkeypatch):
    # The touch is one UPDATE of one row of a captured table, returning its WAL position.
    sql = " ".join(slot.TOUCH_SQL.split())
    assert sql.startswith("UPDATE vans SET updated_at = now() WHERE van_id = (SELECT min(van_id) FROM vans)")
    assert "RETURNING pg_current_wal_lsn()" in sql
    assert "vans" in json.loads((ROOT / "infra/connect/ops.json").read_text(encoding="utf-8"))["config"][
        "table.include.list"
    ].replace("public.", "").split(",")

    events: list[str] = []
    box = FakeBox()
    box.start = lambda: events.append("start")
    box.stop = lambda timeout=None: events.append("stop")
    running = iter([False, False, True])

    def connector_running(url):
        events.append("status")
        return next(running)

    def touch():
        events.append("touch")
        return "0/9500000"

    monkeypatch.setattr(slot, "connector_running", connector_running)
    seq = iter(
        [
            status(80),  # start: current LSN 0/9000000
            status(2, confirmed="0/9000000"),  # past the start, not yet past the touch
            status(2, confirmed="0/9500010"),  # confirmed the touch
        ]
    )
    after, _ = slot.catch_up(FakeDocker([box]), lambda: next(seq), "u", sleep=lambda s: None, touch=touch)
    assert events == ["start", "status", "status", "status", "touch", "stop"]  # touch only once, after RUNNING
    assert after.confirmed_flush_lsn == "0/9500010"
    assert slot.target_lsn("0/9000000", "0/9500000") == "0/9500000"
    assert slot.target_lsn("0/A000000", "0/9500000") == "0/A000000"
    assert slot.target_lsn("0/9000000", None) == "0/9000000"

    # Confirmed past the touch but restart_lsn still pins the backlog: touch again (not before RETOUCH_AFTER_S).
    touches = iter(["0/9500000", "0/9600000"])
    seq = iter(
        [
            status(80),
            status(80, confirmed="0/9500010"),  # 10 s after the touch: confirmed, retained high, too early to retouch
            status(80, confirmed="0/9500010"),  # 30 s: touch again
            status(80, confirmed="0/9500010"),  # not yet past the second touch
            status(1, confirmed="0/9600010"),
        ]
    )
    # clock reads: t0, first touch, then per poll the deadline check and (once confirmed) the retouch check
    t = iter([0, 0, 5, 10, 25, 30, 30, 35, 40])
    monkeypatch.setattr(slot, "connector_running", lambda url: True)
    box3 = FakeBox()
    after, _ = slot.catch_up(
        FakeDocker([box3]),
        lambda: next(seq),
        "u",
        sleep=lambda s: None,
        clock=lambda: next(t),
        touch=lambda: next(touches),
    )
    assert after.retained_bytes == 1 * MIB
    assert next(touches, None) is None  # exactly two touches
    assert box3.calls == ["start", "stop"]

    class EmptyVans:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, q):
            assert q == slot.TOUCH_SQL
            return self

        def fetchone(self):
            return None

    import psycopg

    monkeypatch.setattr(psycopg, "connect", lambda *a, **k: EmptyVans())
    with pytest.raises(slot.KeepaliveError, match="vans is empty"):
        slot.touch_captured_row("postgresql://x")


def test_keepalive_timeout_fails_without_retry_and_records_failed(tmp_path, monkeypatch):
    import sys
    import types

    from dockwatch.batch import dag_tasks

    class AirflowFailException(Exception):
        pass

    fake = types.ModuleType("airflow.exceptions")
    fake.AirflowFailException = AirflowFailException
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.exceptions", fake)
    monkeypatch.setenv("DOCKWATCH_DATA_DIR", str(tmp_path))
    box = FakeBox()
    monkeypatch.setattr(dag_tasks, "running_services", lambda: {"redpanda", "ops-db"})
    monkeypatch.setattr(dag_tasks, "docker_client", lambda: FakeDocker([box]))
    monkeypatch.setattr(dag_tasks, "ops_dsn", lambda: "postgresql://x")
    monkeypatch.setattr(dag_tasks, "connect_url", lambda: "u")
    monkeypatch.setattr(slot, "connector_running", lambda url: True)
    monkeypatch.setattr(slot, "touch_captured_row", lambda dsn: "0/9500000")
    monkeypatch.setattr(slot, "inspect_slot", lambda dsn: status(80, confirmed="0/5000000"))  # never moves
    t = iter(range(0, 10_000, 100))
    real = slot.catch_up
    monkeypatch.setattr(slot, "catch_up", lambda *a, **k: real(*a, sleep=lambda s: None, clock=lambda: next(t), **k))

    class TI:
        dag_id, task_id = "cdc_slot_keepalive", "keepalive"

        def xcom_pull(self, task_ids):
            return slot.to_dict(status(80, confirmed="0/5000000"))

    class Run:
        run_id = "manual__2026-10-10T07:00:00+00:00"
        conf = {"catch_up_above_mb": 16, "timeout_s": 300}

    get_settings.cache_clear()
    try:
        with pytest.raises(AirflowFailException, match="confirmed_flush_lsn stuck at 0/5000000, target 0/9500000"):
            dag_tasks.keepalive(ti=TI(), dag_run=Run(), params={})
    finally:
        get_settings.cache_clear()
    assert box.calls == ["start", "stop"]  # Connect stopped even on timeout
    lines = [json.loads(x) for x in (tmp_path / "metrics" / "slot_health.jsonl").read_text("utf-8").splitlines()]
    assert len(lines) == 1
    assert lines[0]["action"] == "failed"
    assert lines[0]["dag_run_id"] == Run.run_id
    assert "confirmed_flush_lsn stuck at 0/5000000" in lines[0]["reason"]
    assert "target 0/9500000" in lines[0]["reason"]
    assert lines[0]["touch_lsn"] == "0/9500000"
    assert lines[0]["retained_after_bytes"] == 80 * MIB


# ---------------------------------------------------------------- alerts / retries


class FakeTI:
    dag_id = "iceberg_maintenance"
    task_id = "maintain"
    run_id = "manual__2026-10-10T06:00:00+00:00"
    try_number = 2
    max_tries = 3
    log_url = "http://localhost:18090/log?x=1"


def test_failure_alert_is_one_json_line_with_dag_task_and_try(tmp_path, monkeypatch):
    monkeypatch.setenv("DOCKWATCH_DATA_DIR", str(tmp_path))
    sent = []
    monkeypatch.setattr(alerts, "sns_publisher", lambda *a: sent.append(a))
    get_settings.cache_clear()
    monkeypatch.setenv("DOCKWATCH_ALERTS_TOPIC_ARN", "")
    try:
        alerts.task_failed({"ti": FakeTI(), "exception": ValueError("boom")})
    finally:
        get_settings.cache_clear()
    lines = (tmp_path / "airflow" / "alerts.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert list(rec) == ["at", "kind", "dag_id", "task_id", "run_id", "try_number", "max_tries", "exception", "log_url"]
    assert rec["kind"] == "task_failed"
    assert (rec["dag_id"], rec["task_id"], rec["try_number"], rec["max_tries"]) == (
        "iceberg_maintenance",
        "maintain",
        2,
        3,
    )
    assert rec["exception"] == "ValueError: boom"
    assert rec["run_id"] == FakeTI.run_id
    assert sent == []


def test_alert_publishes_to_sns_when_a_topic_is_set(tmp_path):
    sent = []
    arn = "arn:aws:sns:us-west-2:123456789012:dockwatch-alerts"
    alerts.alert(
        "slot_unhealthy",
        dag_id="cdc_slot_keepalive",
        task_id="keepalive",
        exception="lost",
        path=tmp_path / "a.jsonl",
        topic_arn=arn,
        publisher=lambda *a: sent.append(a),
    )
    assert len(sent) == 1
    topic, subject, message = sent[0]
    assert topic == arn
    assert "slot_unhealthy" in subject
    assert json.loads(message)["exception"] == "lost"
    with pytest.raises(ValueError, match="unknown alert kind"):
        alerts.alert("oops", path=tmp_path / "a.jsonl", topic_arn="")


def test_default_args_retry_exponentially_and_alert():
    args = airflow_defaults.default_args()
    assert args["retries"] == 3
    assert args["retry_exponential_backoff"] is True
    assert args["retry_delay"] == timedelta(minutes=1)
    assert args["max_retry_delay"] == timedelta(minutes=30)
    assert args["on_failure_callback"] is alerts.task_failed
    assert args["execution_timeout"]
    kw = airflow_defaults.dag_kwargs()
    assert kw == {"catchup": False, "max_active_runs": 1, "sla_miss_callback": alerts.sla_missed}


# ---------------------------------------------------------------- partition evolution


def test_evolution_summary_groups_files_by_spec():
    files = [
        {"spec_id": 0, "partition": '{"snapshot_ts_day":"2026-10-08"}', "record_count": 100, "file_size_in_bytes": 10},
        {"spec_id": 0, "partition": '{"snapshot_ts_day":"2026-10-08"}', "record_count": 50, "file_size_in_bytes": 5},
        {"spec_id": 1, "partition": '{"snapshot_ts_hour":491000}', "record_count": 7, "file_size_in_bytes": 3},
        {"spec_id": 1, "partition": '{"snapshot_ts_hour":491001}', "record_count": 8, "file_size_in_bytes": 4},
        {"spec_id": 1, "partition": '{"snapshot_ts_hour":491001}', "record_count": 9, "file_size_in_bytes": 4},
    ]
    s = evolution.summarize(files)
    assert s == {
        "specs": {
            "0": {"partitions": 1, "files": 2, "rows": 150, "bytes": 15},
            "1": {"partitions": 2, "files": 3, "rows": 24, "bytes": 11},
        },
        "files": 5,
        "rows": 174,
    }
    assert evolution.summarize([]) == {"specs": {}, "files": 0, "rows": 0}
    rep = evolution.report(["2026-10-08", "2026-10-09"], {"optimized": s, "hourly_load": s, "daily_load": s}, NOW)
    assert list(rep) == ["table", "source", "days", "generated_at", "stages"]
    assert list(rep["stages"]) == ["daily_load", "hourly_load", "optimized"]
    assert rep["table"] == "lab.status_partition_evolution"
    assert rep["source"] == "silver.station_status"


def test_evolution_days_and_sql():
    stats = [
        {"day": "2026-10-07", "hours": 9, "n": 900},
        {"day": "2026-10-08", "hours": 24, "n": 5000},
        {"day": "2026-10-09", "hours": 22, "n": 4000},
        {"day": "2026-10-10", "hours": 6, "n": 600},
    ]
    assert evolution.pick_days(stats) == ["2026-10-08", "2026-10-09"]
    few = [
        {"day": "2026-10-07", "hours": 9, "n": 900},
        {"day": "2026-10-10", "hours": 6, "n": 600},
        {"day": "2026-10-09", "hours": 3, "n": 100},
    ]
    assert evolution.pick_days(few) == ["2026-10-07", "2026-10-10"]
    with pytest.raises(ValueError, match="two different"):
        evolution.check_days(["2026-10-08", "2026-10-08"])
    inserts = evolution.hour_inserts("2026-10-08")
    assert len(inserts) == 24
    assert "TIMESTAMP '2026-10-08 23:00:00 UTC' AND snapshot_ts < TIMESTAMP '2026-10-09 00:00:00 UTC'" in inserts[-1]
    assert "ARRAY['day(snapshot_ts)']" in evolution.create_sql()[-1]
    assert "ARRAY['hour(snapshot_ts)']" in evolution.evolve_sql()


# ---------------------------------------------------------------- host CLI


def test_dag_test_conf_parses_json_values():
    assert host.parse_conf(["catch_up_above_mb=16", 'days=["2026-10-08","2026-10-09"]', "x=abc"]) == {
        "catch_up_above_mb": 16,
        "days": ["2026-10-08", "2026-10-09"],
        "x": "abc",
    }
    with pytest.raises(host.BatchError):
        host.parse_conf(["novalue"])
