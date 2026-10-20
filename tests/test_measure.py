"""Loop O: the measurements behind docs/METRICS.md (dockwatch.measure, Trino faked), the lab mode of the streaming job
(run_mode, tasks.py stream-lab, docker faked), the paced replay (FsArchive, fake publisher and clock) and the
crash-restart parsers and zero-diff rule."""

import copy
import importlib.util
import io
import json
import re
import sys
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import dockwatch.producer.__main__ as producer
from dockwatch import measure
from dockwatch.config import Settings, get_settings
from dockwatch.producer.archive import FsArchive, archive_key
from dockwatch.streaming import run_mode

ROOT = Path(__file__).resolve().parents[1]
T0 = datetime(2026, 10, 20, 1, 0, tzinfo=UTC)
TABLES = ("bronze.station_status", "silver.station_status", "silver.availability_5m", "silver.station_episodes")
LAB = {
    "namespace_prefix": "lab_",
    "starting_offsets": "latest",
    "checkpoint_root": "/checkpoints/lab/20261020T010000Z",
    "topic_station_status": "gbfs.station_status.replay",
    "topic_alerts": "gbfs.alerts.lab",
    "topic_station_state": "dockwatch.station_state.lab",
}


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def trino_text(d: datetime) -> str:
    return f"{d:%Y-%m-%d %H:%M:%S.%f} UTC"


def load_tasks():
    spec = importlib.util.spec_from_file_location("dockwatch_tasks_py_o", ROOT / "tasks.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def fresh_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_percentiles_are_nearest_rank_over_weighted_groups():
    groups = [(3.0, 2), (1.0, 5), (2.0, 3)]  # ten values: 1.0 x5, 2.0 x3, 3.0 x2
    assert measure.nearest_rank(groups, 0.5) == 1.0  # rank 5
    assert measure.nearest_rank(groups, 0.51) == 2.0  # rank ceil(5.1) = 6
    assert measure.nearest_rank(groups, 0.8) == 2.0  # rank 8
    assert measure.nearest_rank(groups, 0.95) == 3.0  # rank ceil(9.5) = 10
    assert measure.nearest_rank(groups, 0.0) == 1.0  # rank 1 at least
    assert measure.nearest_rank([], 0.5) is None
    assert measure.summary(groups) == {"p50": 1.0, "p95": 3.0, "max": 3.0}
    assert measure.summary([(9.0, 0), (4.0, 1)]) == {"p50": 4.0, "p95": 4.0, "max": 4.0}  # a 0-count group is no value


def test_each_micro_batch_pairs_with_the_snapshot_that_holds_its_rows():
    batches = {at(0): 647, at(60): 647, at(120): 1294, at(180): 500}
    snapshots = [
        (at(-30), 647),  # the batch before the window: committed before batch 0 started
        (at(2), None),  # a no-data batch (it only moves the watermark): an append without added-records
        (at(19), 647),  # batch 0
        (at(61), None),
        (at(78), 647),  # batch 1
        (at(139), 1294),  # batch 2
        (at(200), 499),  # not batch 3's rows: batch 3 stays unpaired
    ]
    assert measure.pair(batches, snapshots) == {at(0): at(19), at(60): at(78), at(120): at(139)}
    # a snapshot is used once: two equal batches never share one commit
    assert measure.pair({at(0): 10, at(1): 10}, [(at(5), 10)]) == {at(0): at(5)}


class FakeTrino:
    """Answers dockwatch.measure's statements by their text; records them."""

    def __init__(self, groups, kafka, snapshots, timed=()):
        self.groups, self.kafka, self.snapshots, self.timed = groups, kafka, snapshots, list(timed)
        self.sql: list[str] = []

    def __call__(self, sql: str):
        self.sql.append(sql)
        if "GROUP BY 1, 2" in sql:
            return self.groups, {}
        if "approx_percentile" in sql:
            return [self.kafka], {}
        if "$snapshots" in sql and "added-records" in sql:
            return self.snapshots, {}
        if "$files" in sql:
            return [{"files": 377, "bytes": 123_456_789}], {}
        if "$partitions" in sql:
            return [{"partitions": 13}], {}
        if "$snapshots" in sql:
            return [{"snapshot_id": 4242}], {}
        return [{"station_id": "a", "n": 1, "newest": "x"}], {"elapsedTimeMillis": self.timed.pop(0)}


def test_window_reports_latency_commit_latency_batch_duration_and_events_per_minute(fresh_settings):
    p1, p2 = at(10), at(70)
    groups = [  # processed_ts, fetched_ts, rows (as Trino's varchar)
        {"p": trino_text(p1), "f": trino_text(at(0)), "n": 500},  # 10 s
        {"p": trino_text(p1), "f": trino_text(at(-60)), "n": 150},  # 70 s: a snapshot fetched a batch earlier
        {"p": trino_text(p2), "f": trino_text(at(60)), "n": 650},  # 10 s
    ]
    snapshots = [
        {"c": trino_text(at(11)), "a": None},  # no-data batch
        {"c": trino_text(at(28)), "a": "650"},  # batch 1: 18 s
        {"c": trino_text(at(90)), "a": "650"},  # batch 2: 20 s
    ]
    kafka = {"lo": trino_text(at(-59.8)), "hi": trino_text(at(60.3)), "fk": 0.24}
    q = FakeTrino(groups, kafka, snapshots)
    doc = measure.window("lake.silver.station_status", "2026-10-20T01:00:00Z", "2026-10-20T01:02:00Z", q=q)
    assert (
        "processed_ts >= TIMESTAMP '2026-10-20 01:00:00 UTC' AND processed_ts < TIMESTAMP '2026-10-20 01:02:00 UTC'"
        in q.sql[0]
    )
    assert doc["table"] == "lake.silver.station_status"
    assert (doc["from"], doc["to"]) == ("2026-10-20T01:00:00Z", "2026-10-20T01:02:00Z")
    assert (doc["rows"], doc["micro_batches"], doc["unpaired_batches"], doc["trigger_s"]) == (1300, 2, 0, 60)
    assert doc["latency_s"] == {"p50": 10.0, "p95": 70.0, "max": 70.0}  # 1,150 rows at 10 s, 150 at 70 s
    # fetch -> queryable: 28 s x500, 88 s x150 (batch 1), 30 s x650 (batch 2)
    assert doc["commit_latency_s"] == {"p50": 30.0, "p95": 88.0, "max": 88.0}
    assert doc["batch_s"] == {"p50": 18.0, "p95": 20.0, "max": 20.0}
    assert doc["events_per_min"] == round(1300 * 60 / 120.1, 1)  # rows over the Kafka-time span
    assert doc["fetch_to_kafka_p50_s"] == 0.24
    assert doc["sustained"] is True
    json.dumps(doc)  # the saved / printed record is plain JSON


def test_a_run_is_sustained_only_when_every_micro_batch_finishes_within_the_trigger():
    def report(commits: dict[datetime, datetime]):
        groups = [(p, p - timedelta(seconds=3), 600) for p in commits]
        snaps = [(c, 600) for c in commits.values() if c is not None]
        return measure.window_report(
            groups, snaps, (at(0), at(300), 0.2), "2026-10-20T01:00:00Z", "2026-10-20T01:05:00Z", 60
        )

    assert report({at(0): at(40), at(60): at(119)})["sustained"] is True  # 40 s, 59 s
    late = report({at(0): at(40), at(60): at(121)})  # one 61 s batch: the next one started late
    assert (late["sustained"], late["batch_s"]["max"]) == (False, 61.0)
    lost = report({at(0): at(40), at(60): None})  # a batch with no commit holding its rows
    assert (lost["sustained"], lost["unpaired_batches"]) == (False, 1)
    assert report({})["sustained"] is False  # nothing measured is not sustained


def test_files_measure_times_the_query_and_drops_the_warm_up_run():
    q = FakeTrino([], {}, [], timed=[900, 120, 100, 140, 110, 130])
    doc = measure.files("lake.silver.station_status", runs=5, q=q)
    query = (
        "SELECT station_id, count(*) AS n, max(snapshot_ts) AS newest FROM lake.silver.station_status "
        "GROUP BY station_id"
    )
    assert doc["query"] == query
    assert q.sql.count(query) == 6  # one warm-up + 5 runs
    assert (doc["warmup_ms"], doc["runs_ms"], doc["median_ms"]) == (900, [120, 100, 140, 110, 130], 120)
    assert (doc["files"], doc["bytes"], doc["partitions"], doc["snapshot_id"]) == (377, 123_456_789, 13, 4242)
    assert 'lake.silver."station_status$files"' in " ".join(q.sql)
    assert doc["table"] == "lake.silver.station_status"
    assert datetime.fromisoformat(doc["at"]).tzinfo is not None


class FakeHttp:
    """urllib.request.urlopen for Trino's statement protocol: pages per URL, an optional 503 first."""

    def __init__(self, pages: dict[str, list]):
        self.pages = pages
        self.requests: list = []

    def __call__(self, req, timeout=None):
        self.requests.append(req)
        page = self.pages[req.full_url].pop(0)
        if page == 503:
            raise urllib.error.HTTPError(req.full_url, 503, "busy", {}, None)
        return io.BytesIO(json.dumps(page).encode())


def test_trino_client_follows_next_uri_and_raises_on_a_query_error(monkeypatch, tmp_path):
    base = "http://trino.test:8080"
    http = FakeHttp(
        {
            f"{base}/v1/statement": [
                {"id": "q1", "nextUri": f"{base}/v1/statement/q1/1", "stats": {"state": "QUEUED"}}
            ],
            f"{base}/v1/statement/q1/1": [
                503,
                {"columns": [{"name": "a"}, {"name": "b"}], "data": [[1, "x"]], "nextUri": f"{base}/v1/statement/q1/2"},
            ],
            f"{base}/v1/statement/q1/2": [
                {"data": [[2, "y"]], "stats": {"state": "FINISHED", "elapsedTimeMillis": 42}}
            ],
        }
    )
    monkeypatch.setattr(measure.urllib.request, "urlopen", http)
    monkeypatch.setattr(measure.time, "sleep", lambda s: None)
    rows, stats = measure.trino("SELECT a, b FROM t", url=base)
    assert rows == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    assert stats["elapsedTimeMillis"] == 42
    post = http.requests[0]
    assert (post.get_method(), post.data, post.get_header("X-trino-user")) == (
        "POST",
        b"SELECT a, b FROM t",
        "dockwatch-measure",
    )
    assert len(http.requests) == 4  # POST, a 503 retried, two pages

    http = FakeHttp(
        {
            f"{base}/v1/statement": [{"id": "q2", "nextUri": f"{base}/v1/statement/q2/1"}],
            f"{base}/v1/statement/q2/1": [{"error": {"message": "line 1:15: Table 'lake.x.y' does not exist"}}],
        }
    )
    monkeypatch.setattr(measure.urllib.request, "urlopen", http)
    with pytest.raises(measure.TrinoError, match="does not exist"):
        measure.trino("SELECT * FROM lake.x.y", url=base)

    # the CLI exits 1 on a Trino error and saves nothing
    def refused(req, timeout=None):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(measure.urllib.request, "urlopen", refused)
    monkeypatch.setattr(measure, "METRICS_DIR", tmp_path / "metrics")
    with pytest.raises(SystemExit) as e:
        measure.main(["files", "lake.silver.station_status"])
    assert e.value.code == 1
    assert not (tmp_path / "metrics").exists()


def test_lab_mode_names_only_lab_tables_and_production_never_reads_latest(monkeypatch):
    for k in LAB:
        monkeypatch.delenv(f"DOCKWATCH_{k.upper()}", raising=False)
    prod = Settings(_env_file=None)
    assert (prod.namespace_prefix, prod.starting_offsets) == ("", "earliest")
    # the production names lab_refusal guards are the Settings defaults
    assert (prod.checkpoint_root, prod.topic_station_status, prod.topic_alerts, prod.topic_station_state) == (
        run_mode.PRODUCTION_CHECKPOINT_ROOT,
        run_mode.PRODUCTION_TOPIC_STATION_STATUS,
        run_mode.PRODUCTION_TOPIC_ALERTS,
        run_mode.PRODUCTION_TOPIC_STATION_STATE,
    )
    assert run_mode.lab_refusal(prod) is None
    assert [run_mode.table(prod, t) for t in TABLES] == [f"lake.{t}" for t in TABLES]

    lab = Settings(_env_file=None, **LAB)
    assert run_mode.lab_refusal(lab) is None
    assert [run_mode.table(lab, t) for t in TABLES] == [
        "lake.lab_bronze.station_status",
        "lake.lab_silver.station_status",
        "lake.lab_silver.availability_5m",
        "lake.lab_silver.station_episodes",
    ]
    for key, value in (
        ("checkpoint_root", "/checkpoints"),
        ("checkpoint_root", "/checkpoints/"),
        ("topic_station_status", "gbfs.station_status"),
        ("topic_alerts", "gbfs.alerts"),
        ("topic_station_state", "dockwatch.station_state"),
    ):
        reason = run_mode.lab_refusal(Settings(_env_file=None, **{**LAB, key: value}))
        assert key in (reason or ""), (key, value)
    for bad in ("lab", "Lab_", "lab_;drop", "1lab_", "lab-", "lab_\n"):
        assert run_mode.lab_refusal(Settings(_env_file=None, **{**LAB, "namespace_prefix": bad})), bad
    reason = run_mode.lab_refusal(Settings(_env_file=None, starting_offsets="latest"))
    assert "backlog" in (reason or "")  # production never skips the Kafka backlog

    # tasks.py stream-lab's env gives exactly these settings
    tasks = load_tasks()
    for k, v in {**tasks.LAB_ENV, "DOCKWATCH_CHECKPOINT_ROOT": LAB["checkpoint_root"]}.items():
        monkeypatch.setenv(k, v)
    assert {k: getattr(Settings(_env_file=None), k) for k in LAB} == LAB


def test_stream_lab_starts_one_named_container_with_the_lab_settings_and_refuses_beside_spark(monkeypatch, capsys):
    tasks = load_tasks()
    calls, ready, up = [], [], {"redpanda", "s3", "iceberg-rest", "ops-db"}
    monkeypatch.setattr(tasks, "sh", lambda *cmd: calls.append(cmd))
    monkeypatch.setattr(tasks, "lab_ready", lambda name, ckpt, timeout=240: ready.append((name, ckpt)) or True)
    monkeypatch.setattr(tasks.batch_guard, "running_services", lambda timeout=30: set(up))
    tasks.TASKS["stream-lab"]()
    assert len(calls) == 1
    run = calls[0]
    assert run[:10] == (
        "docker",
        "compose",
        "--profile",
        "spark",
        "run",
        "-d",
        "--rm",
        "--no-deps",
        "--name",
        "dockwatch-stream-lab",
    )
    assert run[-1] == "status-stream"
    env = dict(run[i + 1].split("=", 1) for i, x in enumerate(run) if x == "-e")
    ckpt = env.pop("DOCKWATCH_CHECKPOINT_ROOT")
    assert re.fullmatch(r"/checkpoints/lab/\d{8}T\d{6}Z", ckpt)
    assert env == {
        "DOCKWATCH_NAMESPACE_PREFIX": "lab_",
        "DOCKWATCH_STARTING_OFFSETS": "latest",
        "DOCKWATCH_TOPIC_STATION_STATUS": "gbfs.station_status.replay",
        "DOCKWATCH_TOPIC_ALERTS": "gbfs.alerts.lab",
        "DOCKWATCH_TOPIC_STATION_STATE": "dockwatch.station_state.lab",
    }
    assert not {"-v", "--volume", "-p", "--service-ports"} & set(run)  # no mounts, ports or lineage dir
    assert ready == [("dockwatch-stream-lab", ckpt)]

    for busy in ("status-stream", "cdc-apply", "connect", "trino", "marquez-api"):
        calls.clear()
        up = {busy, "redpanda"}
        with pytest.raises(SystemExit) as e:
            tasks.stream_lab()
        assert (e.value.code, calls) == (1, []), busy
    capsys.readouterr()

    # offsets never resolved: the container is stopped and stream-lab fails
    up, ran = {"redpanda"}, []
    monkeypatch.setattr(tasks, "lab_ready", lambda *a, **kw: False)
    monkeypatch.setattr(tasks.subprocess, "run", lambda cmd, **kw: ran.append(cmd))
    with pytest.raises(SystemExit):
        tasks.stream_lab()
    assert ran == [["docker", "stop", "dockwatch-stream-lab"]]

    # lab_ready waits for every query's sources/0/0 in the container
    answers = iter([1, 1, 0])

    def exec_ls(cmd, **kw):
        ran.append(cmd)
        return type("R", (), {"returncode": next(answers), "stderr": "ls: cannot access"})()

    ran.clear()
    monkeypatch.setattr(tasks.subprocess, "run", exec_ls)
    monkeypatch.setattr(tasks.time, "sleep", lambda s: None)
    lab_ready = load_tasks().lab_ready
    assert lab_ready("dockwatch-stream-lab", "/checkpoints/lab/x") is True
    assert len(ran) == 3
    assert ran[0] == [
        "docker",
        "exec",
        "dockwatch-stream-lab",
        "ls",
        *(
            f"/checkpoints/lab/x/{q}/sources/0/0"
            for q in ("bronze_status", "silver_status", "availability_5m", "episodes")
        ),
    ]

    # stream-lab-stop: docker stop, or nothing when there is no container
    calls.clear()
    monkeypatch.setattr(tasks.subprocess, "run", lambda cmd, **kw: type("R", (), {"returncode": 1})())
    tasks.TASKS["stream-lab-stop"]()
    assert calls == []
    monkeypatch.setattr(tasks.subprocess, "run", lambda cmd, **kw: type("R", (), {"returncode": 0})())
    tasks.TASKS["stream-lab-stop"]()
    assert calls == [("docker", "stop", "dockwatch-stream-lab")]


def test_catchup_and_stream_refuse_while_a_lab_run_holds_the_spark_jvm(monkeypatch, capsys):
    tasks = load_tasks()
    calls, up = [], {"status-stream", "redpanda"}  # a compose run container still carries the service label
    monkeypatch.setattr(tasks, "sh", lambda *cmd: calls.append(cmd))
    monkeypatch.setattr(tasks.batch_guard, "running_services", lambda timeout=30: set(up))
    monkeypatch.setattr(tasks, "connect_running", lambda: False)
    assert tasks.stream_running() is True
    for fn in (tasks.catchup, tasks.TASKS["stream"], tasks.crash_restart):
        with pytest.raises(SystemExit) as e:
            fn()
        assert e.value.code == 1, fn
    assert calls == []
    assert "one Spark JVM at a time" in capsys.readouterr().out

    up = {"redpanda", "s3"}
    assert tasks.stream_running() is False
    tasks.TASKS["stream"]()
    assert calls == [("docker", "compose", "--profile", "spark", "up", "-d", "status-stream")]


def snapshot(doc: dict, ts: int) -> dict:
    snap = copy.deepcopy(doc)
    snap["last_updated"] = ts
    snap["data"]["stations"] = snap["data"]["stations"][:2]
    return snap


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s


def test_replay_skips_and_limits_snapshots_and_keeps_its_schedule(
    tmp_path, monkeypatch, fresh_settings, status_doc, capsys
):
    archive = FsArchive(tmp_path / "raw")
    t = int(datetime(2026, 10, 16, 12, 0, tzinfo=UTC).timestamp())
    feed = [t, t + 60, t + 125, t + 180, t + 240]
    for ts in reversed(feed):  # written out of order: replay sorts the day's keys
        archive.put(archive_key("station_status", ts), json.dumps(snapshot(status_doc, ts)).encode())
    monkeypatch.setenv("DOCKWATCH_ARCHIVE_KIND", "fs")
    monkeypatch.setenv("DOCKWATCH_ARCHIVE_FS_ROOT", str(tmp_path / "raw"))
    clock, sent = Clock(), []

    class SlowPublisher:  # every message takes 0.25 s to publish
        def __init__(self, *a, **kw) -> None:
            pass

        def publish(self, topic, key, value):
            clock.now += 0.25
            sent.append((topic, json.loads(value)))

        def flush(self, timeout=30.0):
            return 0

    monkeypatch.setattr(producer, "KafkaPublisher", SlowPublisher)
    monkeypatch.setattr(producer, "time", clock)
    argv = ["dockwatch.producer", "replay", "--date", "2026-10-16", "--speed", "10", "--skip", "1", "--limit", "3"]
    monkeypatch.setattr(sys, "argv", argv)
    producer.main()

    assert {topic for topic, _ in sent} == {"gbfs.station_status.replay"}
    assert all(v["replay"] for _, v in sent)
    assert [v["feed_last_updated"] for _, v in sent[::2]] == feed[1:4]  # snapshots 1..3, two stations each
    # on schedule: start + (last_updated - first) / 10, whatever publishing costs (0.5 s a snapshot here)
    assert [v["fetched_at"] for _, v in sent[::2]] == [1000.0, 1006.5, 1012.0]
    assert "replayed 3 snapshots (1..3 of 5)" in capsys.readouterr().out

    for args in (["--skip", "5"], ["--limit", "0"], ["--skip", "-1"]):
        monkeypatch.setattr(sys, "argv", ["dockwatch.producer", "replay", "--date", "2026-10-16", *args])
        with pytest.raises(SystemExit):
            producer.main()


VERIFY = """== bronze gaps: per Kafka partition, offsets must be contiguous [gaps = 0]
+---+---+------+------+----+
|  p| lo|    hi|     n|gaps|
+---+---+------+------+----+
|  0|  0|615367|615368|   0|
|  1|  0|565769|565770|   0|
|  2|  0|626052|626053|   0|
|  3|  0|597616|597617|   0|
|  4|  0|659280|659281|   0|
|  5|  0|748815|748816|   0|
+---+---+------+------+----+

== latest snapshot: stations per state
+-----+--------+
|state|stations|
+-----+--------+
|   ok|     402|
+-----+--------+

== verify
check: silver duplicates on (station_id, snapshot_ts) = 0
check: bronze rows = 3812905, distinct (partition, offset) = 3812905
check: bronze offset gaps per partition = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
ok: lake verified
"""
RPK = """PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       5      [0]       196456            615368
1          0       5      [0]       178507            565770
2          0       5      [0]       199085            626053
3          0       5      [0]       190789            597617
4          0       5      [0]       210370            659281
5          0       5      [0]       238014            748816
"""


def test_crash_restart_finds_an_in_flight_batch_and_judges_zero_diff():
    tasks = load_tasks()
    base = tasks.newest_batches("bronze_status:138:138\nsilver_status:230:229\navailability_5m:230:230\nepisodes::\n")
    assert base == {
        "bronze_status": (138, 138),
        "silver_status": (230, 229),
        "availability_5m": (230, 230),
        "episodes": (-1, -1),
    }
    # silver's 230 was left by an earlier stop: it is not this JVM's batch
    assert tasks.in_flight(base, base)["silver_status"] is None
    now = tasks.newest_batches("bronze_status:139:138\nsilver_status:231:230\navailability_5m:231:230\nepisodes:0:\n")
    assert tasks.in_flight(now, base) == {
        "bronze_status": 139,
        "silver_status": 231,
        "availability_5m": 231,
        "episodes": 0,
    }
    done = tasks.newest_batches("bronze_status:139:139\nsilver_status:231:231\navailability_5m:231:231\nepisodes:0:0")
    assert set(tasks.in_flight(done, base).values()) == {None}

    end = tasks.end_offsets(RPK)
    assert end == {0: 615368, 1: 565770, 2: 626053, 3: 597617, 4: 659281, 5: 748816}
    parsed = tasks.parse_verify(VERIFY)
    assert parsed == {
        "bronze_rows": 3812905,
        "bronze_distinct_offsets": 3812905,
        "silver_duplicates": 0,
        "offset_gaps": dict.fromkeys(range(6), 0),
        "bronze_hi": {0: 615367, 1: 565769, 2: 626052, 3: 597616, 4: 659280, 5: 748815},
    }

    rec = {"kafka_end_at_kill": end, "catchup_rc": 0, "verify_rc": 0, **parsed}
    assert {k: tasks.judge(rec)[k] for k in ("duplicates", "missing", "zero_diff")} == {
        "duplicates": 0,
        "missing": 0,
        "zero_diff": True,
    }
    doubled = tasks.judge({**rec, "bronze_rows": 3812907, "silver_duplicates": 1})
    assert (doubled["duplicates"], doubled["zero_diff"]) == (3, False)
    late = tasks.judge({**rec, "kafka_end_at_kill": {**end, 5: 748820}, "offset_gaps": {**parsed["offset_gaps"], 2: 1}})
    assert (late["missing"], late["zero_diff"]) == (1 + 4, False)  # a gap, and 4 offsets published before the kill
    assert tasks.judge({**rec, "verify_rc": 1})["zero_diff"] is False
    unparsed = tasks.judge({**rec, **tasks.parse_verify("Py4JJavaError ...")})
    assert (unparsed["duplicates"], unparsed["missing"], unparsed["zero_diff"]) == (None, None, False)
