"""Loop M on the host: the batch lineage table (OpenLineage naming, the quality gate's inputs), the replay of the
newest run of each DAG from data/lineage into Marquez, the Marquez guard both ways round, and the lineage tasks'
compose calls. No Airflow, OpenLineage or network here: Marquez's HTTP answers are monkeypatched."""

import importlib.util
import io
import json
import re
import urllib.error
from pathlib import Path

import pytest

from dockwatch.batch import __main__ as batch_cli
from dockwatch.batch import expectations, guard, host, lineage, marts
from dockwatch.cdc import runner

ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "https://github.com/apache/airflow/tree/providers-openlineage/2.11.0"
OBS = ("marquez-db", "marquez-api", "marquez-web")


def load_tasks():
    spec = importlib.util.spec_from_file_location("dockwatch_tasks_py_m", ROOT / "tasks.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def event(job: str, kind: str, at: str, run_id: str, dag_run: tuple[str, str] | None = None) -> dict:
    """A provider-shaped run event: a task event names its DAG run in the parent facet; a DAG event (dag_run None)
    carries airflowDagRun. `job` without a dot and dag_run None and kind "SPARK" gives an event with neither facet."""
    facet = {"_producer": PRODUCER, "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/X.json"}
    if kind == "SPARK":
        facets, kind = {}, "COMPLETE"
    elif dag_run:
        facets = {"parent": {**facet, "job": {"namespace": "dockwatch", "name": dag_run[0]},
                             "run": {"runId": dag_run[1]}}}  # fmt: skip
    else:
        facets = {"airflowDagRun": {**facet, "dagRun": {"run_id": f"manual__{at}"}}}
    return {
        "eventType": kind,
        "eventTime": at,
        "producer": PRODUCER,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "job": {"namespace": "dockwatch", "name": job},
        "run": {"runId": run_id, "facets": facets},
        "inputs": [],
        "outputs": [],
    }


def two_runs() -> list[dict]:
    """marts_daily twice (the second run's times use another UTC offset, so text order would pick the first) and
    pipeline_health once, plus an event with neither facet."""
    old, new, health = ("marts_daily", "r-old"), ("marts_daily", "r-new"), ("pipeline_health", "r-h")
    return [
        event("marts_daily.guard", "START", "2026-10-16T18:58:00.000000+00:00", "t1", old),
        event("marts_daily.guard", "COMPLETE", "2026-10-16T18:59:00.000000+00:00", "t1", old),
        event("marts_daily", "COMPLETE", "2026-10-16T18:59:30.000000+00:00", "r-old"),
        event("marts_daily", "COMPLETE", "2026-10-16T12:02:30.5-07:00", "r-new"),  # 19:02:30.5 UTC: the newest
        event("marts_daily.guard", "COMPLETE", "2026-10-16T19:01:00Z", "t2", new),
        event("marts_daily.guard", "START", "2026-10-16T12:00:00-07:00", "t2", new),
        event("pipeline_health.guard", "START", "2026-10-16T17:00:00+00:00", "t3", health),
        event("pipeline_health", "COMPLETE", "2026-10-16T17:01:00+00:00", "r-h"),
        event("status_stream", "SPARK", "2026-10-16T20:00:00+00:00", "s1"),
    ]


def write_events(folder: Path, events: list[dict]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for i, e in enumerate(events):
        (folder / f"ol-20261016-1900{i:02d}.000000.json").write_text(json.dumps(e) + "\n", encoding="utf-8")


def test_lineage_names_follow_the_openlineage_trino_s3_and_file_conventions():
    assert set(lineage.LINEAGE) == {
        "trip_history_monthly.download", "trip_history_monthly.load", "marts_daily.stations", "marts_daily.dbt_build",
        "marts_daily.quality_gate", "marts_daily.export", "pipeline_health.minutely", "pipeline_health.export",
    }  # fmt: skip
    assert lineage.table("silver.station_status") == ("trino://trino:8080", "lake.silver.station_status")
    assert lineage.s3("dockwatch", "raw/gbfs/station_information") == ("s3://dockwatch", "raw/gbfs/station_information")
    assert lineage.NAMESPACE == "dockwatch"
    for job, (inputs, outputs) in lineage.LINEAGE.items():
        assert inputs, job
        assert outputs, job
        for ns, name in (*inputs, *outputs):
            if ns == "trino://trino:8080":
                assert re.fullmatch(r"lake\.(bronze|silver|marts|metrics)\.[a-z0-9_]+", name), (job, name)
            elif ns == "file":  # repo-relative, as the DAGs see it under /opt/dockwatch
                assert re.fullmatch(r"(data|web)/[\w./-]*[\w]", name), (job, name)
            else:
                assert re.fullmatch(r"s3://[a-z0-9.-]+", ns), (job, ns)
                assert re.fullmatch(r"[\w.-][\w./-]*", name), (job, name)  # a key or prefix, no leading slash
    # the code's own table names are the lineage's
    assert lineage.table("bronze.station_information")[1] == marts.STATION_TABLE
    assert {name for _, name in lineage.MARTS} == {f"lake.marts.{m}" for m in marts.MARTS}


def test_quality_gate_reads_silver_station_status_besides_the_marts():
    inputs, outputs = lineage.LINEAGE["marts_daily.quality_gate"]
    assert ("trino://trino:8080", expectations.TABLE) in inputs  # the Great Expectations batch
    assert ("trino://trino:8080", "lake.silver.availability_5m") in inputs  # the marts' freshness
    assert set(lineage.MARTS) <= set(inputs)
    assert ("file", "data/dbt/target/run_results.json") in inputs
    assert set(outputs) == {("file", "data/dbt/quality_gate.json"), ("file", "data/quality/expectations.json")}
    # dbt_build writes what the gate reads; the export reads the gate's verdict
    assert set(lineage.MARTS) | {("file", "data/dbt/target/run_results.json")} == set(
        lineage.LINEAGE["marts_daily.dbt_build"][1]
    )
    assert ("file", "data/dbt/quality_gate.json") in lineage.LINEAGE["marts_daily.export"][0]


def test_replay_keeps_the_newest_run_of_each_dag_in_event_time_order():
    newest = lineage.newest_runs(two_runs())
    assert list(newest) == ["marts_daily", "pipeline_health"]  # the Spark-like event (no facets) is not replayed
    assert [(e["job"]["name"], e["eventType"]) for e in newest["marts_daily"]] == [
        ("marts_daily.guard", "START"),
        ("marts_daily.guard", "COMPLETE"),
        ("marts_daily", "COMPLETE"),
    ]
    assert {e["run"]["runId"] for e in newest["marts_daily"]} == {"t2", "r-new"}
    assert [e["eventType"] for e in newest["pipeline_health"]] == ["START", "COMPLETE"]
    assert lineage.dag_run_of(two_runs()[0]) == ("marts_daily", "r-old")
    assert lineage.dag_run_of(two_runs()[2]) == ("marts_daily", "r-old")
    assert lineage.dag_run_of(two_runs()[-1]) is None


def test_replay_posts_every_event_and_fails_loudly_when_marquez_is_down(tmp_path, monkeypatch, capsys):
    folder = tmp_path / "data" / "lineage"
    write_events(folder, two_runs())
    posted = []
    monkeypatch.setattr(lineage, "post", lambda url, body: posted.append((url, json.loads(body))) or 201)
    assert lineage.replay(folder, "http://localhost:15000") == {"marts_daily": 3, "pipeline_health": 2}
    assert {url for url, _ in posted} == {"http://localhost:15000/api/v1/lineage"}
    times = [lineage.event_time(e) for _, e in posted]
    assert times[:3] == sorted(times[:3])
    assert times[3:] == sorted(times[3:])
    assert all("_file" not in e for _, e in posted)

    def refused(req, timeout):
        raise urllib.error.URLError(ConnectionRefusedError(10061, "refused"))

    monkeypatch.undo()
    monkeypatch.setattr(lineage.urllib.request, "urlopen", refused)
    with pytest.raises(lineage.ReplayError, match="lineage-up"):
        lineage.replay(folder, "http://localhost:15000")

    def unprocessable(req, timeout):
        body = io.BytesIO(b'{"code":422,"message":"run.facets.x._producer must not be null"}')
        raise urllib.error.HTTPError(req.full_url, 422, "Unprocessable Entity", {}, body)

    monkeypatch.setattr(lineage.urllib.request, "urlopen", unprocessable)
    with pytest.raises(lineage.ReplayError) as e:
        lineage.replay(folder, "http://localhost:15000")
    assert "422" in str(e.value)
    assert "_producer must not be null" in str(e.value)
    assert re.search(r"ol-20261016-\d+\.\d+\.json", str(e.value))  # names the file
    # through the CLI: exit 1 with the reason
    monkeypatch.setattr(host, "DATA", tmp_path / "data")
    monkeypatch.setattr(lineage.urllib.request, "urlopen", refused)
    with pytest.raises(SystemExit) as code:
        batch_cli.main(["lineage-replay"])
    assert code.value.code == 1
    assert "lineage-up" in capsys.readouterr().err
    with pytest.raises(lineage.ReplayError, match="no Airflow run"):
        lineage.replay(tmp_path / "missing", "http://localhost:15000")


def test_marquez_never_runs_beside_trino_spark_or_connect_either_way_round(monkeypatch, capsys):
    assert guard.LINEAGE_TASKS == ("lineage-up",)
    for busy in ("trino", "status-stream", "cdc-apply", "connect"):
        reason = guard.refusal("lineage-up", lambda s, b=busy: s == b) or ""
        assert ("Trino" if busy == "trino" else busy) in reason, busy
        assert guard.STOP_HINT[busy] in reason
    assert guard.refusal("lineage-up", lambda s: s in {"redpanda", "s3", "iceberg-rest", "ops-db", "console",
                                                       "airflow-db"}) is None  # fmt: skip
    for task in (*guard.TRINO_TASKS, *guard.SPARK_CONNECT_TASKS, *guard.KEEPALIVE_TASKS):
        reason = guard.refusal(task, lambda s: s == "marquez-api") or ""
        assert "Marquez" in reason, task
        assert "python tasks.py lineage-stop" in reason, task
        assert guard.refusal(task, lambda s: s in {"redpanda", "s3", "ops-db", "airflow-db"}) is None, task
    # the messages without Marquez are unchanged
    assert guard.refusal("batch-up", lambda s: s == "connect") == (
        "batch-up: connect running; stop it first (python tasks.py connect-stop). Trino and the Airflow scheduler "
        "never run beside a Spark JVM or Kafka Connect on this host."
    )
    # the CDC runner and tasks.py catchup refuse before any compose call
    monkeypatch.setattr(runner, "running", lambda s: s == "marquez-api")
    with pytest.raises(runner.CdcError, match="lineage-stop"):
        runner.guard("connect-up")
    tasks = load_tasks()
    calls = []
    monkeypatch.setattr(tasks.batch_guard, "running_services", lambda: {"marquez-api", "redpanda"})
    monkeypatch.setattr(tasks, "sh", lambda *cmd: calls.append(cmd))
    with pytest.raises(SystemExit) as e:
        tasks.catchup()
    assert e.value.code == 1
    assert calls == []
    assert "lineage-stop" in capsys.readouterr().out
    assert {"lineage-up", "lineage-replay", "lineage-shot", "lineage-stop"} <= set(tasks.TASKS)


def test_lineage_tasks_touch_only_the_obs_services(tmp_path, monkeypatch):
    calls, up = [], set()
    monkeypatch.setattr(host, "sh", lambda *cmd, timeout=None: calls.append(cmd))
    monkeypatch.setattr(host, "running_services", lambda: up)
    monkeypatch.setattr(host, "ensure_image", lambda: None)
    monkeypatch.setattr(host, "DATA", tmp_path / "data")

    class Answer:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(host.urllib.request, "urlopen", lambda url, timeout: Answer())
    stop = ("docker", "compose", "--profile", "obs", "rm", "--stop", "--force", "--volumes", *OBS)
    host.lineage_up()
    assert calls == [
        stop,  # always starts empty
        ("docker", "compose", "--profile", "obs", "up", "-d", "--wait", "--no-deps", "marquez-db"),
        ("docker", "compose", "--profile", "obs", "up", "-d", "--no-deps", "marquez-api", "marquez-web"),
    ]
    calls.clear()
    host.lineage_stop()
    assert calls == [stop]
    # refused before any compose call, beside Trino / Spark / Connect
    for busy in ("trino", "status-stream", "cdc-apply", "connect"):
        up.clear()
        up.add(busy)
        calls.clear()
        with pytest.raises(host.BatchError):
            host.lineage_up()
        assert calls == [], busy
    # and batch-up refuses beside Marquez before any compose call
    up.clear()
    up.add("marquez-api")
    with pytest.raises(host.BatchError, match="lineage-stop"):
        host.batch_up()
    assert calls == []
    # every Airflow run goes through batch_up or airflow_db_up: both create data/lineage for the file transport
    up.clear()
    host.batch_up()
    assert (tmp_path / "data" / "lineage").is_dir()
    (tmp_path / "data" / "lineage").rmdir()
    host.airflow_db_up()
    assert (tmp_path / "data" / "lineage").is_dir()
    assert all("obs" not in c and not set(c) & set(OBS) for c in calls)  # batch calls never touch Marquez
