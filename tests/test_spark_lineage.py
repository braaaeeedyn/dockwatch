"""Loop N on the host: the OpenLineage Spark listener's settings (on only in catchup, one JSON-lines file per run in
namespace dockwatch-spark), the replay of a real catch-up's events (trimmed: the application run, one micro-batch's
writes and a few runs the replay drops) with Iceberg tables renamed to their Trino names, and leftover Marquez
containers blocking like the API. No pyspark, Airflow, OpenLineage or network here."""

import importlib.util
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dockwatch.batch import guard, lineage
from dockwatch.config import Settings
from dockwatch.streaming.run_mode import lineage_conf

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/lineage/spark-dockwatch-status-stream-20261017T064202Z.jsonl"
APP = "dockwatch_status_stream"
TOPIC = ("kafka://redpanda:9092", "gbfs.station_status")
# replayed job -> (inputs, outputs): the plan's D6 (the foreachBatch MERGE sees only its own target)
PLANNED = {
    f"{APP}.write_to_data_source_v2.lake_bronze_station_status": ({TOPIC}, {lineage.table("bronze.station_status")}),
    f"{APP}.write_to_data_source_v2.lake_silver_station_status": ({TOPIC}, {lineage.table("silver.station_status")}),
    f"{APP}.write_to_data_source_v2.lake_silver_availability_5m": ({TOPIC}, {lineage.table("silver.availability_5m")}),
    f"{APP}.replace_data.lake_silver_station_episodes": (
        {lineage.table("silver.station_episodes")},
        {lineage.table("silver.station_episodes")},
    ),
}


def load_tasks():
    spec = importlib.util.spec_from_file_location("dockwatch_tasks_py_n", ROOT / "tasks.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def io(evs: list[dict]) -> tuple[set, set]:
    return ({(d["namespace"], d["name"]) for e in evs for d in e["inputs"]},
            {(d["namespace"], d["name"]) for e in evs for d in e["outputs"]})  # fmt: skip


def by_run(events: list[dict]) -> dict[str, list[dict]]:
    runs: dict[str, list[dict]] = {}
    for e in events:
        runs.setdefault(e["run"]["runId"], []).append(e)
    return runs


def test_spark_listener_is_off_unless_a_lineage_dir_is_set(monkeypatch):
    monkeypatch.delenv("DOCKWATCH_SPARK_LINEAGE_DIR", raising=False)
    s = Settings(_env_file=None)
    assert s.spark_lineage_dir == ""
    assert lineage_conf(s, "dockwatch-status-stream", datetime.now(UTC)) == {}  # stream, cdc-apply, inspect, sql
    monkeypatch.setenv("DOCKWATCH_SPARK_LINEAGE_DIR", "/opt/dockwatch/data/lineage")
    conf = lineage_conf(Settings(_env_file=None), "dockwatch-status-stream", datetime.now(UTC))
    assert conf["spark.extraListeners"] == "io.openlineage.spark.agent.OpenLineageSparkListener"


def test_spark_listener_writes_one_json_lines_file_per_run_in_its_own_namespace():
    s = Settings(_env_file=None, spark_lineage_dir="/opt/dockwatch/data/lineage")
    first = lineage_conf(s, "dockwatch-status-stream", datetime(2026, 10, 17, 6, 42, 2, tzinfo=UTC))
    assert first == {
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type": "file",
        "spark.openlineage.transport.location": "/opt/dockwatch/data/lineage/"
        "spark-dockwatch-status-stream-20261017T064202Z.jsonl",
        "spark.openlineage.namespace": "dockwatch-spark",
    }
    later = lineage_conf(s, "dockwatch-status-stream", datetime(2026, 10, 17, 7, 0, 0, tzinfo=UTC))
    assert later["spark.openlineage.transport.location"] != first["spark.openlineage.transport.location"]
    assert first["spark.openlineage.namespace"] == lineage.SPARK_NAMESPACE != lineage.NAMESPACE
    assert Path(first["spark.openlineage.transport.location"]).name == FIXTURE.name  # the real run's file name
    for e in lineage.read_json_lines(FIXTURE):  # the real run: one event per line, all in the Spark namespace
        assert e["job"]["namespace"] == "dockwatch-spark"
        assert "/1.54.0/integration/spark" in e["producer"]


def test_spark_tables_take_the_trino_name_through_the_iceberg_rest_symlink():
    seen = set()
    for e in lineage.read_json_lines(FIXTURE):
        for raw in (*e.get("inputs", []), *e.get("outputs", [])):
            d = lineage.spark_dataset(raw)
            seen.add((d["namespace"], d["name"]))
            links = raw.get("facets", {}).get("symlinks", {}).get("identifiers", [])
            if raw["namespace"] == "s3://dockwatch":  # an Iceberg table of the REST catalog, by its location
                assert links == [{"namespace": "http://iceberg-rest:8181", "name": d["name"][5:], "type": "TABLE"}]
                assert d["namespace"] == "trino://trino:8080"
                assert "symlinks" not in d["facets"]
                assert raw["name"] == "warehouse/" + d["name"][5:].replace(".", "/")
            else:  # Kafka topics keep OpenLineage's names
                assert d == raw
    # the names Spark writes are the ones the batch jobs read
    assert {lineage.SILVER, lineage.table("silver.availability_5m"), lineage.table("silver.station_episodes")} <= seen
    assert set(lineage.LINEAGE["marts_daily.dbt_build"][0]) & seen == set(lineage.DBT_SOURCES[:3])
    assert TOPIC in seen
    glue = {"identifiers": [{"namespace": "glue", "name": "x.y", "type": "TABLE"}]}
    other = {"namespace": "s3://dockwatch", "name": "warehouse/x/y", "facets": {"symlinks": glue}}
    assert lineage.spark_dataset(other) == other


def test_spark_replay_keeps_only_runs_that_read_and_write_and_names_each_streaming_write_by_its_table():
    raw = lineage.read_json_lines(FIXTURE)
    kept = lineage.spark_replay_events(raw)
    assert [lineage.event_time(e) for e in kept] == sorted(lineage.event_time(e) for e in kept)
    runs = by_run(kept)
    apps = [evs for evs in runs.values() if not evs[0]["run"]["facets"].get("parent")]
    assert len(apps) == 1
    assert {e["job"]["name"] for e in apps[0]} == {APP}
    assert {e["eventType"] for e in apps[0]} == {"START", "COMPLETE"}
    got = {}
    for evs in runs.values():
        if evs is apps[0]:
            continue
        names = {e["job"]["name"] for e in evs}
        assert len(names) == 1
        got[names.pop()] = io(evs)
        assert evs[0]["run"]["facets"]["parent"]["run"]["runId"] == apps[0][0]["run"]["runId"]
    assert got == PLANNED
    # dropped: DDL, the stateful step (reads Kafka, writes nothing), the Kafka sinks and writes without an input
    dropped = {e["run"]["runId"] for e in raw} - set(runs)
    assert len(dropped) == len(by_run(raw)) - len(runs) >= 5
    for e in raw:
        if e["run"]["runId"] in dropped:
            assert not (e.get("inputs") and e.get("outputs")) or e["job"]["name"].split(".")[1] == "create_table"
    assert all("_file" in e for e in kept)  # the replay names the file and line of a refused event


def test_spark_replay_picks_the_newest_run_with_lineage_after_the_airflow_runs(tmp_path, monkeypatch):
    folder = tmp_path / "lineage"
    folder.mkdir()
    airflow = {"eventType": "COMPLETE", "eventTime": "2026-10-17T08:00:00+00:00", "producer": "p", "schemaURL": "s",
               "job": {"namespace": "dockwatch", "name": "marts_daily"},
               "run": {"runId": "r1", "facets": {"airflowDagRun": {"dagRun": {"run_id": "manual"}}}},
               "inputs": [], "outputs": []}  # fmt: skip
    (folder / "ol-20261017-080000.000000.json").write_text(json.dumps(airflow), encoding="utf-8")
    shutil.copy(FIXTURE, folder / FIXTURE.name)
    # a newer catch-up with nothing new: only the application run and DDL; it must not blank the graph
    empty = [e for e in lineage.read_json_lines(FIXTURE) if ".create_table." in e["job"]["name"] or
             not e["run"]["facets"].get("parent")]  # fmt: skip
    (folder / "spark-dockwatch-status-stream-20261017T090000Z.jsonl").write_text(
        "\n".join(json.dumps({k: v for k, v in e.items() if k != "_file"}) for e in empty) + "\n", encoding="utf-8"
    )
    posted = []
    monkeypatch.setattr(lineage, "post", lambda url, body: posted.append(json.loads(body)) or 201)
    counts = lineage.replay(folder, "http://localhost:15000")
    want = lineage.spark_replay_events(lineage.read_json_lines(FIXTURE))
    assert counts == {"marts_daily": 1, APP: len(want)}
    assert list(counts) == ["marts_daily", APP]  # the Spark run after the Airflow runs
    assert posted[0]["job"]["name"] == "marts_daily"
    assert posted[1:] == [{k: v for k, v in e.items() if k != "_file"} for e in want]
    assert {e["job"]["namespace"] for e in posted[1:]} == {"dockwatch-spark"}
    # a broken line is refused with its file and line
    (folder / "spark-dockwatch-status-stream-20261017T100000Z.jsonl").write_text('{"eventType": \n', encoding="utf-8")
    with pytest.raises(lineage.ReplayError, match=r"spark-dockwatch-status-stream-20261017T100000Z\.jsonl:1"):
        lineage.replay(folder, "http://localhost:15000")


def test_catchup_alone_mounts_data_lineage_and_turns_the_listener_on(tmp_path, monkeypatch):
    tasks = load_tasks()
    calls = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(tasks, "sh", lambda *cmd: calls.append(cmd))
    monkeypatch.setattr(tasks, "stream_running", lambda: False)
    monkeypatch.setattr(tasks, "connect_running", lambda: False)
    monkeypatch.setattr(tasks.batch_guard, "running_services", lambda: {"redpanda", "s3", "iceberg-rest", "ops-db"})
    tasks.catchup()
    (run,) = calls
    assert run[:7] == ("docker", "compose", "--profile", "spark", "run", "--rm", "--no-deps")
    assert run[-1] == "status-stream"
    env = [run[i + 1] for i, a in enumerate(run) if a == "-e"]
    vols = [run[i + 1] for i, a in enumerate(run) if a == "-v"]
    assert env == ["DOCKWATCH_TRIGGER_MODE=available_now", "DOCKWATCH_SPARK_LINEAGE_DIR=/opt/dockwatch/data/lineage"]
    assert vols == [f"{tmp_path / 'data' / 'lineage'}:/opt/dockwatch/data/lineage"]
    assert (tmp_path / "data" / "lineage").is_dir()  # the transport never creates its directory
    # stream and verify-lake run the same image without the listener
    calls.clear()
    tasks.TASKS["stream"]()
    tasks.spark_inspect("--check")
    assert len(calls) == 2
    assert not any("LINEAGE" in a or "data/lineage" in a.replace("\\", "/") for c in calls for a in c)
    for rel in ("src/dockwatch/streaming/status_stream.py", "src/dockwatch/cdc/apply.py"):  # only catchup sets it
        assert "lineage" not in (ROOT / rel).read_text(encoding="utf-8").lower(), rel


def test_marquez_db_or_web_left_running_blocks_like_the_api():
    for task in (*guard.TRINO_TASKS, *guard.SPARK_CONNECT_TASKS, *guard.KEEPALIVE_TASKS):
        api = guard.refusal(task, lambda s: s == "marquez-api")
        for leftover in ("marquez-db", "marquez-web"):
            assert guard.refusal(task, lambda s, x=leftover: s == x) == api, (task, leftover)
        assert "python tasks.py lineage-stop" in api
    # lineage-up removes leftovers itself, so they never block it
    assert guard.refusal("lineage-up", lambda s: s in {"marquez-db", "marquez-web"}) is None
