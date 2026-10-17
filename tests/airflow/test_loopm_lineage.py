"""Loop M tests that need the Airflow image (python tasks.py test-airflow): the batch tasks' inlets / outlets are
OpenLineage datasets the provider understands, dbt_build's lineage matches the dbt project, and every DAG parses and
serializes with the OpenLineage provider switched on (file transport into a temp dir, in a subprocess: the image
default keeps it off). Loop M2: a forked OpenLineage emitter never collects garbage inherited from the parent."""

import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

import yaml  # noqa: E402
from airflow.models import DagBag  # noqa: E402
from airflow.providers.openlineage.extractors.manager import ExtractorManager  # noqa: E402
from airflow.providers.openlineage.plugins.listener import OpenLineageListener  # noqa: E402
from openlineage.client.event_v2 import Dataset  # noqa: E402

from dockwatch.batch import lineage  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = ROOT / "airflow" / "dags"
DBT = ROOT / "dbt" / "dockwatch"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def datasets(objs) -> set[tuple[str, str]]:
    out = set()
    for obj in objs:
        assert isinstance(obj, Dataset), obj  # not airflow.lineage.entities.Table: the provider ignores those
        d = ExtractorManager.convert_to_ol_dataset(obj)
        out.add((d.namespace, d.name))
    return out


def test_batch_tasks_carry_the_planned_inlets_and_outlets_as_openlineage_datasets(dagbag):
    assert dagbag.import_errors == {}
    seen = set()
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            job = f"{dag.dag_id}.{task.task_id}"
            want = lineage.LINEAGE.get(job, ((), ()))
            assert datasets(task.inlets) == set(want[0]), job
            assert datasets(task.outlets) == set(want[1]), job
            seen.add(job)
    assert set(lineage.LINEAGE) <= seen  # every planned task exists (annotate raises at parse time otherwise)
    gate = dagbag.get_dag("marts_daily").get_task("quality_gate")
    assert ("trino://trino:8080", "lake.silver.station_status") in datasets(gate.inlets)


def test_dbt_build_lineage_matches_the_dbt_project_sources_and_marts(dagbag):
    sources = yaml.safe_load((DBT / "models" / "sources.yml").read_text(encoding="utf-8"))["sources"]
    want_in = {(lineage.TRINO, f"{s['database']}.{s['schema']}.{t['name']}") for s in sources for t in s["tables"]}
    target = yaml.safe_load((DBT / "profiles.yml").read_text(encoding="utf-8"))["dockwatch"]["outputs"]["local"]
    marts = sorted((DBT / "models" / "marts").glob("*.sql"))
    want_out = {(lineage.TRINO, f"{target['database']}.{target['schema']}.{p.stem}") for p in marts}
    assert len(want_in) == 5
    assert len(want_out) == 3
    task = dagbag.get_dag("marts_daily").get_task("dbt_build")
    assert datasets(task.inlets) == want_in  # every dbt source (station_status is read by its source tests)
    assert datasets(task.outlets) == want_out | {("file", "data/dbt/target/run_results.json")}
    # staging is ephemeral: no stg_* dataset, which is why openlineage-dbt is not used
    project = yaml.safe_load((DBT / "dbt_project.yml").read_text(encoding="utf-8"))
    assert project["models"]["dockwatch"]["staging"]["+materialized"] == "ephemeral"
    assert not any("stg_" in name for _, name in datasets(task.inlets) | datasets(task.outlets))


PROBE = """
import json, sys
from airflow import plugins_manager
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagBag
from airflow.providers.openlineage import conf
from airflow.serialization.serialized_objects import SerializedDAG

plugins_manager.ensure_plugins_loaded()
plugins_manager.integrate_listener_plugins(get_listener_manager())
listeners = [type(x).__name__ for x in get_listener_manager().pm.get_plugins()]
bag = DagBag(dag_folder=sys.argv[1], include_examples=False)
dags = {}
for dag_id, dag in bag.dags.items():
    back = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))  # to_dict validates the schema (the scheduler path)
    dags[dag_id] = {"tasks": sorted(t.task_id for t in dag.tasks), "back": sorted(t.task_id for t in back.tasks),
                    "edges": sorted((t.task_id, d) for t in back.tasks for d in t.downstream_task_ids)}
print("PROBE " + json.dumps({"disabled": conf.is_disabled(), "transport": conf.transport(),
                             "namespace": conf.namespace(), "listeners": listeners,
                             "errors": {k: str(v) for k, v in bag.import_errors.items()}, "dags": dags}))
"""


def test_every_dag_parses_and_serializes_with_the_openlineage_provider_enabled(tmp_path, dagbag):
    env = dict(os.environ)
    env.update({
        "AIRFLOW__OPENLINEAGE__DISABLED": "false",
        "AIRFLOW__OPENLINEAGE__NAMESPACE": "dockwatch",
        "AIRFLOW__OPENLINEAGE__TRANSPORT": json.dumps({"type": "file", "log_file_path": f"{tmp_path}/ol",
                                                       "append": False}),
    })  # fmt: skip
    r = subprocess.run([sys.executable, "-c", PROBE, str(DAGS_FOLDER)], capture_output=True, text=True, env=env,
                       timeout=600)  # fmt: skip
    assert r.returncode == 0, r.stderr[-3000:]
    line = next(x for x in r.stdout.splitlines() if x.startswith("PROBE "))
    got = json.loads(line[len("PROBE ") :])
    assert got["disabled"] is False
    assert got["transport"]["type"] == "file"
    assert got["namespace"] == "dockwatch"
    assert "OpenLineageListener" in got["listeners"]
    assert got["errors"] == {}
    assert set(got["dags"]) == set(dagbag.dag_ids)
    assert {"marts_daily", "trip_history_monthly", "pipeline_health"} <= set(got["dags"])
    for dag_id, d in got["dags"].items():
        assert d["back"] == d["tasks"], dag_id
        want = sorted([t.task_id, x] for t in dagbag.get_dag(dag_id).tasks for x in t.downstream_task_ids)
        assert d["edges"] == want, dag_id


FORK_PROBE = """
import gc, json, os
import dockwatch.batch.lineage  # noqa: F401 - installs the at-fork GC freeze

r, w = os.pipe()
PARENT = os.getpid()


class Garbage:  # stands in for the pyarrow S3FileSystem whose destructor joins parent-only curl threads
    def __del__(self):
        os.write(w, b"C" if os.getpid() != PARENT else b"P")


gc.collect()
gc.disable()  # keep the garbage pending, as between two automatic passes
g = Garbage()
g.cycle = g
del g
pid = os.fork()
if pid == 0:
    gc.enable()
    gc.collect()  # the pass the provider's forked child ran inside get_template_context()
    os._exit(0)
_, status = os.waitpid(pid, 0)
gc.enable()
gc.collect()
os.close(w)
print("FORK " + json.dumps({"exit": os.waitstatus_to_exitcode(status), "finalized": os.read(r, 64).decode()}))
"""


def test_forked_openlineage_emitters_never_collect_inherited_garbage():
    # provider 2.11.0 emits every task event from a fork; if it stops forking, the at-fork freeze can go
    assert "os.fork()" in inspect.getsource(OpenLineageListener._fork_execute)
    r = subprocess.run([sys.executable, "-c", FORK_PROBE], capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-3000:]
    got = json.loads(next(x for x in r.stdout.splitlines() if x.startswith("FORK "))[len("FORK ") :])
    assert got["exit"] == 0  # the child survived its GC pass
    assert got["finalized"] == "P"  # never in the child ("C"); the parent still collects its own garbage
