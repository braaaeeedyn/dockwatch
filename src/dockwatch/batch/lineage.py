"""OpenLineage for the batch DAGs (Loop M): what each batch task reads and writes, and the replay of the Airflow file
transport's events (data/lineage/ol-*.json) into Marquez.

Stdlib only; `annotate()` imports OpenLineage lazily (only the Airflow image has it). Dataset names follow the
OpenLineage conventions: lake tables as Trino names them (namespace trino://trino:8080, name lake.<schema>.<table>),
repository files by repo-relative path (namespace file), S3 objects by bucket and key or prefix (namespace s3://<bucket>).
dbt's lineage is carried by marts_daily.dbt_build (every dbt source in, the marts out): openlineage-dbt would read the
ephemeral staging models as phantom datasets. Importing this module also makes the OpenLineage provider's forked
event emitters safe (the at-fork GC freeze below).
"""

import gc
import json
import os
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

NAMESPACE = "dockwatch"
TRINO = "trino://trino:8080"
FILE = "file"

if hasattr(os, "register_at_fork"):  # POSIX only; the host never forks
    # The OpenLineage provider emits every task event from os.fork(). A GC pass in that child freed garbage inherited
    # from the parent (a pyarrow S3FileSystem whose curl destructor joins a parent-only thread): SIGSEGV, event lost.
    os.register_at_fork(before=gc.freeze, after_in_parent=gc.unfreeze)


def table(name: str) -> tuple[str, str]:
    """A lake table, `schema.table` -> (trino://trino:8080, lake.schema.table)."""
    return TRINO, f"lake.{name}"


def repo_file(path: str) -> tuple[str, str]:
    return FILE, path


def s3(bucket: str, key: str) -> tuple[str, str]:
    return f"s3://{bucket}", key


MARTS = tuple(table(f"marts.{m}") for m in ("mart_station_availability_daily", "mart_rebalancing_need",
                                             "mart_trips_vs_availability"))  # fmt: skip
SILVER = table("silver.station_status")
DBT_SOURCES = (SILVER, table("silver.availability_5m"), table("silver.station_episodes"), table("bronze.trips"),
               table("bronze.station_information"))  # fmt: skip
RUN_RESULTS = repo_file("data/dbt/target/run_results.json")
GATE = repo_file("data/dbt/quality_gate.json")
EXPECTATIONS = repo_file("data/quality/expectations.json")
ZIPS = repo_file("data/trips/zips")

# job (<dag>.<task>) -> (inputs, outputs). Guards and the `published` sensor have no datasets.
LINEAGE = {
    "trip_history_monthly.download": ((s3("baywheels-data", "tripdata"),), (ZIPS,)),
    "trip_history_monthly.load": ((ZIPS,), (table("bronze.trips"), table("metrics.trip_loads"))),
    "marts_daily.stations": (
        (s3("dockwatch", "raw/gbfs/station_information"),),
        (table("bronze.station_information"),),
    ),
    "marts_daily.dbt_build": (DBT_SOURCES, (*MARTS, RUN_RESULTS)),
    # the GE batch is the newest 24 h of silver.station_status; marts freshness is checked against availability_5m
    "marts_daily.quality_gate": ((*MARTS, SILVER, table("silver.availability_5m"), RUN_RESULTS), (GATE, EXPECTATIONS)),
    "marts_daily.export": ((*MARTS, GATE), (repo_file("web/data/insights"),)),
    "pipeline_health.minutely": ((SILVER,), (table("metrics.pipeline_minutely"),)),
    "pipeline_health.export": (
        (
            table("metrics.pipeline_minutely"),
            SILVER,
            table("metrics.maintenance_runs"),
            GATE,
            EXPECTATIONS,
            RUN_RESULTS,
        ),
        (repo_file("web/data/health.json"),),
    ),
}


class ReplayError(RuntimeError):
    pass


def annotate(dag) -> None:
    """Set the planned inlets / outlets (OpenLineage datasets) on `dag`'s batch tasks. A planned task missing from the
    DAG raises (TaskNotFound) at parse time."""
    from openlineage.client.event_v2 import Dataset

    for job, (inputs, outputs) in LINEAGE.items():
        dag_id, task_id = job.split(".", 1)
        if dag_id == dag.dag_id:
            task = dag.get_task(task_id)
            task.inlets = [Dataset(namespace=ns, name=name) for ns, name in inputs]
            task.outlets = [Dataset(namespace=ns, name=name) for ns, name in outputs]


# ---------------------------------------------------------------- replay


def event_time(e: dict) -> datetime:
    return datetime.fromisoformat(e["eventTime"].replace("Z", "+00:00"))


def dag_run_of(e: dict) -> tuple[str, str] | None:
    """(DAG job name, DAG run id) of an Airflow event: a task's from its parent facet, a DAG's from itself, else
    None (not an Airflow event)."""
    facets = e.get("run", {}).get("facets") or {}
    if facets.get("parent"):
        return facets["parent"]["job"]["name"], facets["parent"]["run"]["runId"]
    if "airflowDagRun" in facets:
        return e["job"]["name"], e["run"]["runId"]
    return None


def newest_runs(events: list[dict]) -> dict[str, list[dict]]:
    """DAG -> the events of its newest run (the run with the latest eventTime), sorted by eventTime."""
    runs: dict[tuple[str, str], list[dict]] = {}
    for e in events:
        key = dag_run_of(e)
        if key:
            runs.setdefault(key, []).append(e)
    best: dict[str, list[dict]] = {}
    for (dag, _), evs in runs.items():
        if dag not in best or max(map(event_time, evs)) > max(map(event_time, best[dag])):
            best[dag] = evs
    return {dag: sorted(evs, key=event_time) for dag, evs in sorted(best.items())}


def read_events(folder: Path) -> list[dict]:
    """Every ol-*.json event in `folder`, each with its file name under `_file`."""
    out = []
    for p in sorted(folder.glob("ol-*.json")):
        try:
            e = json.loads(p.read_text(encoding="utf-8"))
        except ValueError as ex:
            raise ReplayError(f"{p}: not valid JSON ({ex})") from ex
        out.append({**e, "_file": p.name})
    return out


def post(url: str, body: bytes, timeout: float = 30) -> int:
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status


def replay(folder: Path, api: str) -> dict[str, int]:
    """POST the newest run of each DAG in `folder` to Marquez (`api`), in event-time order; DAG -> events posted.
    Stops at the first failure: a connection error, or any non-2xx answer (status, body and file in the message)."""
    if not folder.is_dir():
        raise ReplayError(f"{folder} does not exist: no Airflow run has written OpenLineage events yet")
    counts = {}
    for dag, evs in newest_runs(read_events(folder)).items():
        for e in evs:
            name = e.pop("_file")
            try:
                status = post(f"{api}/api/v1/lineage", json.dumps(e).encode("utf-8"))
            except urllib.error.HTTPError as ex:
                body = ex.read().decode("utf-8", "replace")[:2000]
                raise ReplayError(f"Marquez answered {ex.code} to {name}: {body}") from ex
            except OSError as ex:
                raise ReplayError(f"Marquez is not answering at {api} ({ex}); start it with python tasks.py "
                                  "lineage-up") from ex  # fmt: skip
            if not 200 <= status < 300:
                raise ReplayError(f"Marquez answered {status} to {name}")
        counts[dag] = len(evs)
    return counts
