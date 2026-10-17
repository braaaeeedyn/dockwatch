"""Host side of the batch tasks (M4a): Trino + Airflow lifecycle, one-shot Airflow CLI, `dag-test`, `test-airflow`;
Marquez on demand (Loop M: lineage-up / -replay / -shot / -stop).

Every compose call runs from the inherited working directory (never a resolved path: on this host
Path.resolve() returns the on-disk `DockWatch` casing, and Compose would recreate the stream containers; tasks.py
refuses to run from a different casing). Batch services are always started with --no-deps.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from dockwatch.batch import guard, lineage

AIRFLOW_IMAGE = "dockwatch-airflow:2.11.2"
BATCH_SERVICES = ("trino", "airflow-db", "airflow-scheduler", "airflow-webserver")
DAG_TEST_TIMEOUT_S = 45 * 60
DATA = Path("data")
BUILD_INPUTS = (
    Path("infra/airflow/Dockerfile"),
    Path("infra/airflow/dbt-requirements.txt"),
    Path("infra/airflow/gx-requirements.txt"),
)
OBS_SERVICES = ("marquez-db", "marquez-api", "marquez-web")
MARQUEZ_API = "http://localhost:15000"
MARQUEZ_WEB = "http://localhost:13000"
# The batch graph from trip_history_monthly.load through dbt_build to the marts, the quality gate and the export.
LINEAGE_SHOT_URL = f"{MARQUEZ_WEB}/lineage/job/{lineage.NAMESPACE}/marts_daily.dbt_build?depth=2"
LINEAGE_SHOT = "web/img/lineage-batch.png"
# The node between download and load, at the far end of the graph from the export: drawn once the graph is.
LINEAGE_SHOT_SELECTOR = 'text="data/trips/zips"'


class BatchError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def sh(*cmd: str, timeout: float | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    code = subprocess.run(list(cmd), timeout=timeout).returncode
    if code:
        raise BatchError(f"{' '.join(cmd[:6])} ... exited {code}")


def compose(*args: str, timeout: float | None = 600) -> None:
    sh("docker", "compose", *args, timeout=timeout)


def airflow_run(*args: str) -> list[str]:
    """`docker compose run --rm --no-deps airflow-cli <args>` (the image's entrypoint runs `airflow <args>`)."""
    return ["docker", "compose", "--profile", "airflow-cli", "run", "--rm", "--no-deps", "airflow-cli", *args]


def running_services() -> set[str]:
    return guard.running_services()


def refuse(task: str) -> None:
    up = running_services()
    reason = guard.refusal(task, lambda s: s in up)
    if reason:
        raise BatchError(reason)


def build_hash() -> str:
    """Hash of the image's build inputs (line endings normalised, so a Windows checkout hashes like CI's)."""
    d = hashlib.sha256()
    for p in BUILD_INPUTS:
        d.update(p.read_bytes().replace(b"\r\n", b"\n"))
    return d.hexdigest()[:16]


def image_hash() -> str | None:
    """The image's dockwatch.build-hash label; None when the image is missing."""
    r = subprocess.run(
        ["docker", "image", "inspect", "-f", '{{ index .Config.Labels "dockwatch.build-hash" }}', AIRFLOW_IMAGE],
        capture_output=True,
        text=True,
    )
    return r.stdout.strip() if r.returncode == 0 else None


def ensure_image() -> None:
    """Build the Airflow image when it is missing or was built from other inputs (Dockerfile, dbt-requirements.txt,
    gx-requirements.txt)."""
    want = build_hash()
    if image_hash() != want:
        compose("--profile", "airflow-cli", "build", "--build-arg", f"DOCKWATCH_BUILD_HASH={want}", "airflow-cli",
                timeout=1800)  # fmt: skip


def airflow_db_up() -> None:
    """airflow-db up (no deps) and its schema migrated (idempotent)."""
    (DATA / "lineage").mkdir(parents=True, exist_ok=True)  # the OpenLineage file transport never creates it
    ensure_image()
    compose("--profile", "batch", "up", "-d", "--wait", "--no-deps", "airflow-db", timeout=300)
    sh(*airflow_run("db", "migrate"), timeout=600)


# ---------------------------------------------------------------- tasks


def batch_up() -> None:
    refuse("batch-up")
    (DATA / "lineage").mkdir(parents=True, exist_ok=True)  # the OpenLineage file transport never creates it
    ensure_image()
    compose("--profile", "batch", "up", "-d", "--wait", "--no-deps", "trino", "airflow-db", timeout=300)
    sh(*airflow_run("db", "migrate"), timeout=600)
    print("batch up: trino (http://localhost:18080) and airflow-db; stop with python tasks.py batch-stop", flush=True)


def batch_stop() -> None:
    compose("--profile", "batch", "stop", *BATCH_SERVICES, timeout=300)


def scheduler_up() -> None:
    refuse("scheduler-up")
    airflow_db_up()
    compose("--profile", "batch", "up", "-d", "--no-deps", "airflow-scheduler", timeout=300)


def scheduler_stop() -> None:
    compose("--profile", "batch", "stop", "airflow-scheduler", timeout=300)


def lineage_up() -> None:
    """Marquez (profile obs), always empty: db first (healthy), then api and web; waits for the API."""
    refuse("lineage-up")
    lineage_stop()
    compose("--profile", "obs", "up", "-d", "--wait", "--no-deps", "marquez-db", timeout=300)
    compose("--profile", "obs", "up", "-d", "--no-deps", "marquez-api", "marquez-web", timeout=300)
    deadline = time.monotonic() + 180
    while True:
        try:
            with urllib.request.urlopen(f"{MARQUEZ_API}/api/v1/namespaces", timeout=5):
                break
        except OSError as e:
            if time.monotonic() > deadline:
                raise BatchError(f"Marquez API {MARQUEZ_API} not answering after 180 s ({e}); see "
                                 "docker compose --profile obs logs marquez-api") from e  # fmt: skip
            time.sleep(3)
    print(f"lineage up: Marquez web {MARQUEZ_WEB} (API {MARQUEZ_API}), empty; fill it with python tasks.py "
          "lineage-replay, stop it with python tasks.py lineage-stop", flush=True)  # fmt: skip


def lineage_replay() -> None:
    try:
        counts = lineage.replay(DATA / "lineage", MARQUEZ_API)
    except lineage.ReplayError as e:
        raise BatchError(str(e)) from e
    for dag, n in counts.items():
        print(f"lineage replay: {dag}: {n} events (its newest run)", flush=True)
    print(f"lineage replay: {sum(counts.values())} events into {MARQUEZ_WEB}", flush=True)


def lineage_shot(out: str = LINEAGE_SHOT) -> None:
    """One Playwright CLI screenshot of the batch graph in Marquez web (1600 x 1000) into `out`."""
    node = shutil.which("node")  # not npx: on Windows that is npx.cmd, and cmd.exe would mangle the quoted selector
    if not node:
        raise BatchError("node not found: install Node and run npm ci")
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    sh(node, "node_modules/@playwright/test/cli.js", "screenshot", "--viewport-size", "1600,1000",
       "--wait-for-selector", LINEAGE_SHOT_SELECTOR, "--wait-for-timeout", "3000", LINEAGE_SHOT_URL, out,
       timeout=180)  # fmt: skip


def lineage_stop() -> None:
    """Remove the obs containers and only their anonymous volumes (idempotent)."""
    compose("--profile", "obs", "rm", "--stop", "--force", "--volumes", *OBS_SERVICES, timeout=300)


def airflow(*args: str) -> None:
    sh(*airflow_run(*args))


def trino_sql(query: str) -> None:
    sh("docker", "compose", "--profile", "batch", "exec", "-T", "trino", "trino", "--execute", query)


def test_airflow(*pytest_args: str) -> None:
    """tests/airflow in the Airflow image against a throwaway SQLite metadata DB (like test-spark)."""
    ensure_image()
    mounts = []
    for src, dst in (
        ("src", "src"),
        ("tests", "tests"),
        ("airflow/dags", "airflow/dags"),
        ("dbt", "dbt"),
        ("pyproject.toml", "pyproject.toml"),
    ):
        mounts += ["-v", f"{os.path.abspath(src)}:/opt/dockwatch/{dst}:ro"]
    script = (
        "airflow db migrate > /tmp/migrate.log 2>&1 || { tail -40 /tmp/migrate.log; exit 1; }; "
        'python -m pytest -p no:cacheprovider -q -o addopts= --rootdir=/opt/dockwatch /opt/dockwatch/tests/airflow "$@"'
    )
    sh(
        "docker",
        "run",
        "--rm",
        "--memory",
        "1g",
        *mounts,
        "-e",
        "PYTHONDONTWRITEBYTECODE=1",
        "-e",
        "PYTHONPATH=/opt/dockwatch/src",
        "-e",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db",
        "-e",
        "AIRFLOW__CORE__EXECUTOR=SequentialExecutor",
        "-e",
        "AIRFLOW__CORE__LOAD_EXAMPLES=false",
        "-e",
        "AIRFLOW__CORE__DAGS_FOLDER=/opt/dockwatch/airflow/dags",
        "-e",
        "AIRFLOW__CORE__UNIT_TEST_MODE=false",
        "-e",
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS=dockwatch.batch.airflow_logging.LOGGING_CONFIG",
        "-e",
        "DOCKWATCH_DATA_DIR=/tmp/dockwatch-data",
        "-w",
        "/opt/dockwatch",
        "--entrypoint",
        "bash",
        AIRFLOW_IMAGE,
        "-c",
        script,
        "test-airflow",
        *pytest_args,
        timeout=1800,
    )


# ---------------------------------------------------------------- dag-test


def parse_conf(pairs: list[str]) -> dict:
    """key=value pairs -> conf dict; a value is parsed as JSON when it can be (16 -> int, ["a","b"] -> list)."""
    conf = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise BatchError(f"expected key=value, got {pair!r}")
        try:
            conf[key] = json.loads(value)
        except ValueError:
            conf[key] = value
    return conf


# $4 = 1 while a scheduler runs (a running dag_run may be live then, so stale runs are left alone).
DAG_TEST_SCRIPT = (
    '[ "$4" = 1 ] || python -m dockwatch.batch.runstate fail-stale "$1"; '
    'airflow dags test "$1" "$2" -c "$3"; rc=$?; '
    'python -m dockwatch.batch.runstate "$1" "$2"; echo "AIRFLOW_DAGS_TEST_EXIT $rc"'
)


def dag_test(dag_id: str, pairs: list[str]) -> dict:
    """Run one bounded `airflow dags test` in airflow-cli, read the run's real state, sample memory, write
    data/airflow/dag-test-<dag_id>.json. Starts airflow-db if needed; never starts Trino."""
    from dockwatch.cdc.runner import MemorySampler

    if not re.fullmatch(r"\w+", dag_id):
        raise BatchError(f"bad dag id {dag_id!r}")
    conf = parse_conf(pairs)
    refuse(dag_id)
    if "airflow-cli" in running_services():
        # dag-test fails stale running runs when no scheduler runs; beside a live dag-test / backfill it would fail them
        raise BatchError(
            "another Airflow CLI run (dag-test / dags backfill) is still running in airflow-cli; wait for it, or "
            "find a leftover one with `docker ps --filter name=airflow-cli` and stop it with `docker stop <name>`"
        )
    airflow_db_up()
    logical = datetime.now(UTC).replace(microsecond=0).isoformat()
    seen = running_services()
    sampler = MemorySampler()
    sampler.start()
    started, t0 = now_iso(), time.monotonic()
    state, airflow_exit, result = None, None, {}
    scheduler = "1" if "airflow-scheduler" in seen else "0"
    cmd = airflow_run("bash", "-c", DAG_TEST_SCRIPT, "dag-test", dag_id, logical, json.dumps(conf), scheduler)
    print("+", " ".join(cmd), flush=True)
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace"
    )
    killer = threading.Timer(DAG_TEST_TIMEOUT_S, proc.kill)
    killer.start()
    try:
        for line in proc.stdout:
            print(line, end="", flush=True)
            m = re.match(r"DAG_TEST_STATE (\{.*\})\s*$", line)
            if m:
                result = json.loads(m.group(1))
                state = result.get("state")
            m = re.match(r"AIRFLOW_DAGS_TEST_EXIT (\d+)", line)
            if m:
                airflow_exit = int(m.group(1))
        proc.wait()
    finally:
        killer.cancel()
        peak = sampler.stop()
    seen |= running_services() | set(peak)
    doc = {
        "dag_id": dag_id,
        "conf": conf,
        "logical_date": logical,
        "state": state,
        "started_at": started,
        "finished_at": now_iso(),
        "seconds": round(time.monotonic() - t0, 1),
        "peak_mem_mib": peak,
        "containers_running": sorted(seen),
        "run_id": result.get("run_id"),
        "tasks": result.get("tasks", {}),
        "airflow_dags_test_exit": airflow_exit,
    }
    out = DATA / "airflow" / f"dag-test-{dag_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1) + "\n", encoding="utf-8")
    print(f"dag-test {dag_id}: state={state} in {doc['seconds']} s, peak MiB {peak} -> {out}", flush=True)
    if state != "success":
        sys.exit(1)
    return doc
