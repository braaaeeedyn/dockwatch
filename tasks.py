"""Task runner (this Windows machine has no `make`; the Makefile forwards here).

Usage: python tasks.py <task> [args]
"""

import ast
import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime

# The stdlib-only batch guard (Trino never beside Spark / Connect); nothing else from src is imported here.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
from dockwatch.batch import guard as batch_guard  # noqa: E402

TF_IMAGE = "hashicorp/terraform:1.9"
# Kafka Connect + Debezium (M3, compose service `connect`, profile cdc). The same literal is in docker-compose.yml.
CONNECT_IMAGE = "quay.io/debezium/connect:2.7.3.Final"
# Trino (M4a, compose service `trino`, profile batch). The same literal is in docker-compose.yml.
TRINO_IMAGE = "trinodb/trino:483"
COMPOSE_PROJECT = "dockwatch"
# Loop O lab mode: the streaming job on lake.lab_* tables, .lab topics and its own checkpoints (never production)
LAB_CONTAINER = "dockwatch-stream-lab"
LAB_ENV = {
    "DOCKWATCH_NAMESPACE_PREFIX": "lab_",
    "DOCKWATCH_STARTING_OFFSETS": "latest",
    "DOCKWATCH_TOPIC_STATION_STATUS": "gbfs.station_status.replay",
    "DOCKWATCH_TOPIC_ALERTS": "gbfs.alerts.lab",
    "DOCKWATCH_TOPIC_STATION_STATE": "dockwatch.station_state.lab",
}
STREAM_QUERIES = ("bronze_status", "silver_status", "availability_5m", "episodes")  # one checkpoint dir each
CASING_RECOVERY = (
    "Run tasks from Git Bash in the exact path the containers were created from (cd /c/Users/brady/coding/dockwatch), "
    "never from cmd.exe, PowerShell or Python shell=True. If ops-db was already recreated, recover (keeps the "
    "volume): docker compose --profile stream up -d --wait --no-deps ops-db && "
    "docker compose --profile stream restart iceberg-rest"
)


def sh(*cmd: str) -> None:
    print("+", " ".join(cmd), flush=True)
    code = subprocess.call(list(cmd))
    if code:
        sys.exit(code)


def uv(*args: str) -> None:
    sh("uv", "run", *args)


def terraform(*args: str) -> None:
    """Run terraform through Docker so it doesn't need installing locally."""
    infra = os.path.abspath("infra/terraform")
    sh("docker", "run", "--rm", "-v", f"{infra}:/work", "-w", "/work", TF_IMAGE, *args)


def compose_cwd_refusal(cwd: str, working_dirs) -> str | None:
    """None if Compose may run from `cwd`, else why not.

    `working_dirs` are the com.docker.compose.project.working_dir labels of the existing dockwatch containers. Docker
    Compose compares that path exactly, so a shell that sees the folder with different letter casing (DockWatch vs
    dockwatch) makes Compose recreate every container it touches. Allowed when there are no containers (CI, a fresh
    machine) or every container was created from exactly `cwd`.
    """
    dirs = sorted({d for d in working_dirs if d})
    others = [d for d in dirs if d != cwd]
    if not others:
        return None
    return (
        f"refusing to run: this shell's directory is '{cwd}' but the dockwatch containers were created from "
        f"'{others[0]}'. Docker Compose treats a different path, even one that differs only in letter case, as another "
        f"project directory and recreates the containers it touches (path-casing hazard). {CASING_RECOVERY}"
    )


def project_working_dirs() -> list[str]:
    """working_dir labels of every dockwatch container (empty when Docker is unreachable or there are none)."""
    try:
        out = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--filter",
                f"label=com.docker.compose.project={COMPOSE_PROJECT}",
                "--format",
                '{{.Label "com.docker.compose.project.working_dir"}}',
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    if out.returncode:
        return []
    return [line.strip() for line in out.stdout.splitlines() if line.strip()]


def refuse_beside_batch(task: str) -> None:
    """Spark and Connect tasks never start while Trino runs (and batch tasks never beside Spark / Connect)."""
    up = batch_guard.running_services()
    reason = batch_guard.refusal(task, lambda s: s in up)
    if reason:
        print(reason)
        sys.exit(1)


def stream_running() -> bool:
    """Any running status-stream container: `stream`, a catch-up or a lab run (compose run containers too)."""
    return "status-stream" in batch_guard.running_services()


def connect_running() -> bool:
    out = subprocess.run(
        ["docker", "compose", "--profile", "stream", "--profile", "cdc", "ps", "-q", "--status", "running", "connect"],
        capture_output=True,
        text=True,
    )
    return bool(out.stdout.strip())


def catchup() -> None:
    """Process the Kafka backlog with the same job, image and checkpoints as `stream`, then exit (availableNow)."""
    refuse_beside_batch("catchup")
    if stream_running():
        print("status-stream is running; stop it first (python tasks.py stream-stop): one Spark JVM at a time.")
        sys.exit(1)
    if connect_running():
        print("Kafka Connect is running; stop it first (python tasks.py connect-stop): never beside Spark.")
        sys.exit(1)
    # Only catchup runs the OpenLineage Spark listener (Loop N): one spark-*.jsonl per run into data/lineage
    os.makedirs("data/lineage", exist_ok=True)
    sh(
        "docker",
        "compose",
        "--profile",
        "spark",
        "run",
        "--rm",
        "--no-deps",
        "-e",
        "DOCKWATCH_TRIGGER_MODE=available_now",
        "-e",
        "DOCKWATCH_SPARK_LINEAGE_DIR=/opt/dockwatch/data/lineage",
        "-v",
        f"{os.path.abspath('data/lineage')}:/opt/dockwatch/data/lineage",
        "status-stream",
    )


def inspect_cmd(*args: str) -> list[str]:
    """The inspect_tables.py command, shared by spark_inspect and crash-restart's verify-lake."""
    return [
        "docker",
        "compose",
        "--profile",
        "spark",
        "run",
        "--rm",
        "--no-deps",
        "status-stream",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[1]",
        "--driver-memory",
        "1g",
        # Whole-stage codegen makes this JVM segfault (SymbolTable::do_lookup) on the COUNT(DISTINCT ...) queries;
        # see DEVLOG 2026-10-07 "JVM crash".
        "--conf",
        "spark.sql.codegen.wholeStage=false",
        "/opt/dockwatch/src/dockwatch/streaming/inspect_tables.py",
        *args,
    ]


def spark_inspect(*args: str) -> None:
    """inspect_tables.py in a single small JVM; `--check` (verify-lake) exits 1 if the lake is not consistent."""
    refuse_beside_batch("verify-lake" if "--check" in args else "inspect")
    sh(*inspect_cmd(*args))


def stream(*args: str) -> None:
    """The long-running job (until stream-stop). One Spark JVM at a time: refused while any status-stream container
    runs (a catch-up or a lab run), and beside Trino / Marquez."""
    refuse_beside_batch("stream")
    if stream_running():
        print("a status-stream container is running (stream, catchup or stream-lab); one Spark JVM at a time.")
        sys.exit(1)
    sh("docker", "compose", "--profile", "spark", "up", "-d", "status-stream", *args)


def spark_busy(task: str) -> None:
    """Exit 1 while a Spark JVM (any status-stream container, cdc-apply) or Kafka Connect runs."""
    busy = sorted({"status-stream", "cdc-apply", "connect"} & batch_guard.running_services())
    if busy:
        print(f"{task}: {' and '.join(busy)} running; one Spark JVM at a time, and never beside Kafka Connect.")
        sys.exit(1)


def lab_ready(name: str, checkpoint: str, timeout: float = 240) -> bool:
    """True once each query has written its Kafka source's initial offsets (<checkpoint>/<query>/sources/0/0): only
    then is `latest` resolved, so a replay started afterwards is read in full."""
    paths = [f"{checkpoint}/{q}/sources/0/0" for q in STREAM_QUERIES]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out = subprocess.run(["docker", "exec", name, "ls", *paths], capture_output=True, text=True)
        if out.returncode == 0:
            return True
        if "No such container" in out.stderr or "is not running" in out.stderr:
            return False
        time.sleep(5)
    return False


def stream_lab() -> None:
    """Loop O lab mode: the streaming job in one `compose run` container on lake.lab_bronze / lake.lab_silver, the
    .lab topics and /checkpoints/lab/<UTC>, reading gbfs.station_status.replay from its end. Feed it with
    `python tasks.py replay --date ...`; stop it with stream-lab-stop."""
    refuse_beside_batch("stream")
    spark_busy("stream-lab")
    ckpt = f"/checkpoints/lab/{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    env = {**LAB_ENV, "DOCKWATCH_CHECKPOINT_ROOT": ckpt}
    sh(
        "docker",
        "compose",
        "--profile",
        "spark",
        "run",
        "-d",
        "--rm",
        "--no-deps",
        "--name",
        LAB_CONTAINER,
        *(x for k, v in env.items() for x in ("-e", f"{k}={v}")),
        "status-stream",
    )
    if not lab_ready(LAB_CONTAINER, ckpt):
        print(f"stream-lab: the queries did not resolve their Kafka offsets in time; stopping {LAB_CONTAINER}")
        subprocess.run(["docker", "stop", LAB_CONTAINER], capture_output=True)
        sys.exit(1)
    print(f"stream-lab: ready ({ckpt}); now python tasks.py replay --date <day> --speed <N>", flush=True)


def stream_lab_stop() -> None:
    """Stop the lab container (--rm removes it); nothing to do when there is none."""
    if subprocess.run(["docker", "inspect", LAB_CONTAINER], capture_output=True).returncode:
        print(f"{LAB_CONTAINER}: no container, nothing to stop")
        return
    sh("docker", "stop", LAB_CONTAINER)


# crash-restart polls the production container with one ls: "<query>:<newest offsets id>:<newest commits id>"
CKPT_LS = (
    f"for q in {' '.join(STREAM_QUERIES)}; do "
    'echo "$q:$(ls /checkpoints/$q/offsets 2>/dev/null | grep -xE "[0-9]+" | sort -n | tail -1)'
    ':$(ls /checkpoints/$q/commits 2>/dev/null | grep -xE "[0-9]+" | sort -n | tail -1)"; done'
)


def newest_batches(ls_out: str) -> dict[str, tuple[int, int]]:
    """CKPT_LS output -> query: (newest offsets id, newest commits id), -1 for an empty directory."""
    out = {}
    for line in ls_out.splitlines():
        query, _, rest = line.strip().partition(":")
        if query in STREAM_QUERIES:
            off, _, com = rest.partition(":")
            out[query] = (int(off) if off.isdigit() else -1, int(com) if com.isdigit() else -1)
    return out


def in_flight(now: dict[str, tuple[int, int]], base: dict[str, tuple[int, int]]) -> dict[str, int | None]:
    """Per query, the batch id this JVM planned (newer than the first reading) and has not committed, else None. A
    stale offsets file left by an earlier stop never counts."""
    out = {}
    for q in STREAM_QUERIES:
        off, com = now.get(q, (-1, -1))
        out[q] = off if off > com and off > base.get(q, (-1, -1))[0] else None
    return out


def end_offsets(rpk_out: str) -> dict[int, int]:
    """`rpk topic describe <topic> -p` -> partition: high watermark (the next offset to be written)."""
    out = {}
    for line in rpk_out.splitlines()[1:]:
        parts = line.split()
        if parts and parts[0].isdigit() and parts[-1].isdigit():
            out[int(parts[0])] = int(parts[-1])
    return out


def parse_verify(text: str) -> dict:
    """verify-lake's output -> bronze rows / distinct offsets, silver duplicates, gaps and newest offset per
    partition (the `hi` column of its bronze-gaps table)."""

    def num(pattern: str) -> int | None:
        found = re.search(pattern, text)
        return int(found.group(1)) if found else None

    gaps = re.search(r"check: bronze offset gaps per partition = (\{[^}]*\})", text)
    section = text[text.find("== bronze gaps") :].split("\n== ", 1)[0] if "== bronze gaps" in text else ""
    row = r"^\|\s*(\d+)\|\s*-?\d+\|\s*(\d+)\|\s*\d+\|\s*\d+\|\s*$"  # | p | lo | hi | n | gaps |
    return {
        "bronze_rows": num(r"check: bronze rows = (\d+)"),
        "bronze_distinct_offsets": num(r"check: bronze rows = \d+, distinct \(partition, offset\) = (\d+)"),
        "silver_duplicates": num(r"check: silver duplicates on \(station_id, snapshot_ts\) = (\d+)"),
        "offset_gaps": {int(p): int(n) for p, n in ast.literal_eval(gaps.group(1)).items()} if gaps else {},
        "bronze_hi": {int(p): int(hi) for p, hi in re.findall(row, section, re.M)},
    }


def judge(rec: dict) -> dict:
    """duplicates = bronze rows - distinct offsets + silver duplicates; missing = gaps + offsets published before
    the kill that bronze lacks; zero_diff = both 0, and catchup and verify-lake exited 0."""
    counts = (rec["bronze_rows"], rec["bronze_distinct_offsets"], rec["silver_duplicates"])
    if None in counts or not rec["bronze_hi"] or not rec["kafka_end_at_kill"]:
        return {**rec, "duplicates": None, "missing": None, "zero_diff": False}
    dup = rec["bronze_rows"] - rec["bronze_distinct_offsets"] + rec["silver_duplicates"]
    hi = rec["bronze_hi"]
    late = sum(max(0, end - 1 - hi.get(p, -1)) for p, end in rec["kafka_end_at_kill"].items())
    missing = sum(rec["offset_gaps"].values()) + late
    ok = dup == 0 and missing == 0 and rec["catchup_rc"] == 0 and rec["verify_rc"] == 0
    return {**rec, "duplicates": dup, "missing": missing, "zero_diff": ok}


def service_container(service: str) -> str:
    """The id of the running production (not `compose run`) container of a compose service, or ""."""
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={COMPOSE_PROJECT}",
            "--filter",
            f"label=com.docker.compose.service={service}",
            "--filter",
            "label=com.docker.compose.oneoff=False",
        ],
        capture_output=True,
        text=True,
    )
    return (out.stdout.split() or [""])[0]


def crash_restart() -> None:
    """SIGKILL the production stream with a silver micro-batch in flight, run catchup and verify-lake, and record
    whether the lake has zero duplicates and zero missing Kafka offsets (data/metrics/crash-restart-<UTC>.json)."""
    refuse_beside_batch("stream")
    spark_busy("crash-restart")
    started = datetime.now(UTC)
    stream()
    cid, base, flight, deadline = "", None, {}, time.monotonic() + 180
    while time.monotonic() < deadline:
        cid = cid or service_container("status-stream")
        out = (
            subprocess.run(["docker", "exec", cid, "sh", "-c", CKPT_LS], capture_output=True, text=True)
            if cid
            else None
        )
        if out is not None and out.returncode == 0:
            now = newest_batches(out.stdout)
            base = base or now  # the first reading, before this JVM has planned a batch
            flight = in_flight(now, base)
            if flight["silver_status"] is not None:
                break
        time.sleep(0.5)
    else:
        print("crash-restart: no silver_status batch in flight within 180 s; stopping the stream")
        sh("docker", "compose", "--profile", "spark", "stop", "status-stream")
        sys.exit(1)
    killed = datetime.now(UTC)
    subprocess.run(["docker", "kill", cid], capture_output=True)  # SIGKILL: exit 137, no shutdown hook runs
    print(f"crash-restart: killed {cid[:12]} at {killed:%H:%M:%S}Z, in flight {flight}", flush=True)
    rpk = subprocess.run(
        ["docker", "exec", service_container("redpanda"), "rpk", "topic", "describe", "gbfs.station_status", "-p"],
        capture_output=True,
        text=True,
    )
    try:
        catchup()
        catchup_rc = 0
    except SystemExit as e:
        catchup_rc = e.code if isinstance(e.code, int) else 1
    verify = subprocess.run(inspect_cmd("--check"), capture_output=True, text=True)
    print(verify.stdout[-4000:], flush=True)
    rec = judge(
        {
            "started_at": f"{started:%Y-%m-%dT%H:%M:%SZ}",
            "killed_at": f"{killed:%Y-%m-%dT%H:%M:%SZ}",
            "in_flight": flight,
            "kafka_end_at_kill": end_offsets(rpk.stdout),
            "catchup_rc": catchup_rc,
            "verify_rc": verify.returncode,
            **parse_verify(verify.stdout),
        }
    )
    os.makedirs("data/metrics", exist_ok=True)
    with open(f"data/metrics/crash-restart-{killed:%Y%m%dT%H%M%SZ}.json", "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=2)
    print(json.dumps(rec))
    if not rec["zero_diff"]:
        sys.exit(1)


def dbt_compile(*args: str) -> None:
    """Compile the dbt project offline with the Airflow image's pinned dbt (uv, Python 3.12): every model and test must
    still compile. --no-populate-cache keeps dbt from connecting to Trino, so it runs in CI with no Trino at all."""
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    sh(
        "uv",
        "run",
        "--no-project",
        "--python",
        "3.12",
        "--with-requirements",
        "infra/airflow/dbt-requirements.txt",
        "dbt",
        "compile",
        "--no-populate-cache",
        "--project-dir",
        "dbt/dockwatch",
        "--profiles-dir",
        "dbt/dockwatch",
        "--target",
        "local",
        "--target-path",
        os.path.abspath("data/dbt-compile/target"),  # dbt reads a relative target path from the project dir
        "--log-path",
        "data/dbt-compile/logs",
        *args,
    )


TASKS = {
    "lint": lambda *a: (uv("ruff", "check", "."), uv("ruff", "format", "--check", ".")),
    "fmt": lambda *a: (uv("ruff", "format", "."), uv("ruff", "check", "--fix", ".")),
    "test": lambda *a: uv("pytest", *a),
    "test-integration": lambda *a: uv("pytest", "-m", "integration", *a),
    "up": lambda profile="stream", *a: sh("docker", "compose", "--profile", profile, "up", "-d", "--wait", *a),
    "down": lambda *a: sh("docker", "compose", "--profile", "*", "down", *a),
    "ps": lambda *a: sh("docker", "compose", "--profile", "*", "ps"),
    "setup": lambda *a: uv("python", "-m", "dockwatch.producer", "setup"),
    "produce": lambda *a: uv("python", "-m", "dockwatch.producer", "run", *a),
    "produce-once": lambda *a: uv("python", "-m", "dockwatch.producer", "once", *a),
    "replay": lambda *a: uv("python", "-m", "dockwatch.producer", "replay", *a),
    "stream": stream,
    "stream-logs": lambda *a: sh("docker", "compose", "logs", "--tail", "60", "status-stream", *a),
    "stream-stop": lambda *a: sh("docker", "compose", "--profile", "spark", "stop", "status-stream"),
    "test-spark": lambda *a: sh(
        "docker",
        "run",
        "--rm",
        "-v",
        f"{os.path.abspath('src')}:/opt/dockwatch/src:ro",
        "-v",
        f"{os.path.abspath('tests')}:/opt/dockwatch/tests:ro",
        "-v",
        f"{os.path.abspath('pyproject.toml')}:/opt/dockwatch/pyproject.toml:ro",
        "-e",
        "PYTHONDONTWRITEBYTECODE=1",
        "-e",
        "PYTHONPATH=/opt/dockwatch/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "dockwatch-spark:3.5.5",
        "python3",
        "-m",
        "pytest",
        "-p",
        "no:cacheprovider",
        "-q",
        "-o",
        "addopts=",
        "--rootdir=/opt/dockwatch",
        "/opt/dockwatch/tests/spark",
        *a,
    ),
    "sql": lambda *a: (
        refuse_beside_batch("sql")
        or sh(
            "docker",
            "compose",
            "--profile",
            "spark",
            "run",
            "--rm",
            "--no-deps",
            "status-stream",
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[1]",
            "--driver-memory",
            "768m",
            "--conf",
            "spark.sql.codegen.wholeStage=false",
            "/opt/dockwatch/src/dockwatch/streaming/sql.py",
            *a,
        )
    ),
    "inspect": lambda *a: spark_inspect(),
    "verify-lake": lambda *a: spark_inspect("--check", *a),
    "catchup": lambda *a: catchup(),
    # Loop O measurements (docs/METRICS.md): the lab mode, a SIGKILL crash test, latency / files read from Trino
    "stream-lab": lambda *a: stream_lab(),
    "stream-lab-stop": lambda *a: stream_lab_stop(),
    "crash-restart": lambda *a: crash_restart(),
    "measure": lambda *a: uv("python", "-m", "dockwatch.measure", *a),
    # Loop P (F5): Lighthouse 12.8.2 (tools/lighthouse) on the Live page with Playwright's Chromium, never beside a JVM
    "lighthouse": lambda *a: uv("python", "-m", "dockwatch.lighthouse", *a),
    "export": lambda *a: uv("python", "-m", "dockwatch.exporter", *a),
    # "Replay a day" file from the raw GBFS archive (host only, no Spark). Not `replay`: that re-publishes to Kafka.
    "replay-export": lambda *a: uv("python", "-m", "dockwatch.exporter.replay", *a),
    "geo": lambda *a: uv("--group", "geo", "python", "-m", "dockwatch.geo", *a),
    # M3 ops database (host Python + psycopg against compose ops-db; no JVM)
    "ops-schema": lambda *a: uv("python", "-m", "dockwatch.ops_sim", "schema", *a),
    "ops-sim": lambda *a: uv("python", "-m", "dockwatch.ops_sim", *a),
    "ops-explain": lambda *a: uv("python", "-m", "dockwatch.ops_sim", "explain", *a),
    # M3 CDC: Kafka Connect (profile cdc, on demand) and the Spark MERGE apply (service cdc-apply). Connect and a
    # Spark JVM never run together: the cdc tasks refuse (dockwatch/cdc/guard.py).
    "connect-up": lambda *a: uv("python", "-m", "dockwatch.cdc", "connect-up", *a),
    "connect-stop": lambda *a: sh("docker", "compose", "--profile", "stream", "--profile", "cdc", "stop", "connect"),
    "cdc-reset": lambda *a: uv("python", "-m", "dockwatch.cdc", "reset", *a),
    "cdc-catchup": lambda *a: uv("python", "-m", "dockwatch.cdc", "catchup", *a),
    "cdc-verify": lambda *a: uv("python", "-m", "dockwatch.cdc", "verify", *a),
    "cdc-e2e": lambda *a: uv("python", "-m", "dockwatch.cdc", "e2e", *a),
    # M4a batch: Trino + Airflow (profile batch, on demand; dockwatch/batch). Never beside Spark or Connect.
    "batch-up": lambda *a: uv("python", "-m", "dockwatch.batch", "batch-up", *a),
    "batch-stop": lambda *a: uv("python", "-m", "dockwatch.batch", "batch-stop", *a),
    "airflow": lambda *a: uv("python", "-m", "dockwatch.batch", "airflow", *a),
    "dag-test": lambda *a: uv("python", "-m", "dockwatch.batch", "dag-test", *a),
    "test-airflow": lambda *a: uv("python", "-m", "dockwatch.batch", "test-airflow", *a),
    "scheduler-up": lambda *a: uv("python", "-m", "dockwatch.batch", "scheduler-up", *a),
    "scheduler-stop": lambda *a: uv("python", "-m", "dockwatch.batch", "scheduler-stop", *a),
    "trino-sql": lambda *a: uv("python", "-m", "dockwatch.batch", "trino-sql", *a),
    # Loop M lineage: Marquez (profile obs, on demand) fed from data/lineage; never beside Trino, Spark or Connect.
    "lineage-up": lambda *a: uv("python", "-m", "dockwatch.batch", "lineage-up", *a),
    "lineage-replay": lambda *a: uv("python", "-m", "dockwatch.batch", "lineage-replay", *a),
    "lineage-shot": lambda *a: uv("python", "-m", "dockwatch.batch", "lineage-shot", *a),
    "lineage-stop": lambda *a: uv("python", "-m", "dockwatch.batch", "lineage-stop", *a),
    # dbt project compiled offline (no Trino; also a CI step in the python job)
    "dbt-compile": dbt_compile,
    "tf-validate": lambda *a: (terraform("init", "-backend=false", "-input=false"), terraform("validate")),
    "tf-fmt": lambda *a: terraform("fmt", "-recursive"),
    "tf-fmt-check": lambda *a: terraform("fmt", "-check", "-recursive", "-diff"),
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in TASKS:
        print(__doc__)
        print("tasks:", ", ".join(TASKS))
        sys.exit(1)
    reason = compose_cwd_refusal(os.getcwd(), project_working_dirs())
    if reason:
        print(reason)
        sys.exit(1)
    TASKS[sys.argv[1]](*sys.argv[2:])


if __name__ == "__main__":
    main()
