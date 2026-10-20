"""Task runner (this Windows machine has no `make`; the Makefile forwards here).

Usage: python tasks.py <task> [args]
"""

import os
import subprocess
import sys

# The stdlib-only batch guard (Trino never beside Spark / Connect); nothing else from src is imported here.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))
from dockwatch.batch import guard as batch_guard  # noqa: E402

TF_IMAGE = "hashicorp/terraform:1.9"
# Kafka Connect + Debezium (M3, compose service `connect`, profile cdc). The same literal is in docker-compose.yml.
CONNECT_IMAGE = "quay.io/debezium/connect:2.7.3.Final"
# Trino (M4a, compose service `trino`, profile batch). The same literal is in docker-compose.yml.
TRINO_IMAGE = "trinodb/trino:483"
COMPOSE_PROJECT = "dockwatch"
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
    out = subprocess.run(
        ["docker", "compose", "--profile", "spark", "ps", "-q", "--status", "running", "status-stream"],
        capture_output=True,
        text=True,
    )
    return bool(out.stdout.strip())


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


def spark_inspect(*args: str) -> None:
    """inspect_tables.py in a single small JVM; `--check` (verify-lake) exits 1 if the lake is not consistent."""
    refuse_beside_batch("verify-lake" if "--check" in args else "inspect")
    sh(
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
    )


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
    "stream": lambda *a: (
        refuse_beside_batch("stream"),
        sh("docker", "compose", "--profile", "spark", "up", "-d", "status-stream", *a),
    ),
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
