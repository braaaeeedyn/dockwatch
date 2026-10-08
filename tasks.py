"""Task runner (this Windows machine has no `make`; the Makefile forwards here).

Usage: python tasks.py <task> [args]
"""

import os
import subprocess
import sys

TF_IMAGE = "hashicorp/terraform:1.9"


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


def stream_running() -> bool:
    out = subprocess.run(
        ["docker", "compose", "--profile", "spark", "ps", "-q", "--status", "running", "status-stream"],
        capture_output=True,
        text=True,
    )
    return bool(out.stdout.strip())


def catchup() -> None:
    """Process the Kafka backlog with the same job, image and checkpoints as `stream`, then exit (availableNow)."""
    if stream_running():
        print("status-stream is running; stop it first (python tasks.py stream-stop): one Spark JVM at a time.")
        sys.exit(1)
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
        "status-stream",
    )


def spark_inspect(*args: str) -> None:
    """inspect_tables.py in a single small JVM; `--check` (verify-lake) exits 1 if the lake is not consistent."""
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
    "stream": lambda *a: sh("docker", "compose", "--profile", "spark", "up", "-d", "status-stream", *a),
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
    "sql": lambda *a: sh(
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
    ),
    "inspect": lambda *a: spark_inspect(),
    "verify-lake": lambda *a: spark_inspect("--check", *a),
    "catchup": lambda *a: catchup(),
    "export": lambda *a: uv("python", "-m", "dockwatch.exporter", *a),
    "geo": lambda *a: uv("--group", "geo", "python", "-m", "dockwatch.geo", *a),
    "tf-validate": lambda *a: (terraform("init", "-backend=false", "-input=false"), terraform("validate")),
    "tf-fmt": lambda *a: terraform("fmt", "-recursive"),
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in TASKS:
        print(__doc__)
        print("tasks:", ", ".join(TASKS))
        sys.exit(1)
    TASKS[sys.argv[1]](*sys.argv[2:])


if __name__ == "__main__":
    main()
