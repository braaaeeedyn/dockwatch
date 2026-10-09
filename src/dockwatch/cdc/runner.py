"""Host side of the CDC tasks: Connect lifecycle, reset, catch-up, verify and the bounded seeded end-to-end run.

Spark work runs in the `cdc-apply` compose service (`docker compose --profile cdc-apply run --rm --no-deps`); its
output is echoed and the marker lines (CDC_RESULT / CDC_ICEBERG) are parsed. Memory rule (guard.refusal): Kafka
Connect and a Spark JVM never run together. Results go to data/cdc/{catchup,verify,e2e-report}.json.
"""

import base64
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from dockwatch.cdc.checksum import table_checksum
from dockwatch.cdc.guard import cdc_topics, refusal, slot_refusal, slot_warning
from dockwatch.config import get_settings

UTC = timezone.utc  # noqa: UP017 - the cdc package stays Python 3.10-compatible

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data" / "cdc"
CONNECTOR_FILE = ROOT / "infra" / "connect" / "ops.json"
PROJECT = "dockwatch"
TABLES = ("stations", "docks", "vans", "rebalancing_jobs", "maintenance_tickets")
SCHEMA_CHANGE = ("rebalancing_jobs", "priority")  # sql/ops/migrations/001_rebalancing_jobs_priority.sql
STREAM_SERVICES = ("redpanda", "s3", "iceberg-rest", "ops-db")
SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit",
    "--master",
    "local[1]",
    "--driver-memory",
    "1g",
    "--conf",
    "spark.sql.codegen.wholeStage=false",  # same precaution as tasks.py inspect (DEVLOG 2026-10-07 JVM crash)
)
APP_DIR = "/opt/dockwatch/src/dockwatch/cdc"
SPARK_TIMEOUT_S = 12 * 60
E2E_TIMEOUT_S = 25 * 60


class CdcError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def write_json(name: str, doc: dict) -> Path:
    DATA.mkdir(parents=True, exist_ok=True)
    path = DATA / name
    path.write_text(json.dumps(doc, indent=1, sort_keys=False, default=str) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------- docker


def running_services() -> list[str]:
    """Compose services of this project with a running container (one-off `compose run` containers included)."""
    out = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--format",
            '{{.Label "com.docker.compose.service"}}',
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return sorted({line.strip() for line in out.stdout.splitlines() if line.strip()})


def running(service: str) -> bool:
    return service in running_services()


def guard(task: str) -> None:
    reason = refusal(task, running)
    if reason:
        raise CdcError(reason)


def sh(*cmd: str, timeout: float | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.run(list(cmd), check=True, timeout=timeout)


def spark(script: str, *args: str) -> dict[str, dict]:
    """Run a cdc/*.py Spark script in the cdc-apply service; return its marker lines {"CDC_RESULT": {...}, ...}."""
    cmd = [
        "docker",
        "compose",
        "--profile",
        "cdc-apply",
        "run",
        "--rm",
        "--no-deps",
        "cdc-apply",
        *SPARK_SUBMIT,
        f"{APP_DIR}/{script}",
        *args,
    ]
    print("+", " ".join(cmd), flush=True)
    markers: dict[str, dict] = {}
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace"
    )
    killer = threading.Timer(SPARK_TIMEOUT_S, proc.kill)
    killer.start()
    try:
        for line in proc.stdout:
            print(line, end="", flush=True)
            m = re.match(r"(CDC_[A-Z]+) (\{.*\})\s*$", line)
            if m:
                markers[m.group(1)] = json.loads(m.group(2))
        code = proc.wait()
    finally:
        killer.cancel()
    if code:
        raise CdcError(f"{script} exited {code}")
    return markers


class MemorySampler(threading.Thread):
    """`docker stats --no-stream` every ~3 s while a step runs: peak MiB per compose service."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.peak: dict[str, int] = {}
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.sample()
            except Exception as e:  # noqa: BLE001 - a sampler never breaks the run
                print("memory sampler:", e, flush=True)
            self.stop_event.wait(3)

    def sample(self) -> None:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        for line in out.stdout.splitlines():
            name, _, usage = line.partition("\t")
            service = service_of(name)
            mib = to_mib(usage.split("/")[0])
            if service and mib is not None:
                self.peak[service] = max(self.peak.get(service, 0), round(mib))

    def stop(self) -> dict[str, int]:
        self.stop_event.set()
        self.join(timeout=40)
        return dict(sorted(self.peak.items()))


def service_of(container: str) -> str | None:
    """dockwatch-connect-1 -> connect; dockwatch-cdc-apply-run-1a2b3c -> cdc-apply; other projects -> None."""
    if not container.startswith(f"{PROJECT}-"):
        return None
    name = container[len(PROJECT) + 1 :]
    name = re.sub(r"-run-[0-9a-f]+$", "", name)
    return re.sub(r"-\d+$", "", name)


def to_mib(text: str) -> float | None:
    m = re.match(r"\s*([\d.]+)\s*([KMG]i?B|B)\s*$", text)
    if not m:
        return None
    scale = {"B": 1 / 1024**2, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}
    return float(m.group(1)) * scale[m.group(2)]


# ---------------------------------------------------------------- Kafka Connect


def connector() -> dict:
    return json.loads(CONNECTOR_FILE.read_text(encoding="utf-8"))


def http(method: str, url: str, body: dict | None = None, timeout: float = 10) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read() or b"{}")


def slot_active(slot: str) -> bool:
    import psycopg

    with psycopg.connect(get_settings().ops_db_dsn, connect_timeout=10) as conn:
        row = conn.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,)).fetchone()
    return bool(row and row[0])


def slot_status(slot: str, dsn: str | None = None) -> str | None:
    """pg_replication_slots.wal_status of `slot` (reserved / extended / unreserved / lost), None if there is none."""
    import psycopg

    with psycopg.connect(dsn or get_settings().ops_db_dsn, connect_timeout=10) as conn:
        row = conn.execute("SELECT wal_status FROM pg_replication_slots WHERE slot_name = %s", (slot,)).fetchone()
    return row[0] if row else None


def check_slot(slot: str) -> None:
    """Refuse (CdcError) on a lost slot; warn on stderr on an unreserved one."""
    status = slot_status(slot)
    reason = slot_refusal(slot, status)
    if reason:
        raise CdcError(reason)
    warning = slot_warning(slot, status)
    if warning:
        print(warning, file=sys.stderr, flush=True)


def connect_up(timeout_s: float = 180) -> dict:
    """Start Connect (profile cdc), register infra/connect/ops.json, wait until it streams from an active slot.

    Before anything starts: the memory guard, the lost-slot check, and unlimited retention on existing ops.public.*.
    """
    guard("connect-up")
    check_slot(connector()["config"]["slot.name"])
    ensure_cdc_topic_retention(admin())
    sh("docker", "compose", "--profile", "stream", "--profile", "cdc", "up", "-d", "--wait", "connect", timeout=300)
    url, doc = get_settings().connect_url.rstrip("/"), connector()
    name, cfg = doc["name"], doc["config"]
    print(f"registering connector {name}", flush=True)
    http("PUT", f"{url}/connectors/{name}/config", cfg, timeout=30)
    deadline = time.monotonic() + timeout_s
    status: dict = {}
    while time.monotonic() < deadline:
        try:
            status = http("GET", f"{url}/connectors/{name}/status")
        except urllib.error.URLError as e:
            status = {"error": str(e)}
        tasks = status.get("tasks") or []
        failed = [t for t in tasks if t.get("state") == "FAILED"]
        if failed:
            raise CdcError(f"connector task failed: {failed[0].get('trace', '')[:2000]}")
        if (
            (status.get("connector") or {}).get("state") == "RUNNING"
            and tasks
            and all(t.get("state") == "RUNNING" for t in tasks)
            and slot_active(cfg["slot.name"])
        ):
            print(f"connector {name} RUNNING, slot {cfg['slot.name']} active", flush=True)
            return status
        time.sleep(2)
    raise CdcError(f"connector {name} not RUNNING with an active slot after {timeout_s:.0f} s: {status}")


def connect_stop() -> None:
    sh("docker", "compose", "--profile", "stream", "--profile", "cdc", "stop", "connect", timeout=120)


def connect_heap() -> str | None:
    """The effective -Xmx of the running Connect JVM (from the container's process list in /proc)."""
    out = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "stream",
            "--profile",
            "cdc",
            "exec",
            "-T",
            "connect",
            "sh",
            "-c",
            'for f in /proc/[0-9]*/cmdline; do tr "\\0" " " < "$f"; echo; done',  # the image has no `ps`
        ],
        capture_output=True,
        text=True,
    )
    m = re.findall(r"-Xmx\S+", out.stdout)
    return m[-1] if m else None


# ---------------------------------------------------------------- Kafka


def admin():
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap})


CDC_TOPIC_RETENTION = {"retention.ms": "-1", "cleanup.policy": "delete"}


def ensure_cdc_topic_retention(client) -> list[str]:
    """Set retention.ms=-1 / cleanup.policy=delete on every existing ops.public.* topic (and nothing else).

    Idempotent. New topics get the same settings from the connector's topic.creation group (infra/connect/ops.json);
    this upgrades topics created before it (7-day default). Returns the topics altered.
    """
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource, ResourceType

    topics = sorted(t for t in client.list_topics(timeout=10).topics if t.startswith("ops.public."))
    if not topics:
        return []
    resources = [
        ConfigResource(
            ResourceType.TOPIC,
            t,
            incremental_configs=[
                ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET) for k, v in CDC_TOPIC_RETENTION.items()
            ],
        )
        for t in topics
    ]
    for fut in client.incremental_alter_configs(resources).values():
        fut.result()
    print(f"retention.ms=-1, cleanup.policy=delete on {', '.join(topics)}", flush=True)
    return topics


def delete_cdc_topics(timeout_s: float = 60) -> list[str]:
    """Delete Debezium's ops.public.* and Connect's dockwatch-connect-* topics (and nothing else)."""
    client = admin()
    doomed = cdc_topics(client.list_topics(timeout=10).topics)
    if doomed:
        for topic, fut in client.delete_topics(doomed, operation_timeout=30).items():
            fut.result()
            print("deleted topic", topic, flush=True)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and cdc_topics(client.list_topics(timeout=10).topics):
        time.sleep(1)
    return doomed


def kafka_counts(timeout_s: float = 60) -> dict[str, dict[str, int]]:
    """Per table and envelope op, the change events now in ops.public.* (read from the start to the high mark)."""
    from confluent_kafka import Consumer, TopicPartition

    counts = {t: {"c": 0, "u": 0, "d": 0, "r": 0} for t in TABLES}
    c = Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap,
            "group.id": f"dockwatch-cdc-count-{os.getpid()}",
            "enable.auto.commit": False,
        }
    )
    try:
        md = c.list_topics(timeout=10)
        assign, remaining = [], {}
        for topic in sorted(t for t in md.topics if t.startswith("ops.public.")):
            for p in md.topics[topic].partitions:
                lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
                if hi > lo:
                    assign.append(TopicPartition(topic, p, lo))
                    remaining[(topic, p)] = hi
        c.assign(assign)
        deadline = time.monotonic() + timeout_s
        while remaining and time.monotonic() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            k = (msg.topic(), msg.partition())
            if msg.value() is not None:
                op = json.loads(msg.value())["payload"]["op"]
                table = msg.topic().rsplit(".", 1)[-1]
                counts.setdefault(table, {"c": 0, "u": 0, "d": 0, "r": 0})
                counts[table][op] = counts[table].get(op, 0) + 1
            if k in remaining and msg.offset() + 1 >= remaining[k]:
                del remaining[k]
        if remaining:
            raise CdcError(f"could not read ops.public.* to the end in {timeout_s} s: {remaining}")
    finally:
        c.close()
    return counts


# ---------------------------------------------------------------- Postgres


def pg_connect():
    import psycopg

    return psycopg.connect(get_settings().ops_db_dsn, autocommit=True, connect_timeout=10)


def pg_columns(conn) -> dict[str, list[str]]:
    rows = conn.execute(
        """SELECT table_name, column_name FROM information_schema.columns
           WHERE table_schema = 'public' AND table_name = ANY(%s) ORDER BY table_name, ordinal_position""",
        (list(TABLES),),
    ).fetchall()
    out: dict[str, list[str]] = {t: [] for t in TABLES}
    for table, column in rows:
        out[table].append(column)
    return out


def pg_side(conn, columns: dict[str, list[str]]) -> dict[str, dict]:
    out = {}
    for t in TABLES:
        cols = ", ".join(f'"{c}"' for c in columns[t])
        rows = conn.execute(f"SELECT {cols} FROM {t}").fetchall()
        out[t] = {"pg_rows": len(rows), "pg_checksum": table_checksum(rows)}
    return out


def reset_pg_kafka() -> None:
    """With Connect stopped: drop the slot and the five tables, re-create the base schema + publication, and delete
    the CDC topics. Connect re-creates the slot (slot.name) when the connector is registered again."""
    from dockwatch.ops_sim import db

    if running("connect"):
        raise CdcError("connect is running; stop it first (python tasks.py connect-stop)")
    with pg_connect() as conn:
        slot = connector()["config"]["slot.name"]
        conn.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (slot,)
        )
        conn.execute("DROP TABLE IF EXISTS maintenance_tickets, rebalancing_jobs, docks, vans, stations CASCADE")
        for f in db.apply_schema(conn, base_only=True):
            print("applied", f, flush=True)
    delete_cdc_topics()


# ---------------------------------------------------------------- tasks


def reset() -> dict:
    guard("cdc-reset")
    result = spark("apply.py", "--reset").get("CDC_RESULT", {})
    reset_pg_kafka()
    return result


def catchup(replay: bool = False) -> dict:
    guard("cdc-catchup")
    result = spark("apply.py", *(["--replay"] if replay else [])).get("CDC_RESULT")
    if result is None:
        raise CdcError("apply.py printed no CDC_RESULT line")
    doc = {
        "mode": "replay" if replay else "normal",
        "events_read": result["events_read"],
        "events": result.get("events", {}),
        "batches": result.get("batches"),
        "seconds": result.get("seconds"),
        "finished_at": now_iso(),
    }
    write_json("catchup.json", doc)
    return doc


def verify() -> dict:
    """Rows, columns and checksums per table: Postgres (host) vs Iceberg (Spark). Writes data/cdc/verify.json."""
    guard("cdc-verify")
    with pg_connect() as conn:
        columns = pg_columns(conn)
        pg = pg_side(conn, columns)
        table, column = SCHEMA_CHANGE
        pg_nonnull = None
        if column in columns[table]:
            pg_nonnull = conn.execute(f'SELECT count(*) FROM {table} WHERE "{column}" IS NOT NULL').fetchone()[0]
    arg = base64.urlsafe_b64encode(json.dumps(columns).encode("utf-8")).decode("ascii")
    ice = spark("iceberg_checksums.py", "--columns", arg, "--nonnull", f"{table}.{column}").get("CDC_ICEBERG")
    if ice is None:
        raise CdcError("iceberg_checksums.py printed no CDC_ICEBERG line")
    tables, ok = {}, True
    for t in TABLES:
        i = ice["tables"].get(t) or {}
        row = {
            **pg[t],
            "iceberg_rows": i.get("iceberg_rows"),
            "iceberg_checksum": i.get("iceberg_checksum"),
            "columns": columns[t],
            "iceberg_columns": i.get("iceberg_columns", []),
        }
        row["match"] = (
            row["pg_rows"] == row["iceberg_rows"]
            and row["pg_checksum"] == row["iceberg_checksum"]
            and sorted(columns[t]) == sorted(c for c in row["iceberg_columns"] if not c.startswith("_"))
        )
        ok = ok and row["match"]
        tables[t] = row
    schema_change = {
        "table": table,
        "column": column,
        "pg_nonnull": pg_nonnull,
        "iceberg_nonnull": ice.get("nonnull", {}).get(f"{table}.{column}"),
    }
    doc = {"ok": ok, "finished_at": now_iso(), "tables": tables, "schema_change": schema_change}
    write_json("verify.json", doc)
    for t, r in tables.items():
        print(
            f"verify {t:20} pg {r['pg_rows']:>5} iceberg {r['iceberg_rows']!s:>5} "
            f"{'match' if r['match'] else 'MISMATCH'} {r['pg_checksum'][:12]} / {str(r['iceberg_checksum'])[:12]}",
            flush=True,
        )
    print(f"verify: {'ok' if ok else 'FAILED'} (schema change {schema_change})", flush=True)
    return doc


# ---------------------------------------------------------------- end to end


class Steps:
    """Records name, start, seconds, running compose services and peak memory per container for each step."""

    def __init__(self, report: dict, timeout_s: float = E2E_TIMEOUT_S) -> None:
        self.report = report
        self.deadline = time.monotonic() + timeout_s

    @contextmanager
    def step(self, name: str):
        if time.monotonic() > self.deadline:
            raise CdcError(f"e2e over its {E2E_TIMEOUT_S // 60} min budget before step {name}")
        print(f"\n=== step {name} ===", flush=True)
        rec = {"name": name, "started_at": now_iso()}
        seen = set(running_services())
        sampler = MemorySampler()
        sampler.start()
        t = time.monotonic()
        try:
            yield rec
        finally:
            peak = sampler.stop()
            rec["seconds"] = round(time.monotonic() - t, 1)
            # services running at the start and end, plus any the sampler saw in between (e.g. cdc-apply)
            rec["containers_running"] = sorted(seen | set(running_services()) | set(peak))
            rec["peak_mem_mib"] = peak
            self.report["steps"].append(rec)
            print(f"=== step {name}: {rec['seconds']} s, peak MiB {peak} ===", flush=True)


def wait_for_kafka(expected: dict[str, dict[str, int]], timeout_s: float = 180) -> dict:
    deadline = time.monotonic() + timeout_s
    got: dict = {}
    while True:
        got = kafka_counts()
        if all(got[t].get(op, 0) == expected[t][op] for t in TABLES for op in ("c", "u", "d")):
            return got
        if time.monotonic() > deadline:
            print("expected (simulator):", json.dumps(expected), flush=True)
            print("in Kafka:            ", json.dumps(got), flush=True)
            raise CdcError(f"Kafka did not catch up with the simulator in {timeout_s:.0f} s")
        time.sleep(5)


def e2e(seed: int = 42, events: int = 1500, n_stations: int = 80) -> dict:
    """Bounded seeded run: reset, Connect up, simulate (column add halfway), Connect catch-up, stop, apply, verify."""
    from dockwatch.ops_sim import db
    from dockwatch.ops_sim.synthetic import SeededSimulation

    report: dict = {"seed": seed, "events": events, "ok": False, "started_at": now_iso(), "steps": []}
    steps = Steps(report)
    expected: dict = {}
    kafka: dict = {}
    calls: dict = {}
    try:
        with steps.step("preflight"):
            missing = [s for s in STREAM_SERVICES if not running(s)]
            if missing:
                raise CdcError(f"stream services not running: {missing} (python tasks.py up stream)")
            for busy in ("status-stream", "cdc-apply"):
                if running(busy):
                    raise CdcError(f"{busy} is running; stop it first")
            if running("connect"):
                print("stopping a leftover Kafka Connect", flush=True)
                connect_stop()
        with steps.step("reset-spark"):
            guard("cdc-reset")
            spark("apply.py", "--reset")
        with steps.step("reset-pg-kafka"):
            reset_pg_kafka()
        with steps.step("connect-up"):
            connect_up()
            report["connect_xmx"] = connect_heap()
        sim = SeededSimulation(seed=seed, events=events, n_stations=n_stations)
        with pg_connect() as conn:
            with steps.step("sim-a"):
                ex = db.run_seeded(conn, sim, range(0, sim.migrate_at), migrate=False)
            with steps.step("schema-change"):
                db.apply_migration(conn)
                sim.model.with_priority = True
            with steps.step("sim-b"):
                db.run_seeded(conn, sim, range(sim.migrate_at, events), ex, migrate=False)
        expected = ex.expected
        calls = dict(ex.calls.most_common())
        with steps.step("connect-catch-up"):
            kafka = wait_for_kafka(expected)
        with steps.step("connect-stop"):
            connect_stop()
        with steps.step("apply"):
            report["apply"] = catchup(replay=False)
        with steps.step("verify"):
            v = verify()
        tables = {}
        for t in TABLES:
            row = dict(v["tables"][t])
            row["expected_events"] = expected[t]
            row["kafka_events"] = kafka[t]
            tables[t] = row
        counts_ok = all(kafka[t].get(op, 0) == expected[t][op] for t in TABLES for op in ("c", "u", "d"))
        report |= {
            "tables": tables,
            "schema_change": v["schema_change"],
            "query_calls": calls,
            "ok": bool(v["ok"] and counts_ok),
        }
    except Exception as e:
        report["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        if running("connect"):
            connect_stop()
        report["finished_at"] = now_iso()
        path = write_json("e2e-report.json", report)
        print(f"\ne2e: ok={report['ok']} -> {path}", flush=True)
    return report


def exit_code(ok: bool) -> None:
    sys.exit(0 if ok else 1)
