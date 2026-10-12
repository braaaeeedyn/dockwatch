"""Task callables for the three M4a DAGs. They run inside the Airflow containers (Airflow, `trino`, `docker` and
`psycopg` are imported lazily); the rules live in the pure modules (guard, maintenance, evolution, slot, alerts).

Guard failures (a Spark JVM or Kafka Connect beside Trino, a lost slot) raise AirflowFailException: no retry, alert.
"""

import json
from datetime import UTC, datetime
from urllib.parse import urlparse

from dockwatch.batch import alerts, evolution, guard, maintenance, slot
from dockwatch.config import get_settings

PROJECT = "dockwatch"


# ---------------------------------------------------------------- environment


def trino_url() -> str:
    return get_settings().trino_url


def trino_execute():
    """A `sql -> list[dict]` function on one Trino connection (catalog lake). Every result is fetched, so DDL and
    table procedures have finished when it returns."""
    import trino

    u = urlparse(trino_url())
    conn = trino.dbapi.connect(
        host=u.hostname, port=u.port or 8080, user="dockwatch", catalog="lake", http_scheme=u.scheme or "http"
    )

    def execute(sql: str) -> list[dict]:
        print("trino>", sql.splitlines()[0][:200], flush=True)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description or []]
        return [dict(zip(cols, r, strict=False)) for r in rows]

    return execute


def docker_client():
    import docker

    return docker.from_env()


def running_services() -> set[str]:
    """Compose services of this project with a running container, through the mounted Docker socket."""
    boxes = docker_client().containers.list(filters={"label": f"com.docker.compose.project={PROJECT}"})
    return {b.labels.get("com.docker.compose.service", "") for b in boxes}


def fail(message: str):
    from airflow.exceptions import AirflowFailException

    raise AirflowFailException(message)


def check_guard(task: str, running: set[str] | None = None) -> None:
    up = running_services() if running is None else running
    reason = guard.refusal(task, lambda s: s in up)
    if reason:
        fail(reason)


def trino_guard(task: str) -> None:
    """Batch-on-Trino tasks: refuse beside Spark / Connect, and say so if Trino itself is not up."""
    up = running_services()
    check_guard(task, up)
    if "trino" not in up:
        fail(f"{task}: Trino is not running (python tasks.py batch-up)")


def conf_params(context: dict) -> dict:
    """DAG params merged with the run's conf (dag-test key=value pairs)."""
    params = dict(context.get("params") or {})
    dag_run = context.get("dag_run")
    params.update(getattr(dag_run, "conf", None) or {})
    return params


# ---------------------------------------------------------------- iceberg_maintenance


def maintenance_guard(**context) -> None:
    trino_guard("iceberg_maintenance")
    maintenance.params_of(conf_params(context))  # refuse bad retention before touching any table


def maintain_table(table: str, **context) -> dict:
    """optimize / expire / remove orphans on one table. The row (ok or error) is pushed as XCom `row` either way, so
    `record` writes it even when this task fails."""
    ti = context["ti"]
    run_id = context["dag_run"].run_id
    started = datetime.now(UTC)
    try:
        r = maintenance.maintain(trino_execute(), run_id, table, conf_params(context))
    except maintenance.MaintenanceError as e:
        r = getattr(e, "row", None) or maintenance.row(
            run_id, table, started, datetime.now(UTC), None, None, error=str(e)
        )
        ti.xcom_push(key="row", value=to_xcom(r))
        fail(str(e))  # a changed row count is not retried
    except Exception as e:
        r = maintenance.row(run_id, table, started, datetime.now(UTC), None, None, error=f"{type(e).__name__}: {e}")
        ti.xcom_push(key="row", value=to_xcom(r))
        raise
    ti.xcom_push(key="row", value=to_xcom(r))
    print(json.dumps(to_xcom(r)), flush=True)
    return to_xcom(r)


def record_maintenance(**context) -> None:
    """One INSERT of every table's row into lake.metrics.maintenance_runs; fails if any table failed or is missing."""
    ti = context["ti"]
    rows = []
    for i, table in enumerate(maintenance.TABLES):
        r = ti.xcom_pull(task_ids="maintain", key="row", map_indexes=i)
        if isinstance(r, list):  # some Airflow versions wrap single map-index pulls
            r = r[0] if r else None
        rows.append(
            from_xcom(r)
            if r
            else maintenance.row(
                context["dag_run"].run_id,
                table,
                datetime.now(UTC),
                datetime.now(UTC),
                None,
                None,
                error="no result (task did not run)",
            )
        )
    execute = trino_execute()
    for sql in maintenance.create_metrics_sql():
        execute(sql)
    execute(maintenance.insert_sql(rows))
    bad = [r["table_name"] for r in rows if r["status"] != "ok"]
    for r in rows:
        print(
            f"{r['table_name']:28} {r['status']:5} files {r['files_before']} -> {r['files_after']}, snapshots "
            f"{r['snapshots_before']} -> {r['snapshots_after']}, rows {r['rows_after']}",
            flush=True,
        )
    if bad:
        raise RuntimeError(f"maintenance failed for {', '.join(bad)} (see lake.metrics.maintenance_runs.error)")


def to_xcom(r: dict) -> dict:
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in r.items()}


def from_xcom(r: dict) -> dict:
    out = dict(r)
    for k in ("started_at", "finished_at"):
        if isinstance(out.get(k), str):
            out[k] = datetime.fromisoformat(out[k])
    return out


# ---------------------------------------------------------------- partition_evolution_demo


def evolution_guard(**context) -> None:
    trino_guard("partition_evolution_demo")


def evolution_prepare(**context) -> list[str]:
    execute = trino_execute()
    days = conf_params(context).get("days")
    days = evolution.check_days(days) if days else evolution.pick_days(execute(evolution.day_stats_sql()))
    print("days:", days, flush=True)
    for sql in evolution.create_sql():
        execute(sql)
    return days


def _load(day: str) -> dict:
    execute = trino_execute()
    for sql in evolution.hour_inserts(day):
        execute(sql)
    return evolution.summarize(execute(evolution.files_sql()))


def evolution_daily(**context) -> dict:
    return _load(context["ti"].xcom_pull(task_ids="prepare")[0])


def evolution_evolve(**context) -> None:
    trino_execute()(evolution.evolve_sql())


def evolution_hourly(**context) -> dict:
    return _load(context["ti"].xcom_pull(task_ids="prepare")[1])


def evolution_optimize(**context) -> dict:
    execute = trino_execute()
    execute(evolution.optimize_sql(conf_params(context).get("file_size_threshold", "100MB")))
    return evolution.summarize(execute(evolution.files_sql()))


def evolution_report(**context) -> str:
    ti = context["ti"]
    stages = {
        "daily_load": ti.xcom_pull(task_ids="daily_load"),
        "hourly_load": ti.xcom_pull(task_ids="hourly_load"),
        "optimized": ti.xcom_pull(task_ids="optimize"),
    }
    doc = evolution.report(ti.xcom_pull(task_ids="prepare"), stages)
    path = alerts.data_dir() / "maintenance" / "partition_evolution.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1) + "\n", encoding="utf-8")
    print(json.dumps(doc, indent=1), flush=True)
    return str(path)


# ---------------------------------------------------------------- cdc_slot_keepalive


def ops_dsn() -> str:
    return get_settings().ops_db_dsn


def connect_url() -> str:
    return get_settings().connect_url


def keepalive_inspect(**context) -> dict:
    st = slot.inspect_slot(ops_dsn())
    print(json.dumps(slot.to_dict(st)), flush=True)
    return slot.to_dict(st)


def keepalive(**context) -> dict:
    """Decide, catch up with the existing Connect container if needed, append one slot_health.jsonl line."""
    ti, run_id = context["ti"], context["dag_run"].run_id
    p = slot.params_of(conf_params(context))
    before = slot.from_dict(ti.xcom_pull(task_ids="inspect_slot"))
    d = slot.decide(
        before, slot.last_catchup(), datetime.now(UTC), p["catch_up_above_mb"], p["max_hours_between_catchups"]
    )
    print(f"decision: {d.action} ({d.reason})", flush=True)
    if d.action == "fail":
        slot.append_health(slot.health_line(run_id, "failed", d.reason, before, None, None))
        alerts.slot_unhealthy(ti.dag_id, ti.task_id, run_id, d.reason)
        fail(d.reason)
    if d.action == "skip":
        line = slot.health_line(run_id, "skipped", d.reason, before, None, None)
        slot.append_health(line)
        return line
    up = running_services()
    reason = guard.refusal("cdc_slot_keepalive", lambda s: s in up)
    if reason:
        slot.append_health(slot.health_line(run_id, "failed", reason, before, None, None))
        fail(reason)
    if "connect" in up:
        fail("Kafka Connect is already running; the keepalive only starts and stops it (python tasks.py connect-stop)")
    touched: dict[str, str] = {}

    def touch() -> str:
        touched["lsn"] = slot.touch_captured_row(ops_dsn())
        return touched["lsn"]

    try:
        after, seconds = slot.catch_up(
            docker_client(), lambda: slot.inspect_slot(ops_dsn()), connect_url(), timeout_s=p["timeout_s"], touch=touch
        )
    except Exception as e:
        slot.append_health(
            slot.health_line(
                run_id,
                "failed",
                f"{type(e).__name__}: {e}",
                before,
                slot.inspect_slot(ops_dsn()),
                None,
                touch_lsn=touched.get("lsn"),
            )
        )
        if isinstance(e, slot.KeepaliveError):
            fail(str(e))  # a hung or impossible catch-up is not retried (4 x timeout_s would pin Connect for 40 min)
        raise
    line = slot.health_line(run_id, "caught_up", d.reason, before, after, seconds, touch_lsn=touched.get("lsn"))
    slot.append_health(line)
    print(json.dumps(line), flush=True)
    safe = after.safe_wal_size
    if safe is not None and safe < p["alert_below_safe_mb"] * slot.MIB:
        alerts.slot_unhealthy(
            ti.dag_id,
            ti.task_id,
            run_id,
            f"safe_wal_size {safe / slot.MIB:.0f} MiB < {p['alert_below_safe_mb']} MiB after catch-up",
        )
    return line
