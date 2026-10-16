"""Task callables for the three M4a DAGs, the two M4b DAGs and pipeline_health (M5, Loop K). They run inside the
Airflow containers (Airflow, `trino`, `docker`, `psycopg`, pyarrow and pyiceberg are imported lazily); the rules live in
the pure modules (guard, maintenance, evolution, slot, alerts, trips, marts, health).

Guard failures (a Spark JVM or Kafka Connect beside Trino, a lost slot) raise AirflowFailException: no retry, alert.
"""

import json
from datetime import UTC, datetime
from urllib.parse import urlparse

from dockwatch.batch import alerts, evolution, guard, health, maintenance, marts, slot, trips
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
    """One INSERT of every table's row into lake.metrics.maintenance_runs (all 16 tables); fails if any table failed
    or has no row. A `skipped` batch table (not created yet) does not fail the run."""
    ti = context["ti"]
    rows = []
    for i, table in enumerate(maintenance.ALL_TABLES):
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
    bad = [r["table_name"] for r in rows if r["status"] not in ("ok", "skipped")]
    for r in rows:
        print(
            f"{r['table_name']:38} {r['status']:7} files {r['files_before']} -> {r['files_after']}, snapshots "
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


# ---------------------------------------------------------------- trip_history_monthly (M4b)


def trips_month(context: dict) -> str:
    """The `month` param (dag-test month=YYYY-MM) if set, else the month of the run's data interval."""
    try:
        return trips.month_of(conf_params(context).get("month"), context["data_interval_start"])
    except trips.TripError as e:
        fail(str(e))


def trips_zips():
    return alerts.data_dir() / "trips" / "zips"


def trips_guard(**context) -> None:
    trino_guard("trip_history_monthly")
    print("month:", trips_month(context), flush=True)


def trips_published(**context) -> bool:
    """PythonSensor: True once the month's ZIP is in the bucket. A month that is absent while a later one exists
    (2020-04) is recorded as `missing` in lake.metrics.trip_loads and skipped; otherwise poke again later."""
    month = trips_month(context)
    status = trips.month_status(trips.fetch_listing(), month)
    print(f"{month}: {status}", flush=True)
    if status == "missing":
        from airflow.exceptions import AirflowSkipException

        execute = trino_execute()
        for sql in trips.create_sql():
            execute(sql)
        now = datetime.now(UTC)
        row = {
            "run_id": context["dag_run"].run_id,
            "trip_month": trips.first_day(month),
            "status": "missing",
            "error": f"{month} is not in the bucket while later months are",
            "started_at": now,
            "finished_at": now,
        }
        execute(trips.insert_load_sql(row))  # fmt: skip
        raise AirflowSkipException(f"{month}: not published (later months are)")
    return status == "published"


def trips_download(**context) -> dict:
    month = trips_month(context)
    listing = trips.fetch_listing()
    key = trips.month_key(listing, month)
    if not key:
        fail(f"{month}: no ZIP in the bucket listing")
    try:
        path = trips.download(key, listing[key], trips_zips())
    except trips.TripError as e:
        fail(str(e))
    print(f"{month}: {path} ({listing[key]} bytes)", flush=True)
    return {"month": month, "key": key}


def trips_load(**context) -> dict:
    """Replace the month in bronze.trips (pyiceberg, one commit), count it back, record trip_loads. Bad data
    (header, timestamps, counts) fails without retry."""
    got = context["ti"].xcom_pull(task_ids="download")
    month, key = got["month"], got["key"]
    try:
        row = trips.load_month(trino_execute(), context["dag_run"].run_id, month, key, trips_zips() / key)
    except trips.TripError as e:
        fail(str(e))
    out = {k: row.get(k) for k in ("status", "schema_version", "csv_rows", "loaded_rows", "bad_timestamps")}
    print(json.dumps({"month": month, **out}), flush=True)
    return out


# ---------------------------------------------------------------- marts_daily (M4b)


def marts_guard(**context) -> None:
    trino_guard("marts_daily")


def marts_stations(**context) -> dict:
    """lake.bronze.station_information from the newest GBFS station_information snapshot in the raw archive, replaced
    with one pyiceberg overwrite (Trino only creates the table and counts it back)."""
    from dockwatch.producer.archive import build_archive

    archive = build_archive(get_settings())
    keys = archive.list_keys("raw/gbfs/station_information/")
    if not keys:
        fail("no station_information snapshot in raw/gbfs/station_information/")
    rows = marts.station_rows(json.loads(archive.get(keys[-1])), datetime.now(UTC))
    if not rows:
        fail(f"{keys[-1]}: no stations")
    execute = trino_execute()
    for sql in marts.create_station_sql():
        execute(sql)
    trips.overwrite(marts.STATION_TABLE, trips.rows_table(marts.STATION_COLUMNS, rows))
    n = int(execute(f"SELECT count(*) AS n FROM {marts.STATION_TABLE}")[0]["n"])
    if n != len(rows):
        raise RuntimeError(f"{marts.STATION_TABLE}: {n} rows after the overwrite, wrote {len(rows)}")
    out = {"source": keys[-1], "stations": n}
    print(json.dumps(out), flush=True)
    return out


DBT = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT = "/opt/dockwatch/dbt/dockwatch"


def dbt_dir():
    return alerts.data_dir() / "dbt"


def insights_dir():
    """web/data/insights (/opt/dockwatch/web/data/insights in the Airflow containers)."""
    return alerts.data_dir().parent / "web" / "data" / "insights"


def run_results() -> dict:
    p = dbt_dir() / "target" / "run_results.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.is_file() else {}


def marts_dbt_build(**context) -> dict:
    """dbt build (models + tests, threads 1). A failed test fails without retry; anything else retries."""
    import subprocess

    out = dbt_dir()
    cmd = [DBT, "build", "--project-dir", DBT_PROJECT, "--profiles-dir", DBT_PROJECT, "--target", "local",
           "--target-path", str(out / "target"), "--log-path", str(out / "logs")]  # fmt: skip
    print("+", " ".join(cmd), flush=True)
    code = subprocess.run(cmd, timeout=1800).returncode
    rr = run_results()
    bad = [(r.get("unique_id"), r.get("status")) for r in rr.get("results") or [] if r.get("status") in ("fail",)]
    if bad:
        fail(f"dbt tests failed: {bad}")
    if code:
        raise RuntimeError(f"dbt build exited {code} (see {out / 'logs' / 'dbt.log'})")
    return {"elapsed_s": rr.get("elapsed_time"), "nodes": len(rr.get("results") or [])}


def marts_quality_gate(**context) -> dict:
    """data/dbt/quality_gate.json from run_results.json, the marts' row counts and freshness against silver."""
    execute = trino_execute()
    rows = {m: int(execute(f"SELECT count(*) AS n FROM lake.marts.{m}")[0]["n"]) for m in marts.MARTS}
    fresh = execute(
        "SELECT (SELECT max(service_date) FROM lake.marts.mart_station_availability_daily) AS d, "
        "(SELECT CAST(max(window_start) AT TIME ZONE 'America/Los_Angeles' AS date) FROM lake.silver.availability_5m)"
        " AS s"
    )[0]
    doc = marts.quality_gate(run_results(), rows, fresh["d"], fresh["s"])
    doc["dag_run_id"] = context["dag_run"].run_id
    path = dbt_dir() / "quality_gate.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    marts.write_atomic(path, json.dumps(doc, indent=1) + "\n")
    print(json.dumps(doc, indent=1), flush=True)
    if not doc["passed"]:
        fail(f"quality gate failed: {doc['problems']}")
    return {k: doc[k] for k in ("passed", "tests", "warnings")}


def marts_export(**context) -> dict:
    """web/data/insights/*.json (format 2: station lookup + window header) from the marts (only after the gate
    passed), manifest.json last."""
    execute = trino_execute()
    tables = {}
    for name, (mart, order) in marts.INSIGHTS.items():
        where = ""
        if name == "station_availability_daily":
            where = (f" WHERE service_date > (SELECT max(service_date) FROM lake.marts.{mart}) - INTERVAL "
                     f"'{marts.DAILY_DATES}' DAY")  # fmt: skip
        result = execute(f"SELECT * FROM lake.marts.{mart}{where} ORDER BY {order}")
        cols = list(result[0]) if result else []
        tables[name] = (cols, [[r[c] for c in cols] for r in result])
    gate = json.loads((dbt_dir() / "quality_gate.json").read_text(encoding="utf-8"))
    dcols, daily = tables["station_availability_daily"]
    days = sorted({r[dcols.index("service_date")] for r in daily})
    need = execute("SELECT min(window_first_day) AS a, max(window_last_day) AS b FROM lake.marts.mart_rebalancing_need")
    trips_w = execute("SELECT min(trip_first_month) AS a, max(trip_last_month) AS b FROM "
                      "lake.marts.mart_trips_vs_availability")  # fmt: skip
    manifest = {
        "format": marts.INSIGHTS_FORMAT,
        "dag_run_id": context["dag_run"].run_id,
        "quality_gate": {k: gate.get(k) for k in ("passed", "tests", "warnings")},
        "windows": {
            "daily_service_dates": [marts.plain(days[0]), marts.plain(days[-1])] if days else None,
            "rebalancing_days": [marts.plain(need[0]["a"]), marts.plain(need[0]["b"])],
            "trip_months": [marts.plain(trips_w[0]["a"]), marts.plain(trips_w[0]["b"])],
        },
        "source": "lake.marts (dbt on Trino; silver live data and bronze.trips Bay Wheels trip history)",
    }
    try:
        v2 = {name: marts.insights_v2(name, *table) for name, table in tables.items()}
        man = marts.export_insights(insights_dir(), v2, manifest)
    except marts.GateError as e:
        fail(str(e))
    print(json.dumps(man["files"]), flush=True)
    return man["files"]


# ---------------------------------------------------------------- pipeline_health (M5, Loop K)


def health_guard(**context) -> None:
    trino_guard("pipeline_health")


def health_window(context: dict):
    """The 1,440 minutes before the run's start minute."""
    return health.window(getattr(context.get("dag_run"), "start_date", None) or datetime.now(UTC))


def health_minutely(**context) -> dict:
    """lake.metrics.pipeline_minutely for the run's window: four GROUP BYs on silver through Trino, the series in
    Python, one pyiceberg transaction (delete the window and anything later, append), counted back through Trino."""
    execute = trino_execute()
    w0, w1 = health_window(context)
    rows = health.minutely_rows(
        w0,
        execute(health.events_sql(w0, w1)),
        execute(health.processed_sql(w0, w1)),
        execute(health.lag_sql(w0, w1, "kafka_ts")),
        execute(health.lag_sql(w0, w1, "greatest(kafka_ts, processed_ts)")),
    )
    for sql in health.create_sql():
        execute(sql)
    health.replace_window(w0, health.minutely_arrow(rows, context["dag_run"].run_id, datetime.now(UTC)))
    n = int(execute(health.count_sql(w0, w1))[0]["n"])
    if n != health.MINUTES:
        raise RuntimeError(f"{health.MINUTELY_TABLE}: {n} rows in the window after the commit, wrote {health.MINUTES}")
    out = {"window": [w0.isoformat(), w1.isoformat()], "rows": n, "events": sum(r[1] for r in rows),
           "processed": sum(r[2] for r in rows)}  # fmt: skip
    print(json.dumps(out), flush=True)
    return out


def health_path():
    """web/data/health.json (/opt/dockwatch/web/data/health.json in the Airflow containers)."""
    return alerts.data_dir().parent / "web" / "data" / "health.json"


def health_export(**context) -> dict:
    """web/data/health.json: the window read back from pipeline_minutely, silver's freshness, every existing
    maintenance table's newest commit / files / partitions, the quality gate and dbt tests, the newest maintenance
    run."""
    execute = trino_execute()
    w0, w1 = health_window(context)
    rows = health.table_rows(execute(health.window_sql(w0, w1)))
    last = execute(f"SELECT max(processed_ts) AS t FROM {health.SILVER}")[0]["t"]
    existing = {r["t"] for r in execute(health.EXISTING_SQL)}
    tables = [{"table": t, **execute(health.table_sql(t))[0]} for t in maintenance.ALL_TABLES if t in existing]
    run = execute(health.NEWEST_RUN_SQL) if "metrics.maintenance_runs" in existing else []
    gate_path = dbt_dir() / "quality_gate.json"
    gate = json.loads(gate_path.read_text(encoding="utf-8")) if gate_path.is_file() else None
    doc = health.health_doc(datetime.now(UTC), context["dag_run"].run_id, rows, last, tables, gate,
                            health.dbt_tests(run_results()), run)  # fmt: skip
    try:
        size = health.write_health(health_path(), doc)
    except health.HealthError as e:
        fail(str(e))
    out = {"bytes": size, "status": doc["status"]["overall"], "tiles": doc["tiles"]}
    print(json.dumps(out), flush=True)
    return out
