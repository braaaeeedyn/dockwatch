"""iceberg_maintenance on the M4b / metrics tables too (Loop K): 9 core + 7 batch tables; a batch table that does not
exist yet is `skipped` without failing the run, a missing core table still fails."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from dockwatch.batch import dag_tasks, maintenance

ROOT = Path(__file__).resolve().parents[1]
RUN = "manual__2026-10-15T12:00:00+00:00"


class FakeTrino:
    """Trino for maintenance: `existing` tables answer the existence probe and measure_sql; any statement on another
    table raises like Trino does ("Table ... does not exist")."""

    def __init__(self, existing: set[str], busiest: list[dict] | None = None) -> None:
        self.existing = existing
        self.busiest = busiest or []  # answer to busiest_partition_sql
        self.sql: list[str] = []

    def __call__(self, sql: str) -> list[dict]:
        self.sql.append(sql)
        if "$partitions" in sql and "file_count > 1" in sql:
            return self.busiest
        if "information_schema.tables" in sql:
            ns, name = sql.split("table_schema = '")[1].split("'")[0], sql.split("table_name = '")[1].split("'")[0]
            return [{"n": int(f"{ns}.{name}" in self.existing)}]
        on = (t for t in maintenance.ALL_TABLES if maintenance.meta(t, "files") in sql or f"lake.{t} " in sql)
        table = next(on, None)
        if table not in self.existing:
            raise RuntimeError(f"TrinoUserError: Table 'lake.{table}' does not exist")
        if sql.startswith("SELECT (SELECT count(*)"):
            return [{"files": 4, "bytes": 1000, "rows": 50, "snapshots": 3, "delete_files": 0, "partitions": 1}]
        return []


def clock():
    t = [datetime(2026, 10, 15, 12, 0, tzinfo=UTC)]

    def now():
        t[0] += timedelta(seconds=5)
        return t[0]

    return now


def test_maintenance_also_covers_the_m4b_and_metrics_tables():
    assert maintenance.BATCH_TABLES == (
        "bronze.trips",
        "bronze.station_information",
        "metrics.trip_loads",
        "marts.mart_station_availability_daily",
        "marts.mart_rebalancing_need",
        "marts.mart_trips_vs_availability",
        "metrics.pipeline_minutely",
    )
    assert maintenance.ALL_TABLES == maintenance.TABLES + maintenance.BATCH_TABLES
    assert len(maintenance.ALL_TABLES) == len(set(maintenance.ALL_TABLES)) == 16
    assert "metrics.maintenance_runs" not in maintenance.ALL_TABLES  # written by the run itself, after measuring
    for t in maintenance.ALL_TABLES:
        assert maintenance.qualified(t) == f"lake.{t}"
    dag = (ROOT / "airflow/dags/iceberg_maintenance.py").read_text(encoding="utf-8")
    assert '[{"table": t} for t in maintenance.ALL_TABLES]' in dag


def test_missing_batch_table_is_skipped_but_a_missing_core_table_fails():
    trino = FakeTrino({"bronze.trips"})
    r = maintenance.maintain(trino, RUN, "metrics.pipeline_minutely", now=clock())
    assert r["status"] == "skipped"
    assert r["error"] is None
    assert (r["files_before"], r["rows_after"]) == (None, None)
    assert set(r) == {n for n, _ in maintenance.METRICS_COLUMNS}
    assert not any("EXECUTE" in s for s in trino.sql)  # nothing is run on a table that does not exist
    assert "'skipped'" in maintenance.insert_sql([r])

    trino.busiest = [{"value": "2026-09-01", "file_count": 7}]
    ok = maintenance.maintain(trino, RUN, "bronze.trips", now=clock())
    assert ok["status"] == "ok"
    assert (ok["rows_before"], ok["rows_after"]) == (50, 50)
    assert [s.split(" EXECUTE ")[-1].split("(")[0] for s in trino.sql if "lake.bronze.trips EXECUTE" in s] == [
        "optimize",
        "expire_snapshots",
        "remove_orphan_files",
    ]

    core = FakeTrino(set())
    with pytest.raises(RuntimeError, match="does not exist"):
        maintenance.maintain(core, RUN, "silver.station_status", now=clock())
    assert not any("information_schema" in s for s in core.sql)  # core tables are not probed: missing = failure


def test_identity_partitioned_table_is_optimized_one_partition_per_run():
    assert maintenance.IDENTITY_PARTITIONS == {"bronze.trips": "trip_month"}
    trino = FakeTrino({"bronze.trips"}, busiest=[{"value": "2025-10-01", "file_count": 5}])
    maintenance.maintain(trino, RUN, "bronze.trips", now=clock())
    optimize = [s for s in trino.sql if "EXECUTE optimize" in s]
    assert optimize == [
        "ALTER TABLE lake.bronze.trips EXECUTE optimize(file_size_threshold => '100MB') "
        "WHERE trip_month = DATE '2025-10-01'"
    ]  # one partition: one `replace` commit, one month in Trino's writers
    assert "ORDER BY file_count DESC" in maintenance.busiest_partition_sql("bronze.trips")

    compacted = FakeTrino({"bronze.trips"})  # every partition already has one file
    r = maintenance.maintain(compacted, RUN, "bronze.trips", now=clock())
    assert r["status"] == "ok"
    assert not any("EXECUTE optimize" in s for s in compacted.sql)
    assert any("EXECUTE expire_snapshots" in s for s in compacted.sql)

    bad = FakeTrino({"bronze.trips"}, busiest=[{"value": "2025-10-01' OR true --", "file_count": 5}])
    with pytest.raises(maintenance.MaintenanceError, match="unexpected partition value"):
        maintenance.maintain(bad, RUN, "bronze.trips", now=clock())


class TI:
    def __init__(self, rows: dict[str, dict | None]) -> None:
        self.rows = rows

    def xcom_pull(self, task_ids, key, map_indexes):
        assert (task_ids, key) == ("maintain", "row")
        r = self.rows[maintenance.ALL_TABLES[map_indexes]]
        return [r] if r else None  # some Airflow versions wrap single map-index pulls


class Run:
    run_id = RUN


def rows(statuses: dict[str, str]) -> dict[str, dict | None]:
    now = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    out = {}
    for t in maintenance.ALL_TABLES:
        status = statuses.get(t, "ok")
        if status == "missing":
            out[t] = None
            continue
        r = maintenance.row(RUN, t, now, now, None, None, error="boom" if status == "error" else None)
        out[t] = dag_tasks.to_xcom({**r, "status": status})
    return out


def test_skipped_tables_do_not_fail_the_run(monkeypatch):
    executed: list[str] = []
    monkeypatch.setattr(dag_tasks, "trino_execute", lambda: lambda sql: executed.append(sql) or [])
    skipped = {"metrics.pipeline_minutely": "skipped", "bronze.trips": "skipped"}
    dag_tasks.record_maintenance(ti=TI(rows(skipped)), dag_run=Run())
    insert = executed[-1]
    assert insert.startswith("INSERT INTO lake.metrics.maintenance_runs")
    assert insert.count(f"'{RUN}'") == 16  # one row per table, in one commit
    assert insert.count("'skipped'") == 2

    executed.clear()
    with pytest.raises(RuntimeError, match="maintenance failed for ops.vans"):
        dag_tasks.record_maintenance(ti=TI(rows({**skipped, "ops.vans": "error"})), dag_run=Run())
    assert executed[-1].count("'error'") == 1  # the failed run is still recorded

    with pytest.raises(RuntimeError, match="marts.mart_rebalancing_need"):
        dag_tasks.record_maintenance(ti=TI(rows({"marts.mart_rebalancing_need": "missing"})), dag_run=Run())
