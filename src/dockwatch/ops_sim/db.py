"""psycopg executor: applies model `Op`s in short transactions (one per simulated minute).

The SQL lives in module constants (sql/ops/PLANS.md cites them). The executor counts calls per constant (the two
most-called are the "hot queries" PLANS.md explains) and the change events Debezium should emit per table and op,
taken from each statement's rowcount. Jobs and tickets are found the way an operator would: "the unfinished job at
this station" (OPEN_JOB_AT_STATION) and "the unfinished ticket on this dock" (OPEN_TICKET_ON_DOCK).
"""

import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from dockwatch.ops_sim.model import TABLES, Op

log = logging.getLogger("dockwatch.ops_sim")

ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = ROOT / "sql" / "ops"

INSERT_STATION = """INSERT INTO stations (station_id, name, short_name, lat, lon, capacity, region_id, is_active)
VALUES (%(station_id)s, %(name)s, %(short_name)s, %(lat)s, %(lon)s, %(capacity)s, %(region_id)s, %(is_active)s)"""

UPSERT_STATION = """INSERT INTO stations (station_id, name, short_name, lat, lon, capacity, region_id, is_active)
VALUES (%(station_id)s, %(name)s, %(short_name)s, %(lat)s, %(lon)s, %(capacity)s, %(region_id)s, %(is_active)s)
ON CONFLICT (station_id) DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name, lat = EXCLUDED.lat,
    lon = EXCLUDED.lon, capacity = EXCLUDED.capacity, region_id = EXCLUDED.region_id
WHERE (stations.name, stations.short_name, stations.lat, stations.lon, stations.capacity, stations.region_id)
    IS DISTINCT FROM (EXCLUDED.name, EXCLUDED.short_name, EXCLUDED.lat, EXCLUDED.lon, EXCLUDED.capacity,
    EXCLUDED.region_id)
RETURNING (xmax = 0) AS inserted"""

SET_STATION_ACTIVE = "UPDATE stations SET is_active = %(is_active)s WHERE station_id = %(station_id)s"

INSERT_DOCKS = """INSERT INTO docks (station_id, dock_number)
SELECT %(station_id)s, n FROM generate_series(1, %(count)s) AS n
ON CONFLICT (station_id, dock_number) DO NOTHING"""

SET_DOCK_STATUS = """UPDATE docks SET status = %(status)s
WHERE station_id = %(station_id)s AND dock_number = %(dock_number)s"""

INSERT_VAN = """INSERT INTO vans (plate, capacity, status) VALUES (%(plate)s, %(capacity)s, %(status)s)
ON CONFLICT (plate) DO NOTHING"""

SET_VAN_STATUS = "UPDATE vans SET status = %(status)s WHERE plate = %(plate)s"

# Hot query 1 (index rebalancing_jobs_open_station_idx): before every job insert (one unfinished job per station)
# and to find the job every job update applies to.
OPEN_JOB_AT_STATION = """SELECT job_id, status FROM rebalancing_jobs
WHERE station_id = %(station_id)s AND status IN ('open', 'assigned')"""

INSERT_JOB = """INSERT INTO rebalancing_jobs (station_id, van_id, status, alert_episode_id, opened_at)
VALUES (%(station_id)s, (SELECT van_id FROM vans WHERE plate = %(plate)s), %(status)s, %(alert_episode_id)s,
    %(opened_at)s)"""

INSERT_JOB_WITH_PRIORITY = """INSERT INTO rebalancing_jobs (station_id, van_id, status, alert_episode_id, opened_at,
    priority)
VALUES (%(station_id)s, (SELECT van_id FROM vans WHERE plate = %(plate)s), %(status)s, %(alert_episode_id)s,
    %(opened_at)s, %(priority)s)"""

UPDATE_JOB = """UPDATE rebalancing_jobs SET status = %(status)s,
    van_id = (SELECT van_id FROM vans WHERE plate = %(plate)s), closed_at = %(closed_at)s,
    bikes_moved = %(bikes_moved)s
WHERE job_id = %(job_id)s"""

UPDATE_JOB_WITH_PRIORITY = """UPDATE rebalancing_jobs SET status = %(status)s,
    van_id = (SELECT van_id FROM vans WHERE plate = %(plate)s), closed_at = %(closed_at)s,
    bikes_moved = %(bikes_moved)s, priority = %(priority)s
WHERE job_id = %(job_id)s"""

DELETE_JOB = """DELETE FROM rebalancing_jobs
WHERE alert_episode_id = %(alert_episode_id)s AND status = 'cancelled'"""

# Hot query 2 (index maintenance_tickets_open_dock_idx): before every ticket insert (one unfinished ticket per dock)
# and to find the ticket every ticket update applies to.
OPEN_TICKET_ON_DOCK = """SELECT ticket_id, status FROM maintenance_tickets
WHERE dock_id = (SELECT dock_id FROM docks WHERE station_id = %(station_id)s AND dock_number = %(dock_number)s)
    AND status <> 'closed'"""

INSERT_TICKET = """INSERT INTO maintenance_tickets (dock_id, issue, status, opened_at)
VALUES ((SELECT dock_id FROM docks WHERE station_id = %(station_id)s AND dock_number = %(dock_number)s), %(issue)s,
    'open', %(opened_at)s)"""

UPDATE_TICKET = """UPDATE maintenance_tickets SET status = %(status)s, closed_at = %(closed_at)s
WHERE ticket_id = %(ticket_id)s"""

SQL_VERBS = ("SELECT", "INSERT", "UPDATE", "DELETE")
QUERIES = {n: q for n, q in globals().items() if n.isupper() and isinstance(q, str) and q.startswith(SQL_VERBS)}


class Conflict(RuntimeError):
    """The database disagrees with the model (a row that should exist doesn't, or vice versa)."""


def to_ts(value):
    return datetime.fromtimestamp(value, UTC) if isinstance(value, int) else value


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True, connect_timeout=10)


def schema_files(base_only: bool = False) -> list[Path]:
    files = [SQL_DIR / "schema.sql"]
    if not base_only:
        files += sorted((SQL_DIR / "migrations").glob("*.sql"))
    return files


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    with conn.transaction():
        conn.execute(path.read_text(encoding="utf-8"))


def apply_schema(conn: psycopg.Connection, base_only: bool = False) -> list[str]:
    applied = []
    for path in schema_files(base_only):
        apply_sql_file(conn, path)
        applied.append(path.relative_to(ROOT).as_posix())
    return applied


def apply_migration(conn: psycopg.Connection, name: str = "001_rebalancing_jobs_priority.sql") -> None:
    apply_sql_file(conn, SQL_DIR / "migrations" / name)


class Executor:
    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        self.calls: Counter[str] = Counter()
        self.expected = {t: {"c": 0, "u": 0, "d": 0} for t in TABLES}

    def _run(self, name: str, params: dict) -> psycopg.Cursor:
        self.calls[name] += 1
        return self.conn.execute(QUERIES[name], {k: to_ts(v) if k.endswith("_at") else v for k, v in params.items()})

    def _write(self, name: str, params: dict, table: str, kind: str, want: int | None = 1) -> int:
        n = self._run(name, params).rowcount
        if want is not None and n != want:
            raise Conflict(f"{name} changed {n} rows, expected {want}: {params}")
        self.expected[table][kind] += n
        return n

    def _find(self, name: str, params: dict) -> int | None:
        rows = self._run(name, params).fetchall()
        if len(rows) > 1:
            raise Conflict(f"{name} found {len(rows)} unfinished rows: {params}")
        return rows[0][0] if rows else None

    def apply(self, ops: list[Op]) -> None:
        """One transaction for the ops of one simulated minute."""
        if not ops:
            return
        with self.conn.transaction():
            for op in ops:
                self.apply_one(op)

    def apply_one(self, op: Op) -> None:
        v, a = op.values, op.action
        if a == "insert_station":
            self._write("INSERT_STATION", v, "stations", "c")
        elif a == "upsert_station":
            row = self._run("UPSERT_STATION", v).fetchone()
            if row is not None:
                self.expected["stations"]["c" if row[0] else "u"] += 1
        elif a == "update_station":
            self._write("SET_STATION_ACTIVE", v, "stations", "u")
        elif a == "insert_docks":
            self._write("INSERT_DOCKS", v, "docks", "c", want=None)
        elif a == "dock_status":
            self._write("SET_DOCK_STATUS", v, "docks", "u")
        elif a == "insert_van":
            self._write("INSERT_VAN", v, "vans", "c", want=None)
        elif a == "van_status":
            self._write("SET_VAN_STATUS", v, "vans", "u")
        elif a == "open_job":
            if self._find("OPEN_JOB_AT_STATION", v) is not None:
                raise Conflict(f"station {v['station_id']} already has an unfinished job")
            self._write("INSERT_JOB_WITH_PRIORITY" if "priority" in v else "INSERT_JOB", v, "rebalancing_jobs", "c")
        elif a == "update_job":
            job_id = self._find("OPEN_JOB_AT_STATION", v)
            if job_id is None:
                raise Conflict(f"station {v['station_id']} has no unfinished job to update")
            name = "UPDATE_JOB_WITH_PRIORITY" if "priority" in v else "UPDATE_JOB"
            self._write(name, {**v, "job_id": job_id}, "rebalancing_jobs", "u")
        elif a == "delete_job":
            self._write("DELETE_JOB", v, "rebalancing_jobs", "d")
        elif a == "open_ticket":
            if self._find("OPEN_TICKET_ON_DOCK", v) is not None:
                raise Conflict(f"dock {v['station_id']}/{v['dock_number']} already has an unfinished ticket")
            self._write("INSERT_TICKET", v, "maintenance_tickets", "c")
        elif a == "update_ticket":
            ticket_id = self._find("OPEN_TICKET_ON_DOCK", v)
            if ticket_id is None:
                raise Conflict(f"dock {v['station_id']}/{v['dock_number']} has no unfinished ticket")
            self._write("UPDATE_TICKET", {**v, "ticket_id": ticket_id}, "maintenance_tickets", "u")
        else:
            raise ValueError(f"unknown action {a!r}")


def table_counts(conn: psycopg.Connection) -> dict[str, int]:
    return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in TABLES}


def run_seeded(
    conn: psycopg.Connection, sim, ticks: range, executor: Executor | None = None, migrate: bool = True
) -> Executor:
    """Apply the setup ops (when ticks starts at 0) and the ops of `ticks`; migration 001 runs at sim.migrate_at."""
    ex = executor or Executor(conn)
    if ticks.start == 0:
        ex.apply(sim.setup_ops())
    for tick in ticks:
        if migrate and tick == sim.migrate_at:
            apply_migration(conn)
            sim.model.with_priority = True
        ex.apply(sim.ops_for(tick))
    return ex
