"""The ops schema in the running ops-db (python tasks.py up stream; python tasks.py ops-schema).

Every test runs inside one transaction that is rolled back at the end: rolled-back transactions never reach the WAL
stream Debezium reads, so these tests emit no CDC events and leave no rows behind.
"""

import os

import psycopg
import pytest
from psycopg import errors

from dockwatch.cdc import runner
from dockwatch.cdc.guard import slot_refusal
from dockwatch.config import get_settings

pytestmark = pytest.mark.integration

TABLES = {"stations", "docks", "vans", "rebalancing_jobs", "maintenance_tickets"}


@pytest.fixture
def conn():
    c = psycopg.connect(get_settings().ops_db_dsn, connect_timeout=10)
    try:
        yield c
    finally:
        c.rollback()
        c.close()


def _station(conn, sid="test-0001"):
    conn.execute(
        "INSERT INTO stations (station_id, name, lat, lon, capacity) VALUES (%s, 'Test', 37.77, -122.42, 10)", (sid,)
    )
    return sid


def _rejects(conn, sql, params, error=errors.CheckViolation):
    with pytest.raises(error), conn.transaction():  # a savepoint, so the outer transaction survives
        conn.execute(sql, params)


def test_five_tables_with_foreign_keys(conn):
    tables = {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}
    assert tables >= TABLES
    fks = set(
        conn.execute(
            """SELECT tc.table_name, kcu.column_name, ccu.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu USING (constraint_schema, constraint_name)
            JOIN information_schema.constraint_column_usage ccu USING (constraint_schema, constraint_name)
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'"""
        ).fetchall()
    )
    assert fks == {
        ("docks", "station_id", "stations"),
        ("rebalancing_jobs", "station_id", "stations"),
        ("rebalancing_jobs", "van_id", "vans"),
        ("maintenance_tickets", "dock_id", "docks"),
    }
    _rejects(
        conn,
        "INSERT INTO docks (station_id, dock_number) VALUES (%s, 1)",
        ("no-such-station",),
        errors.ForeignKeyViolation,
    )
    _rejects(
        conn,
        "INSERT INTO maintenance_tickets (dock_id, issue, opened_at) VALUES (%s, 'jammed', now())",
        (-1,),
        errors.ForeignKeyViolation,
    )


def test_check_constraints_reject_bad_rows(conn):
    sid = _station(conn)
    station = "INSERT INTO stations (station_id, name, lat, lon, capacity) VALUES (%s, 'x', %s, %s, %s)"
    _rejects(conn, station, ("bad-lat", 91.0, -122.4, 10))
    _rejects(conn, station, ("bad-lon", 37.7, -181.0, 10))
    _rejects(conn, station, ("bad-cap", 37.7, -122.4, -1))
    _rejects(conn, "INSERT INTO docks (station_id, dock_number) VALUES (%s, 0)", (sid,))
    _rejects(conn, "INSERT INTO docks (station_id, dock_number, status) VALUES (%s, 1, 'gone')", (sid,))
    _rejects(conn, "INSERT INTO vans (plate, capacity, status) VALUES ('T-1', 20, 'flying')", ())
    _rejects(conn, "INSERT INTO vans (plate, capacity) VALUES ('T-2', 0)", ())
    job = (
        "INSERT INTO rebalancing_jobs (station_id, status, opened_at, closed_at, bikes_moved) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    _rejects(conn, job, (sid, "bogus", "2026-10-01T10:00Z", None, None))
    _rejects(conn, job, (sid, "done", "2026-10-01T10:00Z", "2026-10-01T09:00Z", 3))  # closed before opened
    _rejects(conn, job, (sid, "done", "2026-10-01T10:00Z", None, 3))  # done but never closed
    _rejects(conn, job, (sid, "open", "2026-10-01T10:00Z", "2026-10-01T11:00Z", None))  # open but closed
    _rejects(conn, job, (sid, "assigned", "2026-10-01T10:00Z", None, None))  # assigned without a van
    _rejects(conn, job, (sid, "done", "2026-10-01T10:00Z", "2026-10-01T11:00Z", -1))
    conn.execute(job, (sid, "done", "2026-10-01T10:00Z", "2026-10-01T11:00Z", 4))  # a good row passes
    conn.execute("INSERT INTO docks (station_id, dock_number) VALUES (%s, 1)", (sid,))
    dock = conn.execute("SELECT dock_id FROM docks WHERE station_id = %s", (sid,)).fetchone()[0]
    ticket = (
        "INSERT INTO maintenance_tickets (dock_id, issue, status, opened_at, closed_at) VALUES (%s, %s, %s, %s, %s)"
    )
    _rejects(conn, ticket, (dock, "on_fire", "open", "2026-10-01T10:00Z", None))
    _rejects(conn, ticket, (dock, "jammed", "closed", "2026-10-01T10:00Z", None))
    _rejects(conn, ticket, (dock, "jammed", "closed", "2026-10-01T10:00Z", "2026-10-01T09:59Z"))


def test_updated_at_trigger_sets_now_on_update(conn):
    conn.execute(
        "INSERT INTO stations (station_id, name, lat, lon, capacity, created_at, updated_at) "
        "VALUES ('test-0002', 'Old', 37.77, -122.42, 10, '2020-01-01T00:00Z', '2020-01-01T00:00Z')"
    )
    old = conn.execute("SELECT updated_at FROM stations WHERE station_id = 'test-0002'").fetchone()[0]
    assert old.year == 2020  # an INSERT keeps the value it was given
    conn.execute("UPDATE stations SET is_active = false WHERE station_id = 'test-0002'")
    fresh, created, now = conn.execute(
        "SELECT updated_at, created_at, now() FROM stations WHERE station_id = 'test-0002'"
    ).fetchone()
    assert fresh == now
    assert created.year == 2020
    triggers = {
        r[0]
        for r in conn.execute(
            "SELECT tgrelid::regclass::text FROM pg_trigger "
            "WHERE NOT tgisinternal AND tgfoid = 'set_updated_at'::regproc"
        )
    }
    assert triggers == TABLES


def test_replica_identity_is_full(conn):
    rows = conn.execute(
        "SELECT relname, relreplident FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind = 'r'"
    ).fetchall()
    identity = {name: ident for name, ident in rows if name in TABLES}
    assert identity == dict.fromkeys(TABLES, "f")


def test_publication_lists_only_the_five_tables(conn):
    rows = conn.execute("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'dockwatch_ops'")
    assert {(s, t) for s, t in rows} == {("public", t) for t in TABLES}
    assert conn.execute("SELECT current_database()").fetchone()[0] == "ops"


def test_slot_status_reads_wal_status_from_postgres():
    # A temporary physical slot of our own (released with its session); the real slot dockwatch_ops is never touched.
    name = f"dockwatch_probe_{os.getpid()}"
    probe = psycopg.connect(get_settings().ops_db_dsn, autocommit=True, connect_timeout=10)
    try:
        probe.execute("SELECT pg_create_physical_replication_slot(%s, true, true)", (name,))
        status = runner.slot_status(name)
        assert status == "reserved"
        assert slot_refusal(name, status) is None
        assert runner.slot_status("no_such_slot") is None
    finally:
        probe.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (name,)
        )
        probe.close()
