"""EXPLAIN ANALYZE of the simulator's two hottest queries, before and after their index -> sql/ops/PLANS.md.

Everything happens in a scratch database `ops_bench` on ops-db, created and dropped here; the `ops` database, its
publication and `iceberg_catalog` are never touched. Steps:
1. run the seeded simulation (seed 42, 1500 minutes) in ops_bench to count calls per SQL constant;
2. recreate ops_bench with schema.sql + migrations, drop the two hot indexes, load a deterministic volume
   (setseed + generate_series), ANALYZE;
3. with parallel workers and JIT off: EXPLAIN (ANALYZE, BUFFERS) each query, re-create its index from the DDL in
   schema.sql, ANALYZE, EXPLAIN again.
"""

import re
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo

from dockwatch.ops_sim import db
from dockwatch.ops_sim.synthetic import SeededSimulation

BENCH_DB = "ops_bench"

# (constant, title, index, representative parameters)
HOT = (
    (
        "OPEN_JOB_AT_STATION",
        "the unfinished rebalancing job at a station",
        "rebalancing_jobs_open_station_idx",
        {"station_id": "bench-00042"},
    ),
    (
        "OPEN_TICKET_ON_DOCK",
        "the unfinished maintenance ticket on a dock",
        "maintenance_tickets_open_dock_idx",
        {"station_id": "bench-00042", "dock_number": 7},
    ),
)

WHY = {
    "OPEN_JOB_AT_STATION": (
        "Without an index Postgres has to read every job ever opened ({rows:,} rows, almost all `done` or "
        "`cancelled`) and filter them. The partial index only holds unfinished jobs (`WHERE status IN ('open', "
        "'assigned')`, about 1 % of the table), keyed by station, and the query's predicate matches the index "
        "predicate, so the planner can go straight to the few unfinished rows for this station."
    ),
    "OPEN_TICKET_ON_DOCK": (
        "The dock id comes from the `docks (station_id, dock_number)` unique index either way (the InitPlan). "
        "Without an index on tickets, Postgres then scans all {rows:,} tickets for that dock id. The partial "
        "index holds only unfinished tickets (`WHERE status <> 'closed'`) keyed by dock, so it answers with an "
        "index scan over a handful of entries instead."
    ),
}

BENCH_LOAD = """
SELECT setseed(0.42);
INSERT INTO stations (station_id, name, short_name, lat, lon, capacity, region_id)
SELECT format('bench-%s', lpad(g::text, 5, '0')), format('Bench station %s', g), format('B%s', g),
       37.70 + random() * 0.11, -122.52 + random() * 0.14, 20, (ARRAY['3', '5', '12'])[1 + g % 3]
FROM generate_series(1, 2000) AS g;
INSERT INTO docks (station_id, dock_number)
SELECT s.station_id, d FROM stations AS s CROSS JOIN generate_series(1, 20) AS d ORDER BY 1, 2;
INSERT INTO vans (plate, capacity) SELECT format('BV-%s', lpad(g::text, 3, '0')), 20 FROM generate_series(1, 40) AS g;
INSERT INTO rebalancing_jobs (station_id, van_id, status, alert_episode_id, opened_at, closed_at, bikes_moved)
SELECT format('bench-%s', lpad((1 + floor(r1 * 2000))::int::text, 5, '0')),
       CASE WHEN st = 'open' THEN NULL ELSE 1 + g % 40 END,
       st, format('bench-episode-%s', g),
       timestamptz '2025-01-01 00:00+00' + g * interval '2 minutes',
       CASE WHEN st IN ('done', 'cancelled') THEN timestamptz '2025-01-01 00:00+00' + g * interval '2 minutes'
            + interval '40 minutes' END,
       CASE WHEN st = 'done' THEN floor(r2 * 15)::int END
FROM (SELECT g, random() AS r1, random() AS r2,
             CASE WHEN r < 0.005 THEN 'open' WHEN r < 0.01 THEN 'assigned' WHEN r < 0.15 THEN 'cancelled'
                  ELSE 'done' END AS st
      FROM (SELECT g, random() AS r FROM generate_series(1, %(jobs)s) AS g) AS x) AS y;
INSERT INTO maintenance_tickets (dock_id, issue, status, opened_at, closed_at)
SELECT 1 + floor(r1 * 40000)::int,
       (ARRAY['jammed', 'broken_lock', 'no_power', 'damaged', 'other'])[1 + floor(r2 * 5)::int],
       st, timestamptz '2025-01-01 00:00+00' + g * interval '3 minutes',
       CASE WHEN st = 'closed' THEN timestamptz '2025-01-01 00:00+00' + g * interval '3 minutes'
            + interval '2 hours' END
FROM (SELECT g, random() AS r1, random() AS r2,
             CASE WHEN r < 0.005 THEN 'open' WHEN r < 0.01 THEN 'in_progress' ELSE 'closed' END AS st
      FROM (SELECT g, random() AS r FROM generate_series(1, %(tickets)s) AS g) AS x) AS y;
"""


def index_ddl(name: str) -> str:
    text = (db.SQL_DIR / "schema.sql").read_text(encoding="utf-8")
    m = re.search(rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+{name}\b[^;]*;", text, re.I)
    if not m:
        raise SystemExit(f"index {name} is not created in sql/ops/schema.sql")
    return m.group(0)


def recreate_bench(admin: psycopg.Connection) -> None:
    admin.execute(f"DROP DATABASE IF EXISTS {BENCH_DB} WITH (FORCE)")
    admin.execute(f"CREATE DATABASE {BENCH_DB}")


def count_calls(bench_dsn: str, seed: int, events: int, stations: int) -> tuple[dict[str, int], float]:
    t = time.monotonic()
    sim = SeededSimulation(seed=seed, events=events, n_stations=stations)
    with db.connect(bench_dsn) as conn:
        db.apply_schema(conn, base_only=True)
        ex = db.run_seeded(conn, sim, range(events))
    return dict(ex.calls.most_common()), time.monotonic() - t


def explain(conn: psycopg.Connection, sql: str) -> str:
    conn.execute(sql).fetchall()  # warm the cache, so before and after both read from shared buffers
    return "\n".join(r[0] for r in conn.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}").fetchall())


def exec_ms(plan: str) -> float:
    m = re.search(r"Execution Time: ([\d.]+) ms", plan)
    return float(m.group(1)) if m else -1.0


def run(
    dsn: str,
    out: Path,
    seed: int = 42,
    events: int = 1500,
    stations: int = 80,
    jobs: int = 200_000,
    tickets: int = 200_000,
) -> dict:
    started = time.monotonic()
    bench_dsn = make_conninfo(dsn, dbname=BENCH_DB)
    with db.connect(dsn) as admin:
        try:
            recreate_bench(admin)
            calls, sim_s = count_calls(bench_dsn, seed, events, stations)
            hottest = list(calls)[:2]
            wanted = [h[0] for h in HOT]
            if sorted(hottest) != sorted(wanted):
                raise SystemExit(f"the two most-called queries are {hottest}, not {wanted}: {calls}")

            recreate_bench(admin)
            with db.connect(bench_dsn) as conn:
                db.apply_schema(conn)
                for _, _, index, _ in HOT:
                    conn.execute(f"DROP INDEX {index}")
                load_t = time.monotonic()
                with conn.transaction():
                    conn.execute(BENCH_LOAD.replace("%(jobs)s", str(jobs)).replace("%(tickets)s", str(tickets)))
                conn.execute("ANALYZE")
                load_s = time.monotonic() - load_t
                conn.execute("SET max_parallel_workers_per_gather = 0")
                conn.execute("SET jit = off")
                rows = {t: n for t, n in db.table_counts(conn).items()}
                results = []
                for name, title, index, params in HOT:
                    sql = psycopg.ClientCursor(conn).mogrify(db.QUERIES[name], params)
                    before = explain(conn, sql)
                    conn.execute(index_ddl(index))
                    conn.execute("ANALYZE")
                    after = explain(conn, sql)
                    results.append(
                        {
                            "name": name,
                            "title": title,
                            "index": index,
                            "sql": sql,
                            "before": before,
                            "after": after,
                            "calls": calls[name],
                        }
                    )
        finally:
            admin.execute(f"DROP DATABASE IF EXISTS {BENCH_DB} WITH (FORCE)")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(results, calls, rows, seed, events, stations), encoding="utf-8", newline="\n")
    summary = {
        "out": out.as_posix(),
        "calls": calls,
        "sim_s": round(sim_s, 1),
        "load_s": round(load_s, 1),
        "total_s": round(time.monotonic() - started, 1),
        "queries": {r["name"]: {"before_ms": exec_ms(r["before"]), "after_ms": exec_ms(r["after"])} for r in results},
    }
    return summary


def render(
    results: list[dict], calls: dict[str, int], rows: dict[str, int], seed: int, events: int, stations: int
) -> str:
    top = ", ".join(f"`{k}` {v}" for k, v in list(calls.items())[:6])
    lines = [
        "# Query plans: the simulator's two hottest queries",
        "",
        f"Written by `python tasks.py ops-explain` on {datetime.now(UTC).strftime('%Y-%m-%d')} (PostgreSQL 16, "
        "compose service `ops-db`). Do not edit by hand.",
        "",
        f"**Hottest** means most-called: `ops_sim/db.py` counts calls per SQL constant, and in the seeded run "
        f"(seed {seed}, {events} simulated minutes, {stations} stations) the top calls were {top}. The two lookups "
        "find the unfinished job at a station and the unfinished ticket on a dock; every job and ticket insert and "
        "update goes through one of them.",
        "",
        "**Bench:** a scratch database `ops_bench` (dropped afterwards) with `schema.sql`, the two indexes dropped, "
        "and a deterministic volume loaded with `setseed` + `generate_series`: "
        + ", ".join(f"{n:,} {t}" for t, n in rows.items())
        + ". `ANALYZE` after loading and after each `CREATE INDEX`; `max_parallel_workers_per_gather = 0` and "
        "`jit = off`; each query runs once to warm the cache before `EXPLAIN (ANALYZE, BUFFERS)`.",
        "",
    ]
    for i, r in enumerate(results, 1):
        table_rows = rows["rebalancing_jobs" if "job" in r["name"].lower() else "maintenance_tickets"]
        b, a = exec_ms(r["before"]), exec_ms(r["after"])
        lines += [
            f"## Query {i}: {r['title']}",
            f"Source: `src/dockwatch/ops_sim/db.py` constant `{r['name']}`",
            f"Calls in the seeded run: {r['calls']}",
            "```sql",
            r["sql"],
            "```",
            "### Before (no index)",
            "```",
            r["before"],
            "```",
            f"### After (`{r['index']}`)",
            "```",
            r["after"],
            "```",
            f"**Why the plan changed** ({b} ms → {a} ms, {b / a:.0f}× faster): "
            + WHY[r["name"]].format(rows=table_rows),
            "",
        ]
    return "\n".join(lines)
