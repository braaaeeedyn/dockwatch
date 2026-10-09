# Query plans: the simulator's two hottest queries

Written by `python tasks.py ops-explain` on 2026-10-09 (PostgreSQL 16, compose service `ops-db`). Do not edit by hand.

**Hottest** means most-called: `ops_sim/db.py` counts calls per SQL constant, and in the seeded run (seed 42, 1500 simulated minutes, 80 stations) the top calls were `OPEN_TICKET_ON_DOCK` 365, `OPEN_JOB_AT_STATION` 308, `SET_DOCK_STATUS` 243, `UPDATE_TICKET` 242, `SET_VAN_STATUS` 238, `INSERT_TICKET` 123. The two lookups find the unfinished job at a station and the unfinished ticket on a dock; every job and ticket insert and update goes through one of them.

**Bench:** a scratch database `ops_bench` (dropped afterwards) with `schema.sql`, the two indexes dropped, and a deterministic volume loaded with `setseed` + `generate_series`: 2,000 stations, 40,000 docks, 40 vans, 200,000 rebalancing_jobs, 200,000 maintenance_tickets. `ANALYZE` after loading and after each `CREATE INDEX`; `max_parallel_workers_per_gather = 0` and `jit = off`; each query runs once to warm the cache before `EXPLAIN (ANALYZE, BUFFERS)`.

## Query 1: the unfinished rebalancing job at a station
Source: `src/dockwatch/ops_sim/db.py` constant `OPEN_JOB_AT_STATION`
Calls in the seeded run: 308
```sql
SELECT job_id, status FROM rebalancing_jobs
WHERE station_id = 'bench-00042' AND status IN ('open', 'assigned')
```
### Before (no index)
```
Seq Scan on rebalancing_jobs  (cost=0.00..6419.00 rows=1 width=13) (actual time=0.695..15.553 rows=3 loops=1)
  Filter: ((status = ANY ('{open,assigned}'::text[])) AND (station_id = 'bench-00042'::text))
  Rows Removed by Filter: 199997
  Buffers: shared hit=3419
Planning Time: 0.069 ms
Execution Time: 15.567 ms
```
### After (`rebalancing_jobs_open_station_idx`)
```
Index Scan using rebalancing_jobs_open_station_idx on rebalancing_jobs  (cost=0.28..8.29 rows=1 width=13) (actual time=0.010..0.012 rows=3 loops=1)
  Index Cond: (station_id = 'bench-00042'::text)
  Buffers: shared hit=5
Planning Time: 0.051 ms
Execution Time: 0.021 ms
```
**Why the plan changed** (15.567 ms → 0.021 ms, 741× faster): Without an index Postgres has to read every job ever opened (200,000 rows, almost all `done` or `cancelled`) and filter them. The partial index only holds unfinished jobs (`WHERE status IN ('open', 'assigned')`, about 1 % of the table), keyed by station, and the query's predicate matches the index predicate, so the planner can go straight to the few unfinished rows for this station.

## Query 2: the unfinished maintenance ticket on a dock
Source: `src/dockwatch/ops_sim/db.py` constant `OPEN_TICKET_ON_DOCK`
Calls in the seeded run: 365
```sql
SELECT ticket_id, status FROM maintenance_tickets
WHERE dock_id = (SELECT dock_id FROM docks WHERE station_id = 'bench-00042' AND dock_number = 7)
    AND status <> 'closed'
```
### Before (no index)
```
Seq Scan on maintenance_tickets  (cost=8.31..5311.31 rows=1 width=15) (actual time=10.010..10.011 rows=0 loops=1)
  Filter: ((status <> 'closed'::text) AND (dock_id = $0))
  Rows Removed by Filter: 200000
  Buffers: shared hit=2306
  InitPlan 1 (returns $0)
    ->  Index Scan using docks_station_id_dock_number_key on docks  (cost=0.29..8.31 rows=1 width=8) (actual time=0.012..0.013 rows=1 loops=1)
          Index Cond: ((station_id = 'bench-00042'::text) AND (dock_number = 7))
          Buffers: shared hit=3
Planning Time: 0.058 ms
Execution Time: 10.027 ms
```
### After (`maintenance_tickets_open_dock_idx`)
```
Index Scan using maintenance_tickets_open_dock_idx on maintenance_tickets  (cost=8.59..16.61 rows=1 width=15) (actual time=0.024..0.024 rows=0 loops=1)
  Index Cond: (dock_id = $0)
  Buffers: shared hit=5
  InitPlan 1 (returns $0)
    ->  Index Scan using docks_station_id_dock_number_key on docks  (cost=0.29..8.31 rows=1 width=8) (actual time=0.015..0.015 rows=1 loops=1)
          Index Cond: ((station_id = 'bench-00042'::text) AND (dock_number = 7))
          Buffers: shared hit=3
Planning Time: 0.068 ms
Execution Time: 0.043 ms
```
**Why the plan changed** (10.027 ms → 0.043 ms, 233× faster): The dock id comes from the `docks (station_id, dock_number)` unique index either way (the InitPlan). Without an index on tickets, Postgres then scans all 200,000 tickets for that dock id. The partial index holds only unfinished tickets (`WHERE status <> 'closed'`) keyed by dock, so it answers with an index scan over a handful of entries instead.
