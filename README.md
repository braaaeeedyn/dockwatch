# DockWatch

[![CI](https://github.com/braaaeeedyn/dockwatch/actions/workflows/ci.yml/badge.svg)](https://github.com/braaaeeedyn/dockwatch/actions/workflows/ci.yml)

A real-time bike-share lakehouse on public Bay Wheels data: live GBFS station status streams through Kafka, is
processed by Spark Structured Streaming into Apache Iceberg tables, is joined with trip history and a CDC-replicated
operations database, and is published as tested, lineage-tracked marts, alerts and a live station map.

> **Status:** early build (M2 streaming done; site shell F1 and live station map F2 done; M3 CDC done: Postgres ops database → Debezium → Kafka → Spark `MERGE INTO` Iceberg, verified by row counts and checksums; M4a done: Airflow + Trino on demand, Iceberg maintenance, partition evolution demo, CDC slot keepalive; M4b done: Bay Wheels trip history with a 12-month backfill, the dbt marts on Trino and the insights JSON; Loop K: the Insights page `web/insights.html` (F4) on a compact insights format, pipeline health (`pipeline_health` → `web/data/health.json`) on the Pipeline page `web/pipeline.html`, `iceberg_maintenance` on the M4b tables too, `dbt compile` in CI, and launch tests; next: Great Expectations and lineage, then the AWS milestone, which needs an account). See [`docs/CURRENT_STATE.md`](docs/CURRENT_STATE.md) for exactly what works today.

DockWatch is an independent portfolio project. It is **not affiliated with Lyft or Bay Wheels**.

## Docs
| File | What it is |
|---|---|
| [`docs/DOCKWATCH_PLAN.md`](docs/DOCKWATCH_PLAN.md) | The project plan: what and why. |
| [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) | Ordered milestones and tasks: how. |
| [`docs/DESIGN.md`](docs/DESIGN.md) | Design system for the site (v1.0, frozen during frontend work). |
| [`docs/CURRENT_STATE.md`](docs/CURRENT_STATE.md) | What the app is right now and what is being worked on. |
| [`docs/DEVLOG.md`](docs/DEVLOG.md) | Append-only history of everything done, tried and removed. |

## Run it (so far)
Needs Python 3.12, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync                              # install
python tasks.py test                 # unit tests (no Docker, no network)
cp .env.example .env                 # local settings
python tasks.py up stream            # Redpanda, SeaweedFS (S3 API), Iceberg REST catalog, Postgres
python tasks.py setup                # create Kafka topics + local bucket
python tasks.py produce              # poll Bay Wheels GBFS -> Kafka + raw archive in local S3
python tasks.py export               # live.json / alerts.json for the site, every 60 s
python tasks.py geo                  # (once) Census map shapes -> web/geo/*.json (committed)
python tasks.py replay-export        # "Replay a day": web/data/replay.json from the raw archive (no Spark; --date, --step)
python tasks.py tf-fmt-check         # terraform fmt -check (Docker; tf-fmt rewrites, tf-validate validates)
```

Spark (Kafka -> Iceberg, episodes, alerts) runs **on demand only** (one Spark JVM at a time; never restarted
automatically):

```bash
python tasks.py catchup              # process the Kafka backlog (availableNow), then exit
python tasks.py verify-lake          # consistency checks; exits 1 on duplicates, gaps, lag or no recent commits
python tasks.py stream               # or: keep the job running, one micro-batch per minute ...
python tasks.py stream-stop          # ... and stop it (always before catchup / inspect / sql / verify-lake)
python tasks.py inspect              # the same report as verify-lake, without failing
```

The operations database (M3, Postgres `ops` on `ops-db`) is filled by a seeded simulator on the host (no JVM):

```bash
python tasks.py ops-schema           # sql/ops/schema.sql + migrations (idempotent)
python tasks.py ops-sim seeded --seed 42 --events 1500   # 1500 simulated minutes into empty tables
python tasks.py ops-explain          # EXPLAIN ANALYZE of the two hottest queries -> sql/ops/PLANS.md
```

Query plans before and after the two hot-query indexes: [`sql/ops/PLANS.md`](sql/ops/PLANS.md).

Change data capture (M3): Debezium on Kafka Connect streams every insert, update and delete in `ops` to Kafka topics
`ops.public.*`, and a Spark job applies them to Iceberg `lake.ops.*` with `MERGE INTO`. Connect and the Spark apply
start on demand and never run together (memory):

```bash
python tasks.py cdc-e2e --seed 42 --events 1500   # the whole bounded run: reset, Connect up, simulate (with a
                                                  # column add halfway), Connect stop, apply, verify -> data/cdc/e2e-report.json
python tasks.py connect-up           # or step by step: start Kafka Connect + register infra/connect/ops.json ...
python tasks.py connect-stop         # ... stop it before any Spark task
python tasks.py cdc-catchup          # MERGE the new change events into lake.ops.* (availableNow), then exit
python tasks.py cdc-catchup --replay # re-apply every event from the start: changes nothing (idempotent)
python tasks.py cdc-verify           # Postgres vs Iceberg row counts and checksums; exits 1 on any mismatch
python tasks.py cdc-reset            # drop lake.ops.*, the replication slot, the ops tables (re-created) and CDC topics
```

If `connect-up` stops with "replication slot dockwatch_ops is lost", more than 1 GB of WAL was written while Connect
was stopped (`max_slot_wal_keep_size`); rebuild CDC with `python tasks.py cdc-reset`.

Batch and orchestration (M4a): Airflow 2.11.2 (LocalExecutor, its own `airflow-db`) drives Trino 483 over the same
Iceberg catalog. Both start on demand and never beside Spark or Kafka Connect (memory); DAGs are paused by default:

```bash
python tasks.py batch-up             # Trino + airflow-db (refuses while Spark or Connect runs), airflow db migrate
python tasks.py dag-test iceberg_maintenance          # one bounded DAG run -> data/airflow/dag-test-<dag>.json
python tasks.py dag-test partition_evolution_demo     # (key=value pairs become the run conf)
python tasks.py dag-test cdc_slot_keepalive catch_up_above_mb=16   # starts / stops Connect; touches one van row so the slot advances
python tasks.py trino-sql "SELECT * FROM lake.metrics.maintenance_runs"
python tasks.py dag-test trip_history_monthly month=2019-01   # M4b: one trip month into lake.bronze.trips (pyiceberg)
python tasks.py airflow dags backfill --reset-dagruns -y -s 2025-10-01 -e 2026-09-01 trip_history_monthly   # 12 months, one at a time
python tasks.py dag-test marts_daily  # dbt build -> quality gate -> web/data/insights/*.json (read by insights.html)
python tasks.py dbt-compile          # compile the dbt project offline (no Trino; also a CI step) -> data/dbt-compile/
python tasks.py dag-test pipeline_health   # per-minute pipeline metrics -> lake.metrics.pipeline_minutely + web/data/health.json (pipeline.html)
python tasks.py batch-stop           # stop Trino and Airflow (volumes kept); always before catchup / stream / CDC tasks
python tasks.py test-airflow         # DAG tests in the Airflow image (SQLite metadata DB)
python tasks.py scheduler-up         # the Airflow scheduler, for a smoke test (scheduler-stop stops it)
```

`dag-test` refuses while another Airflow CLI run (another `dag-test` or a `dags backfill`) is still going. Keep
`trip_history_monthly` paused: unpaused with `catchup=True`, the scheduler would load every month since 2019.

Run every task from Git Bash in the folder path the containers were created from: `tasks.py` refuses when the
path's letter casing differs, because Docker Compose would recreate the stream containers.

Redpanda Console: http://localhost:8088 · local S3 endpoint: http://localhost:8333 · Trino: http://localhost:18080

The site in `web/` is static (no build step). Serve it and run its tests (Node 20 or newer locally; CI runs Node 24):

```bash
python -m http.server 5179 --bind 127.0.0.1 --directory web   # http://127.0.0.1:5179
npm ci                               # Playwright 1.64.0 + axe (dev only)
npx playwright test                  # every page: layout, accessibility, behaviour, launch screenshots (uses tests/web/fixtures)
```

**CI** (`.github/workflows/ci.yml`, on pushes to `main` and on pull requests) runs ruff, `ruff format --check`,
pytest and an offline `dbt compile` (`python tasks.py dbt-compile`), the Spark transform tests in the Spark image, `terraform fmt -check` + `validate`, the full Playwright suite
on Node 24 and a repeat job for the alerts and shell specs. Playwright runs with `--retries=0`, so a flaky test shows up red.

## Data and licence
- **Bay Wheels GBFS feeds** and **trip history**: Bay Wheels data is provided by Lyft Bikes and Scooters, LLC
  ("Bay Wheels") under the Bay Wheels License Agreement (https://www.lyft.com/bikes/bay-wheels/data-license-agreement).
  DockWatch uses it for a non-commercial portfolio project and does not use Lyft or Bay Wheels branding.
- **Map shapes:** US Census Bureau TIGER cartographic boundary files (`cb_2023_us_county_500k`, public domain),
  simplified into `web/geo/` by `python tasks.py geo`.
- **Fonts:** IBM Plex Sans and IBM Plex Mono, SIL Open Font License 1.1 (`web/fonts/OFL.txt`).
- **Icons:** Lucide v1.52.0, ISC (`web/licenses/lucide-LICENSE.txt`).
