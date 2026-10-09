# DockWatch: implementation plan

> Turns [`DOCKWATCH_PLAN.md`](DOCKWATCH_PLAN.md) (the *what* and *why*) into an ordered list of tasks (the *how*),
> and adds the web app: a small, fully responsive site built on [`DESIGN.md`](DESIGN.md) with a live station map,
> an alerts feed, an insights page and a pipeline health page.
>
> **How to use this file:** work top to bottom. Each milestone has tasks you can check off, the files they create,
> and a **Done when** line. Don't start a milestone until the previous one's *Done when* holds, except where a task
> is marked *(parallel)*.
>
> **Living docs kept alongside this plan:**
> - [`CURRENT_STATE.md`](CURRENT_STATE.md): what the app is *right now* and what is being worked on. Rewritten as things
>   change; anything removed from the code is removed from it.
> - [`DEVLOG.md`](DEVLOG.md): append-only history of everything done, decided, tried, broken and removed.

---

## 0. Decisions this plan makes

These fill the gaps the main plan leaves open. Changing one later means a `DEVLOG.md` entry explaining why.

| Topic | Decision | Why |
|---|---|---|
| Feed version | **GBFS 2.3** at `https://gbfs.lyft.com/gbfs/2.3/bay/gbfs.json`, found via auto-discovery from `gbfs.baywheels.com/gbfs/gbfs.json` (which 301-redirects). | Checked 2026-10-07: 2.3 adds `vehicle_types_available`, `num_ebikes_available`; 1.1 is also published but older. |
| Poll cadence | `station_status` and `station_information` are **both** polled at their `ttl` (60 s today). `station_information` is only *produced* when its content hash changes. | The main plan assumed a daily `station_information`; the live feed says `ttl: 60`. Hashing keeps the compacted topic quiet. |
| Two run targets | **`local`** (SeaweedFS for the S3 API, Iceberg REST catalog, Trino) and **`aws`** (S3, Glue Data Catalog, Athena). Same Spark/dbt code; only config changes. | Lets every milestone be built and tested at $0 with no AWS account, then pointed at AWS once the account and budget alerts exist. |
| Docker memory | Compose **profiles**: `stream` (Redpanda, Postgres, Kafka Connect, SeaweedFS, Iceberg REST, Spark), `batch` (Airflow, Trino), `obs` (Marquez). Run two at most at once. | This machine gives Docker ~8 GB. Running everything at once will not fit. |
| Spark runtime | Spark **3.5.x** jobs run in the `apache/spark` Docker image (Java 17). Unit tests use a local `SparkSession` on the host (PySpark 3.5 works with the installed Java 22 per the TransitPulse log; if it stops working, tests move into the container). | Avoids host Java version problems for the long-running jobs. |
| Stateful episodes | `applyInPandasWithState` (PySpark) for empty/full episodes, keyed by `station_id`, with an event-time timeout. | The Python-native stateful API in Spark 3.5; no Scala needed. |
| Python | **3.12**, managed by `uv`; task runner is `tasks.py` (no `make` on this Windows machine), `Makefile` forwards to it for CI/Linux. | Same habit as TransitPulse. |
| Frontend | Vanilla HTML/CSS/JS ES modules, **no build step**, static files. Reads small JSON files only (`live.json`, `alerts.json`, `health.json`, `insights/*.json`). | A static site is free to host and can't break the pipeline. |
| "Live" data path | An **exporter** consumes the `silver` and `alerts` topics and writes `live.json` / `alerts.json` every 60 s (local disk; S3 in the `aws` target). Insights and health JSON come from Trino/Athena queries run by Airflow. | Keeps "refreshed from the stream, not the public API" true without a server. |
| When the laptop is off | The public site shows the **last export** with a clear "Data as of <time>" banner, plus a "Replay a day" mode that plays an archived day of snapshots. | The pipeline runs on a laptop; the site must stay honest when it's not running. |
| Station positions | **Projected in the browser** (`web/js/map/project.js`) with the per-region projection parameters stored in `web/geo/<view>.json`, mirroring `dockwatch.geo.build.project`; `web/geo` holds only land shapes, landmarks and the projection. (Changed 2026-10-08 from "station positions → `web/geo`".) | New or moved stations show up without re-running the build step; `live.json` already carries lat/lon. |
| Map base | Static **SVG** land/water from US Census TIGER cartographic boundaries (public domain), simplified; stations drawn in SVG (641 points is fine for SVG). Three **region views**: San Francisco · East Bay (Oakland, Emeryville, Berkeley) · San José. No tiles, no API keys. | Bay Wheels is three separate clusters; one map of all of them makes the stations tiny. |
| Design | [`DESIGN.md`](DESIGN.md) **v1.0, frozen** from the start of F1 until launch (F5 done). Gaps go to `docs/DESIGN_BACKLOG.md`; only accessibility fixes edit `DESIGN.md`. | Same rule as TransitPulse: the design doesn't move while the frontend is being built. |
| Local S3 | **SeaweedFS** (`chrislusf/seaweedfs`, S3 gateway on :8333) instead of MinIO. | MinIO's images can no longer be pulled from Docker Hub or Quay (checked 2026-10-07). |
| Local ports | Redpanda 19092, Console **8088**, S3 8333, Iceberg REST 8181, ops Postgres **5434**. | 8080, 5432 and 5433 are already used by other projects on this machine. |
| Git | `dockwatch/` is its own git repo (the parent `coding/` folder is not the project). No commits are made by tooling unless asked. | The main plan calls for `github.com/braaaeeedyn/dockwatch`. |

### Data facts checked on 2026-10-07 (live feed)
- 641 stations, regions: San Francisco (358), Oakland (103), San José (82), Berkeley (48), Emeryville (15),
  **no region (35)**. `system_regions` lists each region **twice** → de-duplicate.
- `station_id` is a UUID; `short_name` (e.g. `SJ-J6`) is the human code.
- At one moment: 80 stations empty, 8 full, 4 not renting. `last_reported` ranged from 5 s to **~16 days** old →
  stale stations must be flagged "offline", not counted as empty/full.
- `station_status` row fields: `is_installed`, `is_renting`, `is_returning`, `last_reported`, `num_bikes_available`,
  `num_bikes_disabled`, `num_docks_available`, `num_docks_disabled`, `num_ebikes_available`,
  `num_scooters_available`, `num_scooters_unavailable`, `vehicle_types_available[]`.
- `bart.gov` blocked Python's default User-Agent in TransitPulse; `gbfs.lyft.com` accepted a custom one. The producer
  always sends `User-Agent: dockwatch/<version> (+repo url)`.

---

## 1. Repo layout (created in M0)

```
dockwatch/
├── pyproject.toml / uv.lock        # Python 3.12, uv, dependency groups per area
├── tasks.py  Makefile              # python tasks.py up|down|test|lint|produce|stream|...
├── docker-compose.yml              # profiles: stream, batch, obs
├── .env.example                    # every setting the code reads; real .env is gitignored
├── .github/workflows/              # ci.yml (PR), deploy.yml (main)
├── infra/
│   ├── terraform/                  # AWS: S3, Glue, Athena workgroup, IAM, Lambda, SNS, budgets, OIDC
│   ├── connect/                    # Debezium connector JSON
│   └── trino/  iceberg-rest/       # local catalog + query engine config
├── src/dockwatch/
│   ├── config.py                   # one settings object (pydantic-settings), local vs aws target
│   ├── gbfs/                       # discovery, fetch, parse, models
│   ├── producer/                   # GBFS → Kafka (+ raw archive, DLQ)
│   ├── streaming/                  # Spark Structured Streaming jobs + pure transform functions
│   ├── cdc/                        # Debezium envelope parsing + MERGE apply job
│   ├── ops_sim/                    # operations DB simulator (Postgres)
│   ├── exporter/                   # topics → live.json / alerts.json
│   ├── alerts/                     # Lambda handler (SNS email)
│   └── metrics/                    # pipeline metrics writer
├── airflow/dags/                   # trip_history_monthly, iceberg_maintenance, marts_daily, exports
├── dbt/dockwatch/                  # models/ tests/ macros/ (dbt-trino locally, dbt-athena on aws)
├── quality/                        # Great Expectations suites + checkpoints
├── sql/ops/                        # Postgres schema, indexes, EXPLAIN ANALYZE notes
├── web/                            # static site
│   ├── index.html insights.html pipeline.html about.html
│   ├── css/ tokens.css base.css layout.css components.css map.css
│   ├── js/  main.js map/ alerts.js insights.js pipeline.js util/
│   └── fonts/ data/ geo/
├── tests/                          # pytest (unit + integration markers) and web/ (Playwright)
└── docs/                           # this plan, DESIGN.md, CURRENT_STATE.md, DEVLOG.md, METRICS.md
```

---

## 2. Milestones at a glance

| Week | Backend | Frontend | Checkpoint |
|---|---|---|---|
| 0 | **M0** setup, Compose, Terraform base | | Repo runs `tasks.py test`; Terraform validates |
| 1 | **M1** GBFS → Kafka | **F1** tokens + responsive shell *(parallel)* | Live messages in Redpanda |
| 2–3 | **M2** Spark → Iceberg, episodes, alerts | **F2** live station map · **F3** alerts feed | Live map from the stream |
| 4 | **M3** Postgres ops DB + Debezium CDC | | `MERGE` with deletes proven |
| 5–6 | **M4** Airflow: history backfill, maintenance, dbt marts | **F4** insights page | Marts queryable |
| 7 | **M5** Great Expectations, OpenLineage, metrics | **F4b** pipeline health page | Health page from real metrics |
| 8 | **M6** AWS target, CI/CD, measurements | **F5** polish, a11y, perf, device QA | Public launch |
| 9–10 | buffer · optional **M7** (Snowflake *or* Databricks) | portfolio page | |

---

## 3. Backend milestones

### M0 · Setup (week 0)
- [x] `git init`; `.gitignore` (secrets, `.env`, data, Terraform state, Spark checkpoints, `web/data/*.json`).
- [x] `pyproject.toml` with uv dependency groups (`dev`, `producer` so far; `spark`, `cdc`, `airflow`, `dbt`,
      `quality` are added by the milestone that needs them). Python pinned to 3.12.
- [x] `src/dockwatch/config.py`: one settings object, read from env / `.env`, with `DOCKWATCH_TARGET=local|aws`.
- [x] `tasks.py` with `test`, `lint`, `fmt`, `up <profile>`, `down`, `ps`, `setup`, `produce`, `produce-once`,
      `replay`, `tf-fmt`, `tf-validate`; `Makefile` forwards.
- [x] `docker-compose.yml`, profile `stream` first: Redpanda (+ Console), SeaweedFS (S3 API), Iceberg REST catalog
      (catalog persisted to a volume), Postgres 16 (`wal_level=logical`). Later milestones add their own services.
- [x] `infra/terraform/`: S3 bucket (`raw/`, `warehouse/`, `checkpoints/`, lifecycle on `raw/` after 30 days),
      Glue database, Athena workgroup with a bytes-scanned limit, IAM roles (spark-writer, athena-reader, lambda),
      SNS topic, **budgets at $1 and $5**, GitHub OIDC provider. Remote state in S3 with lockfile.
      Validate with the `hashicorp/terraform` Docker image (terraform is not installed on this machine).
      *(Remote-state backend is written but commented out until the AWS account exists.)*
- [ ] **You (manual):** AWS account, MFA on root, an admin IAM user, then `terraform apply`. Not needed until M6.
- [x] README: what DockWatch is, data attribution and licence (Bay Wheels data licence agreement), how to run.

**Done when:** `python tasks.py lint test` passes, `python tasks.py up stream` starts healthy containers, and
`terraform validate` passes.

### M1 · Ingest: GBFS → Kafka (week 1)
- [x] `gbfs/client.py`: follow the auto-discovery file (and its redirect) to feed URLs; cache them; re-discover on 404.
- [x] `gbfs/models.py`: typed parsing of `station_status` / `station_information` / `system_regions`
      (pydantic). Unknown fields are kept in an `extra` map so a GBFS addition doesn't break parsing (schema evolution later).
- [x] `producer/`: poll loop that honours `ttl` and `last_updated` (skip a snapshot whose `last_updated` didn't change);
      explode a snapshot into one message per station, **key = `station_id`**, value = JSON with `last_reported`
      (event time), `feed_last_updated`, `fetched_at`, `snapshot_id`.
- [x] Topics: `gbfs.station_status` (6 partitions, 7-day retention), `gbfs.station_information` (compacted),
      `gbfs.dlq` (malformed payloads with the error), `gbfs.station_status.replay`, created by `tasks.py setup`.
- [x] Idempotent producer (`enable.idempotence=true`, `acks=all`); graceful shutdown flushes.
- [x] Raw archive: every fetched snapshot as gzip JSON to `raw/gbfs/<feed>/dt=YYYY-MM-DD/HH/<last_updated>.json.gz`
      (SeaweedFS locally, S3 on aws; UTC dates/hours). This is the replay source for M6's throughput test.
- [x] `producer replay --date --speed 10`: re-publishes an archived day at N× speed to `gbfs.station_status.replay`
      (not the live topic, so old events never mix with live data).
- [x] Tests: discovery, parsing (fixtures from real snapshots), explode/keying, change detection, DLQ routing.
- [x] Notes in `docs/CONCEPTS.md`: partitions and ordering, consumer groups, offsets, rebalancing, compaction,
      at-least-once vs exactly-once — written as the features are built.

**Done when:** the producer has run for an hour against the live feed, Redpanda shows ~641 messages per minute
in `gbfs.station_status`, the raw archive has 60 snapshot files, and the tests pass.
**✅ Met 2026-10-07:** ran ~4 h; full hours have 59–60 archived snapshots; 641 messages per snapshot; 0 DLQ rows.

### M2 · Stream processing into Iceberg (weeks 2–3)
- [x] `streaming/transforms.py`: **pure functions** on DataFrames (parse, clean, classify station state); tested in the
      Spark image (`python tasks.py test-spark`) because the host has Java 22.
- [x] Station state rule (shared with the web legend): `offline` if not installed / not renting / `last_reported` older
      than 30 min; `empty` if `num_bikes_available = 0`; `full` if `num_docks_available = 0`; `low` if bikes ≤ 2 or
      ≤ 10 % of capacity; `high` if docks ≤ 2 or ≤ 10 % of capacity; else `ok`. Thresholds live in config.
      Capacity = bikes + docks, available + disabled (`station_status` has no capacity field).
- [x] Job `status_stream`: Kafka → parse → `bronze.station_status` (append, every Kafka record + offsets + raw JSON)
      and `silver.station_status` (cleaned, state column), **de-duplicated on (`station_id`, `snapshot_ts`)** with a
      `10 minutes` watermark (`dropDuplicatesWithinWatermark`). *Changed from (`station_id`, `last_reported`): see
      decision below.*
- [x] Iceberg tables partitioned by `days(snapshot_ts)`, write order `station_id, snapshot_ts`.
- [x] **Schema evolution:** `num_scooters_available` / `num_scooters_unavailable` added in place (`lake.migrate()`);
      old rows read as null, nothing rewritten.
- [ ] **Partition evolution** — moved to M4 (see decision below).
- [x] `silver.availability_5m`: 5-minute tumbling windows per station (samples, min/avg/max bikes, share empty/full/offline).
- [x] `silver.station_episodes`: empty/full **episodes** via `applyInPandasWithState` (state machine in plain Python,
      `streaming/episodes.py`), upserted with **`MERGE INTO`** in `foreachBatch`.
- [x] `gbfs.alerts` topic: an episode that passes N minutes (default 15) emits `raised`; its close emits `resolved`.
      Also `dockwatch.station_state` (compacted): latest state per station, for the exporter.
- [x] Checkpoints in the `checkpoints` volume. **Kill-and-restart test** (SIGKILL, restart): every Kafka offset in
      bronze exactly once, 0 gaps in all 6 partitions, 0 silver duplicates. Checks live in `python tasks.py inspect`.
- [x] `exporter/`: consumes `dockwatch.station_state`, `gbfs.station_information` and `gbfs.alerts`; writes
      `web/data/live.json` and `alerts.json` every 60 s (atomic writes).
- [x] `alerts/handler.py`: Lambda handler that formats an alert and publishes to SNS; prints locally. Wired to AWS in M6.

**Done when:** the live map (F2) is drawn from `live.json` written by the exporter, the restart test passes, and an
alert shows up in `alerts.json` for a real long-empty station.
**Status 2026-10-08:** restart test ✅; real alerts in `alerts.json` ✅ (51 open at first export); Postgres-backed
catalog verified with `python tasks.py verify-lake` (208,966 bronze rows = every Kafka offset, 0 gaps, 0 silver
duplicates, fresh commits on all four tables) ✅; Spark now runs on demand (`catchup` / `stream`); live map (F2) ✅,
drawn from the exporter's `live.json`; spot-check 10/10 stations match the public feed for the same snapshot ✅.

**M2 decisions**
| Decision | Why |
|---|---|
| Silver de-duplicates on (`station_id`, `snapshot_ts`), not (`station_id`, `last_reported`). | Stale stations repeat the same `last_reported` for days, so a watermark on it would drop them as late (they'd never show as offline), and keying on it without a watermark grows state forever. One row per station per snapshot is also what time-weighted availability needs. |
| Event time = `snapshot_ts` (feed `last_updated`). | It's when the state was observed; `last_reported` stays as a column for the offline rule. |
| One Spark app, four queries, each with its own checkpoint. | Fits in ~1.5 GB; each output recovers on its own. |
| Alerts and station state go to Kafka from `foreachBatch` (at-least-once), keyed by `alert_id` / `station_id`. | Kafka batch writes in `foreachBatch` aren't transactional; keys make duplicates harmless (consumers keep the latest per key). |
| Partition evolution moved to M4. | Moving `days → hours` now would multiply the small-files problem before compaction exists. It will be shown with `iceberg_maintenance`, where its effect on file counts can be measured. |
| Inspection tasks run Spark with whole-stage codegen off. | A second JVM segfaulted (`SymbolTable::do_lookup`) on `COUNT(DISTINCT …)` with codegen on, reproducibly, with plenty of memory; codegen off fixes it. The streaming job hasn't hit it. |
| Iceberg REST catalog pointers in Postgres (`iceberg_catalog` on `ops-db`), not SQLite. | SQLite (in-memory or file) allows one writer; four streaming queries committing together got `SQLITE_BUSY` → `CommitStateUnknownException` → job restarts. Verified 2026-10-08 with `verify-lake`. |
| Spark runs on demand: `restart: "no"`, `catchup` (`availableNow`, drains Kafka then exits) or `stream` until `stream-stop`. | The host is memory-tight; a 24/7 job stalled (driver heartbeat timeout) and auto-restarted. Kafka keeps 7 days, so a catch-up run later loses nothing; checkpoints are shared by both modes. |

### M3 · CDC from the operational database (week 4)
- [ ] `sql/ops/schema.sql`: `stations`, `docks`, `maintenance_tickets`, `rebalancing_jobs`, `vans`; normalised,
      foreign keys, `CHECK` constraints, `updated_at` triggers.
- [ ] `ops_sim/`: reacts to `gbfs.alerts` (opens a rebalancing job when a station has been empty 20 min, closes it
      when the episode resolves), opens/closes random maintenance tickets, and **deletes** cancelled jobs.
- [ ] Debezium Postgres connector (`infra/connect/ops.json`) → `ops.public.*` topics; Kafka Connect added to the
      `stream` profile.
- [ ] `cdc/apply.py`: Spark job applying inserts/updates/deletes to Iceberg `ops.*` tables with `MERGE INTO`,
      ordered by the source LSN, idempotent on replay.
- [ ] `EXPLAIN ANALYZE` the simulator's two hottest queries before and after adding an index → `sql/ops/PLANS.md`.
- [ ] Add a column in Postgres; show it flowing through Debezium into Iceberg (schema evolution end to end).

**Done when:** row counts and a checksum of each `ops.*` table match between Postgres and Iceberg after an hour of
simulation including deletes.

### M4 · Batch, history and orchestration · Airflow (weeks 5–6)
- [ ] Airflow 2.x in the `batch` profile (LocalExecutor, Postgres metadata DB shared with nothing else).
- [ ] DAG `trip_history_monthly`: download a month's trip ZIP, validate columns, load to `bronze.trips`;
      `catchup=True` so `airflow dags backfill -s 2019-01-01 -e ...` loads history. Handles the older and newer
      trip-file schemas (the column names changed over the years).
- [ ] DAG `iceberg_maintenance` (daily): `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`; logs
      file counts and sizes before/after into `metrics.maintenance_runs`.
- [ ] dbt project (`dbt-trino` locally, `dbt-athena` on aws, one project with two targets):
      `mart_station_availability_daily`, `mart_rebalancing_need`, `mart_trips_vs_availability`, plus staging models
      and tests (`unique`, `not_null`, `accepted_values` for state, relationships to stations).
- [ ] DAG `marts_daily`: dbt build → Great Expectations checkpoint → **only then** export `web/data/insights/*.json`.
- [ ] Retries with exponential backoff, SLA misses and task failures alert (email/SNS on aws, log locally).

**Done when:** a backfill of 12 months runs green, marts build, and the insights JSON is produced by the DAG.

### M5 · Quality, lineage, observability (week 7)
- [ ] Great Expectations suites: schema; ranges (`num_bikes_available ≥ 0`, `bikes + docks ≤ capacity` with a
      tolerance for disabled docks); uniqueness; freshness (`silver.station_status` updated < 5 min ago).
- [ ] OpenLineage: Airflow provider + Spark listener → Marquez (`obs` profile). Screenshot of the graph from
      `gbfs.station_status` to each mart, saved to `docs/img/lineage.png`.
- [ ] `metrics/`: `metrics.pipeline_minutely` (events/min, end-to-end latency p50/p95 = Iceberg commit time −
      `last_reported`, consumer lag, files per partition) and an export to `web/data/health.json`.

**Done when:** the pipeline page (F4b) shows real latency, lag, freshness and the latest GE results, and Marquez
shows the full graph.

### M6 · AWS target, CI/CD, measurements (week 8)
- [ ] `terraform apply` (after the manual AWS steps in M0). Point `DOCKWATCH_TARGET=aws` at S3 + Glue; run the stream
      for a day; query with Athena.
- [ ] Lambda + SNS wired to `gbfs.alerts` through a tiny consumer.
- [x] GitHub Actions ci.yml: ruff, ruff format, pytest, Spark transform tests (Spark image), terraform fmt -check + validate, Playwright suite + repeat job, retries 0. (2026-10-08; first GitHub run checked after push)
- [ ] ci.yml: dbt compile (when M5 adds dbt).
- [ ] ci.yml: terraform plan via GitHub OIDC (needs the AWS account; no stored keys).
- [ ] deploy.yml on main publishes web/ to S3 static hosting.
- [ ] Measurements into `docs/METRICS.md` (each with the command that produced it):
      p50/p95 end-to-end latency; sustained throughput at 10× and 50× replay; file count and query time before vs.
      after compaction; Athena bytes scanned for a full backfill vs. one incremental month; restart with a zero-diff count.

**Done when:** every number in the README résumé bullet is filled from `METRICS.md`.

### M7 · Optional (choose at most one)
- [ ] **7a** Snowflake reads the same Iceberg tables via a Glue catalog integration; one dbt mart built there and
      compared with Athena.
- [ ] **7b** Databricks Free Edition runs the trip-history backfill into Delta Lake; compare MERGE, time travel and
      compaction with Iceberg.

---

## 4. Frontend milestones

All of these follow [`DESIGN.md`](DESIGN.md). Every page must work from 320 px to 2560 px with no horizontal page scroll.

### F1 · Tokens and responsive shell (week 1, parallel with M1)
- [x] `web/css/tokens.css` generated by hand from the `DESIGN.md` front matter (light + dark themes).
- [x] Self-hosted IBM Plex Sans (400/500/600) and IBM Plex Mono (400/500), `woff2`, Latin subset.
- [x] Shell: nav (Live · Insights · Pipeline · About), "data as of" status pill, footer with attribution; skip link.
- [x] Theme: follows `prefers-color-scheme`, with a manual toggle remembered in `localStorage` (wrapped in try/catch).
- **Done when:** the empty shell passes axe with no violations at 320, 768 and 1440 px in both themes.
- **Status 2026-10-08:** ✅ done. `tests/web/shell.spec.js` (Playwright 1.64.0 + axe): 0 WCAG 2.x A/AA violations on
  all four pages at 320 / 768 / 1440 px in light and dark, no horizontal scroll from 320 to 2560 px. Gaps against
  DESIGN.md are in `docs/DESIGN_BACKLOG.md`.

### F2 · Live station map (weeks 2–3)
- [x] Build step (Python, run once): TIGER land/water → simplified SVG paths per region, projected to each region's
      `viewBox`; station positions projected the same way → `web/geo/*.json` (see §0: projected in browser).
- [x] Map: SVG land + station markers coloured by state (`DESIGN.md §6`), region segmented control, `List` view
      (sortable table) as the text alternative, legend with counts per state.
- [x] Station tooltip/sheet: name, code, bikes (e-bikes), docks, state, "empty for 34 min", last reported.
- [x] Polls `live.json` every 60 s; markers update in place (no flash). Stale banner when `generated_at` > 5 min old.
- **Done when:** with the pipeline running, the map matches the public feed for 10 spot-checked stations.
- **Status 2026-10-08:** ✅ done. Live page with KPI tiles (incl. open alerts from `alerts.json`), map, list view,
  "Show only problems", details tooltip/sheet, keyboard roving focus, in-place refresh and stale banner;
  `tests/web/live-map.spec.js` 13/13 (Playwright + axe). Spot-check: 10/10 stations across all three views match
  the public feed for the same snapshot (DEVLOG 2026-10-08). The alerts rail is F3.

### F3 · Alerts feed (week 3)
- [x] Alert rail beside the map on desktop, below it on tablet, a bottom sheet on phones: open alerts first, newest
      resolved below; tapping an alert focuses the station on the map. *(2026-10-08: `web/js/live/alerts.js`; sticky
      rail ≥ 1120 px, card under the map 600–1119 px (two columns of rows from a 40rem container), peek bar + alerts
      sheet on phones; selecting a row switches region, focuses the marker and opens its details; new alerts are
      announced politely; in replay mode the rail shows a note instead. The rail re-reads `alerts.json` with every
      60 s `live.json` poll and the exporter writes it every 60 s, so an alert reaches the page within about 2 min of
      the exporter seeing it. The end-to-end "Done when" below, with the stream running, is checked by hand, not by
      the loop's automated checks.)*
- **Done when:** an alert raised by the stream appears within 2 minutes and resolves on its own.

### F4 · Insights (weeks 5–6) and F4b · Pipeline health (week 7)
- [ ] Insights: KPI tiles; "% of time empty" heatmap (station × hour); rebalancing-need table; trips vs availability
      scatter; each chart with a takeaway line and a source caption.
- [ ] Pipeline: status summary, latency p50/p95 line chart, events/min, consumer lag, freshness per table, GE results
      table, compaction before/after, lineage screenshot.
- **Done when:** every chart reads from a JSON export produced by the pipeline, none hand-made.

### F5 · Polish and launch (week 8)
- [ ] Playwright screenshots at 320/390/768/1024/1440/2560 px, both themes; axe; keyboard-only pass;
      reduced-motion pass; Lighthouse ≥ 95 performance and 100 accessibility on the Live page.
- [x] "Replay a day" mode for when the pipeline is off. *(2026-10-08: built early with the paused state:
      `python tasks.py replay-export` → `web/data/replay.json` from the raw archive, played at 10× from the
      paused banner; the file is refreshed by hand for now.)*
- **Done when:** checklist in `DESIGN.md §9` passes and the site is live on S3.

### Portfolio page (after F5)
New page `/projects/dockwatch/` in the portfolio repo per `DOCKWATCH_PLAN.md §9` — proposed to you before it is built.

---

## 5. Working rules
- **Every change** updates `CURRENT_STATE.md` (so it always matches the code) and adds a `DEVLOG.md` entry
  (so history is never lost, including things removed or abandoned).
- Tests live next to the milestone that needs them; a milestone isn't done with failing tests.
- No secrets in the repo; `.env.example` lists every variable with a safe default.
- Cost guardrails from the main plan (§8) apply from the first `terraform apply`.
