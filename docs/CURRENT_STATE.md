# DockWatch: current state

> **What this file is:** a description of DockWatch *exactly as it is right now* and what is being worked on.
> It is rewritten whenever the code changes. If something is removed from the code, it is removed from here too.
> For the full history, including what was tried and removed, see [`DEVLOG.md`](DEVLOG.md).
> For the plan, see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).

**Last updated:** 2026-10-09 · **Milestones:** M0 ✅ (except manual AWS steps) · M1 ✅ · M2 ✅ except
partition evolution (moved to M4) · F1 ✅ · F2 ✅ (plus F-1 / F-2 / touch-target fixes, the paused state and
44 px touch targets everywhere) · F3 alerts rail ✅ · F5 "Replay a day" ✅ · M6 CI (`ci.yml`) ✅ · M3 ✅ (CDC: Postgres ops database → Debezium → Kafka → Spark `MERGE INTO` Iceberg) · DESIGN.md v1.0.1

---

## Working on now

| What | Why | Status |
|---|---|---|
| Nothing in progress | M3 is done (see below). | Next: **M4**, batch, history and orchestration with Airflow (not started). |

Done most recently: **M3, CDC from the operational database** (see *Ops database* and *CDC* below).
- **Ops database** (`sql/ops/schema.sql`, idempotent): `stations`, `docks`, `vans`, `rebalancing_jobs`,
  `maintenance_tickets` in database `ops`, with foreign keys, `CHECK` constraints, `updated_at` triggers,
  `REPLICA IDENTITY FULL` and the publication `dockwatch_ops` on exactly those five tables. Migration
  `sql/ops/migrations/001_rebalancing_jobs_priority.sql` adds `rebalancing_jobs.priority` (no default).
- **Simulator** (`src/dockwatch/ops_sim/`): a pure model turns `gbfs.alerts`-shaped alerts and a simulated clock into
  operations (jobs opened after 20 min empty, closed when the alert resolves, cancelled and later deleted;
  maintenance tickets; station on/off). The seeded run (seed 42, 1500 simulated minutes, 80 stations) is
  deterministic: 3,049 change events, 22 job deletes.
- **Query plans** (`sql/ops/PLANS.md`, from `python tasks.py ops-explain`): the two most-called queries go from a
  Seq Scan (~15 ms and ~10 ms on 200,000 rows) to an index scan on a partial index (~0.02–0.04 ms).
- **CDC:** Debezium on Kafka Connect (on demand, profile `cdc`) streams the five tables to `ops.public.*`; the Spark
  job `cdc/apply.py` (service `cdc-apply`) applies them to Iceberg `lake.ops.*` with `MERGE INTO`, LSN-ordered,
  deletes included, idempotent on replay, adding new Postgres columns automatically. `python tasks.py cdc-e2e --seed
  42 --events 1500` runs it all in 83 s: Kafka has exactly the simulator's expected changes, and rows and checksums
  of all five tables match between Postgres and Iceberg, including the column added halfway (`priority`, 55
  non-null values on both sides). `cdc-catchup --replay` re-applies all 3,049 events and changes nothing.

Before that: **CI on GitHub Actions** (`.github/workflows/ci.yml`, five jobs: lint + host tests, Spark
transform tests, Terraform fmt + validate, the full Playwright suite with `--retries=0`, and a repeat job for the
alerts and shell specs; see *CI* below). New task `python tasks.py tf-fmt-check`; README CI badge.

Before that: **F3, the alerts rail** (`web/js/live/alerts.js`; see *Website* below).
- **Rail per breakpoint:** from 1120 px the Live page is two columns, the map (≈ 2/3) and a sticky alerts rail
  (≈ 1/3) whose rows scroll inside it. From 600 to 1119 px the rail is a card under the map; its body scrolls inside
  (`min(36rem, 70dvh)`), and a container query gives two columns of rows in the wide card. Below 600 px a **peek bar**
  fixed to the bottom ("61 open alerts") opens the **alerts sheet**, a bottom-sheet dialog holding the same list.
- **Rows:** open alerts longest-running first, then up to 20 recently resolved, each with a state marker, the name,
  "Empty for 34 min" / "Full, resolved after 22 min", and "San Francisco · started 3:32 PM". Selecting a row switches
  to the Map view and to the station's region if needed, focuses its marker and opens its details. Rows are keyed by
  `episode_id` and updated in place, so focus and scroll survive the 60 s refresh. New open alerts are announced
  politely ("New alert: … is empty." / "3 new alerts."), never on first load.
- **Replay mode:** the rail shows a note ("Alerts aren’t part of the replay. Exit the replay to see live alerts.")
  and the phone peek bar is hidden; the rows come back on Exit.
- **Read timeouts:** `alerts.json` is read with a 10 s timeout (then the rail's error message + Retry), and
  `replay.json` with 30 s (then "The replay couldn’t be loaded. Try again later." and the button works again).

Before that: **the paused state and Replay a day** (suggestions H1 and H3 of the run-2 review).
- **Paused banner:** keeps DESIGN §6's sentence ("Live data paused — showing the state at 3:42 PM.", with "on Oct 6"
  when the data is from an earlier Pacific day) and adds why and what next: "The DockWatch pipeline isn’t running
  right now, so no new station data is coming in; the map updates on its own when it restarts." The pill, the banner
  and the announcements use the one `freshnessState()` rule on the same 15 s tick (`dockwatch:freshness` event). The
  pill's "Data as of" also gains the date for an earlier day. Insights, Pipeline and About announce politely when
  live data pauses or comes back (never on first load); on Live the banner's `role="status"` does it.
- **Replay a day:** `python tasks.py replay-export` builds `web/data/replay.json` from the raw GBFS archive in
  SeaweedFS on the host (no Spark), with the shared `classify()` rule. The banner offers "Replay a day" when that file
  is published (one `HEAD` probe); replay mode plays the day at 10× speed through the normal map, legend, list, details
  and KPI code paths, with a neutral banner "Replay mode — showing Wednesday 7 Oct, 10× speed [Exit]" and a replay
  clock outside any live region. No pulses, cross-fades or per-frame announcements; the pill stays honest about live
  data; Exit returns to live data and focus.
- **44 px everywhere on touch:** KPI ⓘ buttons no longer shrink (were 41 px wide), icon buttons get back their
  rounded-off corners as hit area, and footer links (incl. the one inside the licence sentence) are 44 px tall on
  `pointer: coarse`. The touch test now scans every button and link on all four pages at 390 and 1280 px.
- **Follow-up:** the `live.json` read gives up after 10 s (no more "Loading…" for up to a minute on a stalled
  connection), the test server keeps connections alive, shell-test failures report what the page loaded, and the
  replay size budget is tested on a synthetic full day (80 changes per frame, 1.5 MB).

Before that: three fixes from the F2 review.
- **F-1 honest freshness:** the exporter's `generated_at` now means "time of the last export with new data". If
  `data_as_of` has not advanced since the last written `live.json` (also after an exporter restart: it reads the old
  file at start), it keeps the old `generated_at` and does not rewrite `live.json` / `alerts.json`. With Spark
  stopped the site therefore goes Delayed (2 min) → Paused (5 min) and shows the stale banner. The site's thresholds
  are unchanged.
- **F-2 list focus:** the List view updates its rows in place, keyed by station id, so the 60 s refresh keeps keyboard
  focus on the same station button and keeps the table's scroll position.
- **Touch targets:** on touch (`pointer: coarse`) the List view's sort buttons and station-name buttons are ≥ 44 px
  hit areas; DESIGN.md is now **v1.0.1** (an accessibility fix allowed by its change policy).

Before that: **F2**, the live station map on the Live page (see *Website* below): KPI tiles (Empty now ·
Full now · Open alerts from `alerts.json` · Bikes available), SVG map per region from `web/geo/`, markers by state,
legend with live counts, `Map | List` (sortable table), "Show only problems", station tooltip / phone bottom sheet,
keyboard roving focus, in-place refresh every 60 s, paused banner. The M2 spot-check passed: 10/10 stations match
the public feed for the same snapshot. Gaps against the frozen DESIGN.md are listed in `docs/DESIGN_BACKLOG.md`.

Before that: **F1**, the site shell: four pages, design tokens in both themes, self-hosted IBM Plex, freshness
pill, nav overlay.

Before that: the Iceberg catalog on Postgres is **verified** (`python tasks.py verify-lake` passes: every Kafka
offset in bronze exactly once, 0 gaps, 0 silver duplicates, fresh commits on all four tables), and Spark now runs
**on demand** (`catchup` / `stream`), never restarted by Docker. Details in DEVLOG 2026-10-08.

Next: M4 (batch, history and orchestration: Airflow), not started.

### Running right now on this machine
| Process | How it was started | Stop with |
|---|---|---|
| GBFS producer | `uv run python -m dockwatch.producer run` (log: `data/producer-run.log`) | Ctrl+C / kill the process |
| Exporter | `uv run python -m dockwatch.exporter` (log: `data/exporter.log`) | Ctrl+C / kill the process |
| Stream stack | `python tasks.py up stream` | `python tasks.py down` |
| Spark streaming job | **Not running** (on demand only). Container `status-stream` has `restart: "no"`. | — |
| Kafka Connect (Debezium) | **Not running** (on demand only: `python tasks.py connect-up` or `cdc-e2e`). Container `connect` has `restart: "no"`. | `python tasks.py connect-stop` |
| Spark CDC apply (`cdc-apply`) | **Not running**; started by `cdc-catchup` / `cdc-verify` / `cdc-reset` with `docker compose run`, exits when done. | — |

Spark on demand: `python tasks.py catchup` processes everything in Kafka since the last run (`availableNow`) and
exits (~30 s for a few minutes of backlog; Kafka keeps 7 days). `python tasks.py stream` keeps it running (one
micro-batch per minute) until `python tasks.py stream-stop`. Run one Spark JVM at a time: stop `stream` before
`catchup`, `verify-lake`, `inspect`, `sql` or `test-spark`. `live.json` only moves forward while Spark runs (the exporter
skips rewriting it while `data_as_of` stands still), so run `catchup` before looking at the exporter output.

Memory: the streaming job uses about 1.5–1.9 GB (limit 2 GB); the rest of the stack under 1 GB. The host often has
only ~1–3 GB free; the 24/7 job stalled there (driver heartbeat timeout), which is why it is now on demand.
Kafka Connect peaks at ~520 MiB (`mem_limit: 1g`, heap `-Xmx512m`) and the CDC apply at ~1.2 GB (`mem_limit: 2g`);
Connect and a Spark JVM never run together (the CDC tasks and `catchup` refuse).

---

## How data flows today

```
Bay Wheels GBFS 2.3 ──(producer, every 60 s)──► Kafka gbfs.station_status (6 partitions, key station_id)
        │                                     ├─► Kafka gbfs.station_information (compacted, only on change)
        │                                     └─► Kafka gbfs.dlq (invalid rows)
        └─► raw archive  s3://dockwatch/raw/gbfs/<feed>/dt=…/HH/<last_updated>.json.gz

gbfs.station_status ──(Spark status_stream, on demand: catchup = availableNow, or stream = 60 s micro-batches)──►
        bronze_status    → lake.bronze.station_status      (every Kafka record, offsets, raw JSON)
        silver_status    → lake.silver.station_status      (cleaned, state, de-duplicated)
        availability_5m  → lake.silver.availability_5m     (5-minute windows per station)
        episodes         → lake.silver.station_episodes    (MERGE INTO)
                         → Kafka gbfs.alerts               (raised / resolved)
                         → Kafka dockwatch.station_state   (compacted, latest per station)

dockwatch.station_state + gbfs.station_information + gbfs.alerts ──(exporter, every 60 s)──►
        web/data/live.json, web/data/alerts.json

raw archive (one Pacific day = two UTC dt= partitions) ──(python tasks.py replay-export, by hand, host only)──►
        web/data/replay.json   (Replay a day: one frame per minute, only changed stations after frame 0)

seeded synthetic alerts (or gbfs.alerts in live mode) ──(ops_sim, host, psycopg)──► Postgres ops (ops-db)
        stations, docks, vans, rebalancing_jobs, maintenance_tickets   (publication dockwatch_ops)

Postgres ops WAL ──(replication slot dockwatch_ops, pgoutput; Debezium on Kafka Connect, on demand)──►
        Kafka ops.public.<table>   (one topic per table, 1 partition, key = primary key, JSON with schemas)
ops.public.* ──(Spark cdc/apply.py in cdc-apply, on demand, availableNow; never beside Connect)──►
        lake.ops.<table>           (MERGE INTO per batch: newest event per key by LSN; deletes; new columns added)
cdc-verify: Postgres (host) vs Iceberg (Spark) rows + checksums per table → data/cdc/verify.json
```

---

## What exists right now

### Documents (`docs/`)
| File | What it is |
|---|---|
| `DOCKWATCH_PLAN.md` | The original project plan (what and why). Unchanged. |
| `IMPLEMENTATION_PLAN.md` | Milestones M0–M7 / F1–F5, decisions (incl. M2 decisions), progress checkboxes. |
| `DESIGN.md` | DockWatch design system **v1.0.1**, frozen until launch (F5); v1.0.1 = accessibility fix (44 px touch targets in the list table). |
| `DESIGN_BACKLOG.md` | Gaps and judgment calls found while building against the frozen DESIGN.md (14 from F1, 18 from F2, 3 from the F2 review, 9 from the paused state / Replay a day loop, 6 from F3: #46–#51); #35 (touch targets) is resolved. |
| `CONCEPTS.md` | Kafka, stream processing, Iceberg and change data capture explained through this codebase. |
| `reference/DESIGN_SOURCE_transitpulse.md` | The TransitPulse design file that was here before; reference only. |
| `CURRENT_STATE.md` / `DEVLOG.md` | This file / append-only history. |

### Python package `src/dockwatch/`
Host code runs on Python 3.12 (uv). Code under `streaming/` and `cdc/` also runs in the Spark image (Python 3.10), so it avoids 3.11+ features.

| Module | What it does | Why it's built this way |
|---|---|---|
| `config.py` | One `Settings` object from env / `.env` (prefix `DOCKWATCH_`): target, GBFS, Kafka topics, S3, Iceberg, checkpoints, trigger seconds and `trigger_mode` (`processing_time` / `available_now`), watermark, ops database / CDC (`ops_db_dsn`, `connect_url`, `cdc_namespace`, `cdc_topic_pattern`), state thresholds, exporter. | Switching to AWS is a config change. |
| `gbfs/models.py`, `gbfs/client.py` | Typed GBFS records (unknown fields kept); discovery → GBFS 2.3; re-discover on 404; custom User-Agent. | Feeds move and grow. |
| `producer/` | `messages.py` (explode per station, DLQ rejects, order-insensitive content hash), `archive.py` (gzip JSON to S3 or a folder), `kafka.py` (topic specs, idempotent publisher), `poller.py` (ttl-paced poll loop), `__main__.py` (`run`, `once`, `setup`, `replay`). | Keyed by station for per-station ordering; idempotent to avoid retry duplicates. |
| `streaming/episodes.py` | **Plain Python:** `Thresholds`, `classify()` (station state rule), `EpisodeState` + `advance()` (episode/alert state machine). | Unit-tested without Spark; the Spark rule is tested to give the same answers. |
| `streaming/transforms.py` | Spark: `parse_kafka()`, `with_state()`, `to_silver()`, `availability_windows()`. | Pure DataFrame functions, tested on tiny inputs. |
| `streaming/lake.py` | Spark/Iceberg session config (`local` = REST catalog + SeaweedFS, `aws` = Glue), table DDL, `migrate()` for schema evolution (`EVOLUTIONS` list). | Tables evolve in place; fresh and old installs share one history. |
| `streaming/run_mode.py` | `trigger_options()`: `processingTime` every `trigger_seconds`, or `availableNow`. No pyspark import. | Host-testable. |
| `streaming/status_stream.py` | The streaming job (four queries, see flow above). In `available_now` mode it waits for every query to drain Kafka, then exits (non-zero if a query failed). | One app keeps memory low; separate checkpoints recover independently; both modes share them. |
| `streaming/inspect_tables.py`, `streaming/sql.py` | Health/consistency report (counts, freshness, duplicates, Kafka-offset gaps, states, episodes, file sizes); `--check` (`verify-lake`) asserts it and exits 1 on failure (duplicates, offset gaps or missing partitions, silver > 5 min behind bronze, no Iceberg commit in 60 min). One-off SQL. | Proof for the restart test, the catalog fix and day-to-day checks. |
| `exporter/build.py`, `exporter/__main__.py` | Builds `live.json` (641 stations with name, code, lat/lon, region, map view, bikes/e-bikes/docks, state, state since, last reported; counts per view) and `alerts.json` (open alerts longest first, last 20 resolved). Atomic writes. `data_as_of` = newest feed snapshot in the export; `generated_at` = time of the last export whose `data_as_of` advanced (`carry_generated_at`, `has_new_data`). Exports without new data are not written; at start the previous `live.json` is read (`read_previous`), so a restart does not make old data look new. `alerts.json` gets the same `generated_at`. | The static site only reads these files, and its freshness pill reads `generated_at`, so it must not move when the data doesn't. Stations without a `region_id` are placed in a view by coordinates. Edge: with no previous `live.json` (first run), the first export is written with `generated_at` = now, so the site shows Live for up to 2 min on old Kafka data, then goes stale. |
| `exporter/replay.py` | `python tasks.py replay-export [--date YYYY-MM-DD] [--step 60] [--out web/data/replay.json]`: reads one **Pacific** day of `raw/gbfs/station_status` snapshots from the archive (`FsArchive` / `S3Archive`; both UTC `dt=` partitions, filtered by Pacific date), keeps the last snapshot per `step` bucket, computes each station's state with `classify()` / `capacity_of()` and its view with `view_for()`, and writes `replay.json` atomically (format v1: `stations` with name, code, lat/lon, view, capacity; `frames` of `[index, state, bikes, docks]` groups, frame 0 full, later frames only changed stations; busiest stations get the smallest indices). Station names come from the day's last `station_information` snapshot. Default day: the latest complete Pacific day with snapshots (today if there is none). Prints size, stations and frames; exits non-zero if the day has no snapshots. | Uses the same rule as Spark without a JVM, and the archive the producer already writes. Snapshot by snapshot: only each station's previous value is kept. |
| `geo/build.py`, `geo/__main__.py` | Map-shape build step: Census cartographic boundary counties (`cb_2023_us_county_500k`, downloaded once to `data/geo/`) → per-view projection, rectangle clip (land kept 320 units beyond the square view box for wide/tall map cards), Douglas–Peucker simplify → `web/geo/<view>.json`. `pyshp` is in uv group `geo`, imported lazily. | Run once; output committed. Stations are projected in the browser with the same params, so new stations need no rebuild. |
| `ops_sim/model.py` | **Pure** simulator rules: `Model.step(tick, now_ts, alerts)` → `Op`s (action, table, Debezium op letter c/u/d, values). An empty episode open ≥ 20 min with no unfinished job at the station opens a job (the first idle van by plate, else it waits); its resolved alert closes the job `done` with `bikes_moved` and frees the van; full alerts open nothing; unfinished jobs are cancelled with a seeded chance per minute and deleted 5–60 min later; tickets open on random docks (dock → `out_of_service`), go `in_progress`, close (dock → `ok`); stations are switched off / on now and then. After migration 001 every job insert / update carries `priority` (1–3). `parse_alert()` reads a `gbfs.alerts` message. | One seeded RNG and sorted iteration: the same seed and alerts always give the same operations, so the CDC run can be compared exactly. |
| `ops_sim/synthetic.py` | Seeded stations (`sim-0001`…, capacities 11–35), vans (`DW-001`…) and a `gbfs.alerts` stream on a simulated clock (tick = 1 min from 2026-10-01 00:00 UTC; alert after 15 min, resolved at the episode end, the M2 payload); `SeededSimulation` (setup ops, `ops_for(tick)`, migration at tick `events // 2`). | Replaces "an hour of live simulation" with a bounded, repeatable run. |
| `ops_sim/db.py` | psycopg executor: one transaction per simulated minute; the SQL as module constants; finds "the unfinished job at a station" (`OPEN_JOB_AT_STATION`) and "the unfinished ticket on a dock" (`OPEN_TICKET_ON_DOCK`) before each job / ticket write and raises `Conflict` if the database disagrees with the model; counts calls per constant and the change events Debezium should emit per table and op (from `rowcount`); `apply_schema()` (schema.sql + migrations). | Expected CDC counts come from what Postgres actually changed. |
| `ops_sim/explain.py`, `ops_sim/__main__.py` | `explain`: scratch database `ops_bench` (created and dropped on ops-db) → seeded run to count calls → reload with the two hot indexes dropped and 200,000 jobs / tickets (`setseed` + `generate_series`) → `EXPLAIN (ANALYZE, BUFFERS)` before / after re-creating each index from schema.sql → `sql/ops/PLANS.md`. CLI: `schema [--base-only]`, `seeded --seed --events [--stations]` (refuses non-empty tables), `live` (consumes `gbfs.alerts`, group `dockwatch-ops-sim`, 60 s tick; stations from `gbfs.station_information`), `explain [--out]`. | The `ops` database and `iceberg_catalog` are never touched by the bench. |
| `cdc/checksum.py` | Shared table checksum (Python 3.10-safe, for the host and the Spark image): canonical text per value (None `\N`, bool t/f, float `repr`, datetime UTC ISO µs with naive = UTC, Decimal normalised), sha256 per row (fields joined with `\x1f`), sha256 of the sorted row hashes; `_`-prefixed CDC columns dropped. | Postgres and Iceberg rows are compared with one function. |
| `cdc/guard.py` | `refusal(task, running)`: `cdc-catchup`, `cdc-reset`, `cdc-verify` (and `catchup`) refuse while Kafka Connect or `status-stream` runs, `connect-up` while `cdc-apply` or `status-stream` runs; `cdc_topics()`: a CDC reset may only delete `ops.public.*` and `dockwatch-connect-*`. Pure and unit-tested; `cdc/runner.py` injects a `docker ps` probe. | One heavy JVM at a time on this host. |
| `cdc/events.py` | **Pure** (Python 3.10): parses one Debezium envelope (JSON with schemas) into an `Event` (table from the topic, op, `source.lsn`, Kafka offset, primary key from the key schema, the row from `after`, or from `before` for a delete); maps Connect types to Iceberg (int16/int32 → int, int64 → bigint, double → double, boolean, string, `io.debezium.time.ZonedTimestamp` / `MicroTimestamp` → timestamp; anything else raises `SchemaError`); `latest_per_key()` by (lsn, offset); `merged_columns()` / `plan_evolution()` (add-only, a type change is refused); the CREATE / ADD COLUMN / MERGE SQL. | Envelope logic is unit-tested on the host with records captured from the real connector. |
| `cdc/apply.py` | Spark job (service `cdc-apply`): Kafka `subscribePattern` `ops\.public\..*`, earliest, `maxOffsetsPerTrigger` 1000, availableNow; `foreachBatch` (`make_batch_fn`) collects the batch, and per table creates `lake.ops.<table>` (format v2, unpartitioned), adds new columns, keeps the newest event per key and runs `MERGE INTO` (`WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd'` → DELETE, `WHEN MATCHED AND s._lsn > t._lsn` → UPDATE, `WHEN NOT MATCHED AND s._op <> 'd'` → INSERT). Metadata columns `_lsn`, `_op`, `_source_ts`, `_kafka_offset`, `_applied_ts`. Checkpoint `/checkpoints/cdc/ops`; `--replay` = fresh checkpoint `/checkpoints/cdc/replay-<ts>` from earliest, deleted afterwards; `--reset` = DROP TABLE `lake.ops.*` PURGE and delete `/checkpoints/cdc`. Prints `CDC_RESULT {json}`. | Stale or repeated events change nothing, so any replay converges to the same table. |
| `cdc/iceberg_checksums.py` | Spark: rows, columns and checksum of each `lake.ops.*` table over Postgres's column list (passed by the host), plus non-null counts of `rebalancing_jobs.priority`; prints `CDC_ICEBERG {json}`. | The Iceberg side of `cdc-verify`. |
| `cdc/runner.py`, `cdc/__main__.py` | Host CLI `python -m dockwatch.cdc`: `connect-up` (compose up `connect`, PUT `infra/connect/ops.json`, wait until the connector is RUNNING and the slot active), `connect-stop`, `reset`, `catchup [--replay]` (→ `data/cdc/catchup.json`), `verify` (→ `data/cdc/verify.json`, exit 1 on any mismatch), `e2e`, `kafka-counts`. Spark work runs with `docker compose --profile cdc-apply run --rm --no-deps cdc-apply …`; the output is echoed and the marker lines parsed. `e2e` records each step's time, the compose services running, and peak MiB per container (`docker stats` every ~3 s) in `data/cdc/e2e-report.json`. | One entry point for the tasks; the report is the evidence. |
| `alerts/handler.py` | `format_alert()` (plain English, Pacific time) and `lambda_handler()` (SNS if `DOCKWATCH_ALERTS_TOPIC_ARN` is set, else prints). | Ready for M6; not deployed. |

### Station state rule (one definition, used by Spark, Python and the site legend)
First match wins: `offline` (not installed, not renting, or no report for 30 min) → `empty` (0 bikes) → `full`
(0 docks) → `low` (≤ 2 bikes or ≤ 10 % of capacity) → `high` (≤ 2 docks or ≤ 10 %) → `ok`.
Capacity = bikes + docks, available + disabled. Episodes open on `empty`/`full`; an alert is raised after 15 min.

### Iceberg tables (catalog `lake`, warehouse `s3://dockwatch/warehouse/`, pointers in Postgres `iceberg_catalog`)
| Table | Partition | Grain | Notes |
|---|---|---|---|
| `bronze.station_status` | `days(snapshot_ts)` | one row per Kafka record | Kafka topic/partition/offset, raw JSON; evolved: `num_scooters_available`, `num_scooters_unavailable`. |
| `silver.station_status` | `days(snapshot_ts)` | one row per station per snapshot | `state`, `capacity`; write order `station_id, snapshot_ts`; evolved: `num_scooters_available`. |
| `silver.availability_5m` | `days(window_start)` | station × 5-minute window | samples, min/avg/max bikes, avg docks, share empty/full/offline; final ~10–15 min after the window. |
| `silver.station_episodes` | `days(start_ts)` | one row per episode | kind, start/end, duration, status open/closed, alerted; upserted with `MERGE INTO`. |
| `ops.stations`, `ops.docks`, `ops.vans`, `ops.rebalancing_jobs`, `ops.maintenance_tickets` | none | one row per Postgres row | Written only by `cdc/apply.py` (`MERGE INTO`, deletes included); the Postgres columns plus `_lsn`, `_op`, `_source_ts`, `_kafka_offset`, `_applied_ts`; format v2. `rebalancing_jobs.priority` was added by the apply when it first appeared in the change events. After the seeded run: 80 / 1,774 / 4 / 111 / 123 rows. |

### Ops database (Postgres `ops` on `ops-db`, `sql/ops/schema.sql`)
| Table | Key | Notes |
|---|---|---|
| `stations` | `station_id` text | name, short_name, lat / lon (`double precision`, range CHECKs), `capacity >= 0`, region_id, is_active |
| `docks` | `dock_id` identity | FK station; `dock_number > 0`; status `ok` / `out_of_service`; UNIQUE (station_id, dock_number) |
| `vans` | `van_id` identity | unique plate; capacity 1–40; status `idle` / `busy` / `maintenance` |
| `rebalancing_jobs` | `job_id` identity | FK station, nullable FK van; status `open` / `assigned` / `done` / `cancelled`; unique `alert_episode_id`; opened_at / closed_at (closed iff done or cancelled, never before opened); `bikes_moved >= 0` (required when done); `assigned` needs a van; `priority` smallint 1–3 from migration 001 (NULL on older rows) |
| `maintenance_tickets` | `ticket_id` identity | FK dock; issue `jammed` / `broken_lock` / `no_power` / `damaged` / `other`; status `open` / `in_progress` / `closed`; closed_at iff closed, never before opened |

All five have `created_at` / `updated_at` (`timestamptz`; the `set_updated_at()` BEFORE UPDATE trigger sets
`updated_at = now()`), `REPLICA IDENTITY FULL`, and are exactly the tables of publication `dockwatch_ops`. Hot-query
indexes (partial): `rebalancing_jobs_open_station_idx` (station_id WHERE status IN ('open', 'assigned')) and
`maintenance_tickets_open_dock_idx` (dock_id WHERE status <> 'closed'); plans in `sql/ops/PLANS.md`. The
replication slot `dockwatch_ops` is owned by the Debezium connector: it is created when the connector is registered
(`connect-up`) and dropped by `cdc-reset` / `cdc-e2e`; `ops-schema` owns only the publication. The tables hold the
seeded run's result (80 stations, 1,774 docks, 4 vans, 111 jobs, 123 tickets; `priority` set on 55 jobs) and the
slot exists, inactive while Connect is stopped.

### CDC (Debezium → Kafka → Iceberg)
- **Kafka Connect** (`connect`, profile `cdc`, `quay.io/debezium/connect:2.7.3.Final`): `mem_limit: 1g`,
  `KAFKA_HEAP_OPTS` / `HEAP_OPTS` `-Xms256m -Xmx512m` (effective `-Xmx512m`), `restart: "no"`, REST API on
  http://localhost:18083. Started only on demand.
- **Connector** `infra/connect/ops.json` (`ops-postgres`): pgoutput, database `ops`, the five `public.*` tables,
  publication and slot `dockwatch_ops` (`publication.autocreate.mode=disabled`), `snapshot.mode=initial`,
  `tombstones.on.delete=false`, `JsonConverter` with schemas for key and value, heartbeat every 10 s, topics with 1
  partition and replication factor 1.
- **Apply** (`cdc-apply`, see `cdc/apply.py` above) → `lake.ops.*`; **verify** compares rows, columns and the
  shared checksum per table and writes `data/cdc/verify.json`.
- **`python tasks.py cdc-e2e --seed 42 --events 1500`** (~85 s): preflight (stops a leftover Connect) → reset-spark →
  reset-pg-kafka (slot, tables re-created with the base schema, CDC topics) → connect-up → sim-a (ticks 0–749) →
  schema-change (migration 001) → sim-b (ticks 750–1499) → connect-catch-up (Kafka counts per table and op = the
  simulator's) → connect-stop → apply → verify → `data/cdc/e2e-report.json` (exit 0 only if everything matched).
  Last run: ok, 3,049 events, all five tables equal; peak `connect` 518 MiB, `cdc-apply` 1,170 MiB.

### Kafka topics (Redpanda)
| Topic | Partitions | Policy | Written by |
|---|---|---|---|
| `gbfs.station_status` | 6 | 7 days | producer (~641 msg/min) |
| `gbfs.station_information` | 1 | compacted | producer (only on change) |
| `gbfs.dlq` | 1 | 28 days | producer (0 rows so far) |
| `gbfs.station_status.replay` | 6 | 7 days | `producer replay` (unused so far) |
| `gbfs.alerts` | 1 | 28 days | Spark episodes query (key `alert_id`, at-least-once) |
| `dockwatch.station_state` | 1 | compacted | Spark episodes query (key `station_id`) |
| `ops.public.stations`, `ops.public.docks`, `ops.public.vans`, `ops.public.rebalancing_jobs`, `ops.public.maintenance_tickets` | 1 each | default (Debezium topic creation) | Debezium (key = primary key, Debezium envelope as JSON with schemas); deleted by `cdc-reset` |
| `dockwatch-connect-configs`, `dockwatch-connect-offsets` (25), `dockwatch-connect-status` (5) | 1 / 25 / 5 | compacted | Kafka Connect's own state; deleted by `cdc-reset` |
| `__debezium-heartbeat.ops` | 1 | default | Debezium heartbeats (every 10 s while Connect runs); not read by anything, kept by `cdc-reset` |

### Local stack (`docker-compose.yml`)
| Service | Profile | Port | Purpose |
|---|---|---|---|
| `redpanda` | stream | 19092, 18081 | Kafka API. |
| `console` | stream | 8088 | Redpanda Console. |
| `s3` (SeaweedFS 3.80) | stream | 8333 | Local S3; bucket `dockwatch`. |
| `iceberg-rest` | stream | 8181 | Iceberg REST catalog; its table pointers live in Postgres database `iceberg_catalog` on `ops-db`. |
| `ops-db` (Postgres 16) | stream | 5434 | Database `ops` (the M3 ops schema, see *Ops database*; `wal_level=logical`) and database `iceberg_catalog` (catalog). `infra/postgres/init/` creates the catalog DB on a fresh volume. |
| `connect` | cdc | 18083 | Kafka Connect + Debezium 2.7.3 (`quay.io/debezium/connect:2.7.3.Final`, same tag as `tasks.py` `CONNECT_IMAGE`); 1 GiB `mem_limit`, heap `-Xmx512m`, `restart: "no"`, healthcheck on `/connectors`; on demand only (`connect-up` / `cdc-e2e`). Its compose calls pass `--profile stream --profile cdc` (it depends on redpanda and ops-db). |
| `cdc-apply` | cdc-apply | — | The CDC Spark apply / verify / reset (`dockwatch-spark:3.5.5`, `local[1]`, driver 1g, `TZ=UTC`, `src/` read-only, checkpoints volume); 2 GiB `mem_limit`, `restart: "no"`; only started with `docker compose run` by the CDC tasks, not part of the `spark` profile. |
| `status-stream` | spark | 4040 (Spark UI) | The M2 streaming job; image `dockwatch-spark:3.5.5` (`infra/spark/Dockerfile`: Spark 3.5.5, Java 17, Iceberg 1.6.1 + AWS bundle, Kafka connector, pandas, pyarrow, pytest); `src/` mounted read-only; checkpoints in volume `dockwatch_checkpoints`; 2 GB memory limit; `restart: "no"` (on demand only). |

### Website (`web/`), static, no build step
| Path | What it is |
|---|---|
| `index.html` (Live), `insights.html`, `pipeline.html`, `about.html` | Shared shell: skip link, sticky 56 px nav (wordmark, links, freshness pill, theme toggle), full-screen nav overlay below 1120 px, footer with Bay Wheels attribution, "Not affiliated with Lyft or Bay Wheels", GitHub link, "Data through <date>". Live = title + freshness pill, paused banner (with "Replay a day" when `data/replay.json` exists) and the replay banner, KPI tiles, the map card at `#map` (F2) and the alerts rail `[data-alerts-rail]` beside it (F3), plus the phone peek bar after `.live-layout` and the alerts sheet `[data-alerts-sheet]`; Insights / Pipeline show "Arrives with M4 / M5" empty states; About has sources and licences. On touch (`pointer: coarse`) every button and link is a ≥ 44 × 44 px hit area (footer links 44 px tall; icon buttons keep their corners via a square `::after`); in-sentence links in About's text are the WCAG inline exception. |
| `css/tokens.css`, `base.css`, `layout.css`, `components.css`, `map.css` | Tokens from the DESIGN.md front matter (light default; dark via `prefers-color-scheme` or the toggle's `data-theme`); `@font-face`, reset, focus ring, reduced motion; page frame; components (incl. stat tile, segmented control, chip, banner, skeleton, data table with 44 px touch targets, details, bottom sheet, and the alerts rail / alert row / peek bar / alerts sheet with the alert row's container query); the map card, markers, tooltip and legend, and `.live-layout`'s two columns from 1120 px. |
| `js/main.js`, `insights.js`, `pipeline.js`, `about.js` | One ES-module entry per page; each calls `shell.js` (`theme.js`, `nav.js`, `freshness.js`, `util/time.js`). `freshness.js` is the page's only `live.json` poll (60 s; a read that hasn't answered in 10 s is aborted and counts as failed, so the pill says "Paused · No data yet" instead of staying on "Loading…", and Live shows its error with Retry) and broadcasts each result as a `dockwatch:live` event (or `dockwatch:live-error`); every pill render (15 s tick and each refresh) also broadcasts `dockwatch:freshness` with the state, so the Live banner flips with the pill. On pages without the banner it adds a visually hidden `aria-live="polite"` region that announces only transitions: "Live data paused — showing the state at 3:42 PM." and "Live data is back." "Data as of" carries the date when the data is from an earlier Pacific day. |
| `js/live/live.js` + `states.js`, `kpis.js`, `list.js`, `details.js`, `replay.js` | Live page controller: region (remembered in `localStorage`, mirrored in the URL with `view` and `problems`), `Map | List`, "Show only problems" (every state except `ok`), legend counts from `live.json` `counts[view]`, polite `aria-live` summary only when counts change, paused banner (`freshnessState(generated_at) === "paused"`, > 5 min) with the reason sentence and "Replay a day", skeleton / error (Retry) / empty states; KPI tiles; the sortable list table (rows updated in place by station id, so focus and scroll survive a refresh); tooltip (≥ 600 px) or bottom sheet (phones). `alerts.json` is read at start (independently of `live.json`) and re-read with each refresh, with a 10 s read timeout; an older read never overwrites a newer one. **Replay mode** (`replay.js`): `HEAD data/replay.json` once when the banner first shows (no button if it's missing); on click it fetches the file (30 s read timeout; then the banner says "The replay couldn’t be loaded. Try again later." and the button can be pressed again), decodes frames incrementally into `live.json`-shaped stations (`state_since` / `last_reported` null, "—" in the list, "Replay, state at 7:35 PM" in details) and steps one frame every `step_s × 1000 / 10` ms with `setInterval`, looping at the end. Focus moves to Exit, and back to "Replay a day" on Exit; Escape does not exit. KPI tiles show the frame's Empty / Full / Bikes, Open alerts "—" ("Alerts aren’t part of the replay") and a caption naming the replayed day. Live refreshes during replay update the pill only; the alerts rail shows its replay note. Not in the URL. |
| `js/live/alerts.js` | The alerts rail (F3): "Open now" (count badge, open alerts by `started`, longest-running first) and "Recently resolved" (by `resolved`, newest first, at most 20); stable sorts. Each row is a `button.alert-row` (`data-episode`, `data-alert-station`): a 12 px state marker, the name (`translate="no"`), "Empty for 34 min" (page clock, counting on with the 15 s tick, also while paused) or "Empty, resolved after 22 min", and "San Francisco · started 3:32 PM" (date added when not today, Pacific). Stations missing from `live.json` get a static `div`. Rows are keyed by `episode_id` and updated in place: text changes only when it differs, rows move only when out of place, a resolving alert moves to the resolved list as the same node, focus goes back to the same episode (or its neighbour) and the body's scroll is kept. States: skeleton rows + `aria-busy` while loading, "No open alerts right now.", "Alerts couldn’t be loaded. Check your connection, then try again." + Retry (only when there is no earlier copy), and the replay note. Desktop (≥ 1120 px): sticky rail under the nav, rows scroll inside, height capped to fit between the nav and the footer. Tablet (600–1119 px): card under the map, body scrolls inside, `@container (min-width: 40rem)` → two columns. Phone: the section isn't shown; the **peek bar** ("61 open alerts" / "1 open alert" / "No open alerts" / "Loading alerts…" / "Alerts couldn’t be loaded"; hidden in replay) opens the alerts sheet (`showModal()`, `is-scroll-locked`), into which the one list body moves; Escape / "Close alerts" return focus to the peek bar; the page gets bottom padding and `scroll-padding-bottom` for the bar. Selecting a row: closes the sheet on phones, switches List → Map, awaits `setRegion()` if the station is in another region, turns "Show only problems" off if needed, then focuses the marker and opens its details. A visually hidden `aria-live="polite"` region in the rail announces new open alerts ("New alert: … is empty." / "3 new alerts."), never on the first read or during replay. |
| `js/util/read.js` | `readJson(url, {timeoutMs})`: `fetch` + JSON with an `AbortController` read timeout (`setTimeout`, cleared in `finally` after the body, so the tests' fake clock drives it). Used by `loadAlerts()` (10 s) and `loadReplay()` (30 s); `freshness.js` keeps its own proven copy for `live.json`. |
| `js/map/map.js` | SVG map: land + landmarks, one `<circle>` per station keyed by id and updated in place (fill cross-fade + one pulse on a state change, none under reduced motion and none in replay mode), tier groups for draw order, screen-pixel sizes via `--u` / `--r-ok` from a `ResizeObserver`, nearest-station hit testing (10 px pointer, 22 px touch), roving tabindex with arrow keys. No wheel/pinch handling. |
| `fonts/` | IBM Plex Sans 400/500/600 + Mono 400/500, Latin `woff2`, and `OFL.txt`. |
| `licenses/lucide-LICENSE.txt` | Licence for the inline Lucide v1.52.0 icons (sun, moon, menu, x, info, triangle-alert, history, chevrons-up-down, arrow-up, arrow-down). |
| `data/replay.json` | Written by hand with `python tasks.py replay-export` (git-ignored, like `live.json`); read only when a visitor presses "Replay a day". The real build of 2026-10-07 (archive starts ~3 PM Pacific): 401,833 bytes, 641 stations, 492 one-minute frames; budget 1.5 MB per full day. |
| `data/live.json`, `data/alerts.json` | Written by the exporter (git-ignored). The freshness pill reads `generated_at` from `live.json` (time of the last export with new data): Live < 2 min, Delayed 2–5, Paused > 5. The map, legend, list and KPI tiles read the stations, `counts` and `bikes_available`; the Open alerts tile and the alerts rail read `alerts.json` (`open`, `resolved`). |
| `geo/sf.json`, `eastbay.json`, `sj.json` | Map shapes per region view from `python tasks.py geo` (committed): projection params, `0 0 1000 1000` view box, simplified land paths, 3–4 landmarks. Drawn by `js/map/map.js`. |
| `js/map/project.js` | lon/lat → view box with a view's projection; mirrors `dockwatch.geo.build.project` (stations are projected in the browser). |

Serve locally with `python -m http.server 5179 --bind 127.0.0.1 --directory web`.

### Infrastructure (`infra/terraform/`), validated, **not applied**
S3 bucket (private, encrypted, lifecycle), Glue databases `dockwatch_{bronze,silver,ops,marts,metrics}`, Athena workgroup
(1 GB scan limit), SNS topic, $1 / $5 budgets, IAM roles (spark-writer, athena-reader, alert-lambda, github-ci via OIDC).
Remote state backend commented out until the AWS account exists.

### Tooling and tests
- `tasks.py` (`Makefile` forwards): `lint`, `fmt`, `test`, `test-integration`, `test-spark`, `up [profile]`, `down`, `ps`,
  `setup`, `produce`, `produce-once`, `replay` (re-publish an archived day to Kafka), `stream`, `stream-logs`,
  `stream-stop`, `catchup`, `inspect`, `verify-lake`, `sql "<query>"`, `export`, `replay-export` (Replay a day file
  from the archive), `geo`, `ops-schema` (schema.sql + migrations on `ops`), `ops-sim` (`seeded --seed --events`,
  `live`), `ops-explain` (`--out`, default `sql/ops/PLANS.md`), `connect-up`, `connect-stop`, `cdc-reset`,
  `cdc-catchup [--replay]`, `cdc-verify`, `cdc-e2e [--seed --events --stations]`, `tf-fmt`, `tf-fmt-check`
  (`terraform fmt -check -recursive -diff`, for CI), `tf-validate`.
- **Host tests:** 69 pass, 2 skipped (`python tasks.py test`; the two skips are the `tests/spark` modules): ops simulator (`tests/test_ops_sim.py`, 7: same seed same ops, job only after 20 min empty, resolved alert closes the job, cancelled jobs deleted, full alerts open nothing, the seeded run touches every table with ≥ 10 job deletes, real `gbfs.alerts` messages parse), CDC (`tests/test_cdc.py`, 9: Connect schema → Iceberg types, a new column plans an ADD COLUMN, a type change is refused, the newest event per key wins by LSN, a delete takes its key from the before image — all on records captured from the real connector in `tests/fixtures/cdc/` — plus checksum order-free and canonical, the Connect / Spark guard, the reset topic list), GBFS client, messages/archive, poller, episodes/state rule, exporter (incl. `generated_at` kept without new data and across restarts),
  replay builder (`tests/test_replay.py`, 7 tests: full first frame then deltas, the shared `classify()` rule, a
  Pacific day across two UTC partitions, decoded frames equal the snapshots, a synthetic full day of 641 stations ×
  1440 frames at 80 changes per frame fits the 1.5 MB budget with the real writer (1,411,600 bytes), the CLI on an
  `FsArchive`, the default day), alerts, run mode, geo (projection, simplify, clip, fixture stations inside their view). **Spark tests:** 8 pass in the Spark image (`python tasks.py test-spark`; 4 transform tests and 4 CDC apply tests in `tests/spark/test_cdc_apply.py`: inserts / updates / deletes across batches, stale events ignored, replay changes nothing, the column evolved before the MERGE, on a hadoop Iceberg catalog in a temp dir); skipped on the host.
- **Web tests (Playwright 1.64.0 + axe, Chromium):** `npm ci` then `npx playwright test` (serves `web/` on port 5179
  with `tests/web/serve.py`, 2 workers; the server has a 128-connection backlog and HTTP/1.1 keep-alive, so the browser
  reuses a few sockets instead of opening one per file). `tests/web/shell.spec.js`: 11 pass (no horizontal scroll 320–2560 px, axe 0 violations at 320 / 768 /
  1440 px in both themes, skip link, theme toggle, nav overlay focus trap, freshness pill, pause / resume
  announcements on pages without the banner, pill and Live page when `live.json` never answers). `prepare()` also
  watches each page (`watchPage()`: failed and pending requests, page / console errors, whether `live.json` was
  requested and answered), and the shell suite's `open()` adds that summary to the error if the pill stays on
  "loading".
  `tests/web/live-map.spec.js`: 14 pass (one marker per station, region remembered, legend counts, sortable list,
  problems filter + empty state, details tooltip and phone sheet, keyboard roving focus, paused banner copy, in-place
  refresh, KPI tiles, error + Retry, axe with tooltip / list / sheet open, list keeps focus across a refresh).
  `tests/web/touch-targets.spec.js`: 3 pass (touch emulation, `pointer: coarse` asserted): the list test, plus every
  button, link, segmented label and summary on all four pages at 390 and 1280 px (paused banner, replay mode, list
  view with every sort and station button, phone sheet, nav overlay, Insights, Pipeline, About, focused skip link;
  the 81 alert rows at 1280 px, and at 390 px the peek bar plus every row and "Close alerts" in the open alerts sheet).
  Each is hit-tested at its centre, ±21 px and the corners of a 44 × 44 px square; only About's in-sentence links
  and map markers (22 px nearest-station radius) are exempt, and the test checks the exemptions.
  `tests/web/replay.spec.js`: 8 pass (banner says why and offers Replay a day; names the day for older data; no button
  without a file; plays frames 0 and 1 of the fixture at 10×; Exit restores live data and focus; no announcement per
  frame; axe on both banners at 390 / 1440 px, light and dark; `replay.json` that never answers ends in the message
  after 30 s).
  `tests/web/alerts.spec.js`: 13 pass (open alerts longest-running first then up to 20 resolved; row copy, start line
  and durations moving with the clock; sticky rail beside the map at 1440 px with rows scrolling inside; card under
  the map with two columns at 900 px and one at 650 px; phone peek bar that doesn't cover the footer and opens the
  alerts sheet; selecting a row switches region, focuses the marker and opens the tooltip, or the station sheet on a
  phone; focus stays on the same alert across refreshes, also when it resolves; polite announcements, none on first
  load; replay note and hidden peek bar; 503 + Retry; a read that never answers ends in the error after 10 s; axe on
  the rail, card, peek bar and sheet in both themes). 49 Playwright tests in total. Tests route
  `data/*.json` to `tests/web/fixtures/` (a real 641-station export, and `replay.json` built from the real archive
  with `--step 600`, 51 frames) and fake time with `page.clock`; `data/replay.json` is a 404 unless a test asks for
  the fixture.
- **Ops DB integration tests** (`tests/test_ops_db.py`, marked `integration`, not in CI): 5 pass after `python tasks.py ops-schema` (foreign keys, CHECK constraints, `updated_at` trigger, REPLICA IDENTITY FULL, the publication's five tables); each test's transaction is rolled back, so they leave no rows and no CDC events.
- `inspect`, `verify-lake` and `sql` run Spark with whole-stage codegen **off** (a JVM crash otherwise; see DEVLOG).
- Lint: ruff, line length 120.

### CI (`.github/workflows/ci.yml`, GitHub Actions)
Runs on pushes to `main`, on pull requests and by hand (`workflow_dispatch`). Top-level `permissions: contents: read`;
a newer run on the same ref cancels the older one (`concurrency: ci-<ref>`). Every job runs on `ubuntu-24.04`; actions
are pinned to major tags; no matrix, so the check names stay fixed. Five jobs:

| Job | Runs | Timeout |
|---|---|---|
| `Python lint + tests` | `astral-sh/setup-uv` (uv 0.12.11, Python 3.12), `uv sync --locked`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest -q` (the local commands) | 10 min |
| `Spark transform tests` | `docker build -t dockwatch-spark:3.5.5 infra/spark`, then `python tasks.py test-spark` (same image and command as locally) | 20 min |
| `Terraform fmt + validate` | `python tasks.py tf-fmt-check`, `python tasks.py tf-validate` (both in `hashicorp/terraform:1.9`, `init -backend=false`, no AWS credentials) | 10 min |
| `Playwright suite` | Node 20, `npm ci`, `npx playwright install --with-deps chromium`, `npx playwright test --retries=0` (all 49 tests); uploads `test-results/` (traces) on failure | 30 min |
| `Playwright repeat (alerts + shell)` | the same setup, then `npx playwright test tests/web/alerts.spec.js tests/web/shell.spec.js --repeat-each 3 --retries=0`, as its own red check if one of them flakes | 30 min |

- **Retries 0:** `playwright.config.js` says `retries: CI ? 1 : 0`, so on GitHub (`CI=true`) a flaky test would pass
  on its second try and stay hidden. Both web jobs pass `--retries=0` on the command line, which overrides the config;
  the config itself is unchanged. `CI=true` still gives `forbidOnly`, the line reporter, a fresh test server and 2
  workers.
- **Caching:** uv's cache (`setup-uv` `enable-cache`), npm's cache (`setup-node` `cache: npm`) and the Playwright
  browsers (`actions/cache` on `~/.cache/ms-playwright`, keyed on `package-lock.json`). The Spark image is rebuilt
  each run (no Docker layer cache).
- **Verified locally** (nothing is pushed during a loop): actionlint 1.7.7 (with shellcheck) is clean; the run steps
  of the `python`, `web` and `web-repeat` jobs pass verbatim in Linux containers (`python:3.12-slim-bookworm` and
  `node:20-bookworm`, `CI=true`) on a copy of the working tree; the `spark` and `terraform` commands are the local
  `test-spark` / `tf-fmt-check` / `tf-validate` checks.
- First run on GitHub: **passed** — run 37875703602 on commit 9a42ba4 (2026-10-09 02:40–02:44 UTC), all five jobs green: Python lint + tests (12 s), Terraform fmt + validate (18 s), Spark transform tests (56 s), Playwright suite (2 min 28 s), Playwright repeat (alerts + shell) (3 min 22 s). https://github.com/braaaeeedyn/dockwatch/actions/runs/37875703602
- **Not in CI:** `deploy.yml` (publishing `web/` to S3 needs the AWS account), `terraform plan` (needs the AWS
  account and the GitHub OIDC role), `dbt compile` (arrives with M5), the integration and network tests, the Kafka
  stack and `verify-lake`.

---

## Known gaps and facts to remember
- Git: commits are made at the end of each loop, not by tooling during it (latest at the start of this loop:
  `1474d1e`, the F3 alerts rail).
- **Node 20** reached end of life in April 2026. CI uses Node 20 to match this machine (v20.20.2); moving local and
  CI to Node 22/24 together is a follow-up.
- An unused Docker volume `dockwatch_iceberg-catalog` (old SQLite catalog) still exists; backup copy at
  `data/iceberg_catalog_backup.db`. The Postgres catalog is verified, so the volume can be deleted (left to the user).
- **Alerts reach the page within about 2 min** of the exporter seeing them (the exporter writes `alerts.json` every
  60 s and the rail re-reads it with every 60 s `live.json` poll). The end-to-end check with the stream running (an
  alert raised by Spark appears and resolves on its own) is done by hand, not by the automated checks.
- The alerts rail is not filtered by region and has no history in replay mode (`replay.json` v1 has no episodes;
  DESIGN_BACKLOG #46).
- **Replay file refresh is manual:** `web/data/replay.json` changes only when someone runs
  `python tasks.py replay-export` (no schedule, not published to AWS yet). The archive starts on 2026-10-07 ~3 PM
  Pacific, so the default day (Oct 7) is partial; the replay starts at its first archived frame. A day at 10× takes
  2.4 h; there is no pause, scrubber or speed choice (Exit is the stop control).
- **Replay size budget:** 1.5 MB uncompressed per full day (DESIGN_BACKLOG #45). The test's synthetic day uses 80
  changes per frame (1.5× the real mean of ~53 per minute) and is ~6% under; 100 per frame would be ~1.74 MB, so the
  budget holds up to ~84. A full real day is ~1 MB. If real days approach the cap: a larger default `--step`, or a
  compact v2 encoding / gzip.
- **Shell-suite flake (not reproduced):** once, after the Spark-in-Docker checks, the nav pill stayed "loading" for
  5 s in `shell.spec.js`. Not reproduced since in ~180 runs after `test-spark` (see DEVLOG). Of the two likely causes,
  the live.json read timeout removes cause (b), a first read that never settles; the keep-alive test server reduces
  (does not remove) cause (a), a module request failing under load; a recurrence now names its cause.
- The paused state can't tell "Spark stopped" from "the whole laptop is off": both mean "the pipeline isn't running"
  to a visitor. An exporter heartbeat would need a supervised exporter (out of scope).
- Docker Compose warns that volume `dockwatch_checkpoints` "already exists but was not created by Docker Compose";
  harmless.
- **CDC is checked with a bounded, seeded run** (`cdc-e2e`, 1500 simulated minutes), not an hour of live
  simulation. The simulator's `live` mode needs the Spark stream running to get `gbfs.alerts`, and is not part of
  any check.
- **CDC is not in CI** (Postgres, Kafka Connect and the e2e run need the local stack); its host unit tests run in the
  `python` job and the Spark apply tests in the `spark` job.
- **Kafka Connect memory:** `mem_limit: 1g` with `KAFKA_HEAP_OPTS` = `-Xms256m -Xmx512m` (the image's
  entrypoint copies `HEAP_OPTS` into `KAFKA_HEAP_OPTS`; with neither set, `connect-distributed.sh` would use
  `-Xmx2G`, more than the 1 GiB limit); measured peak ~520 MiB. The apply peaks at ~1.2 GB of its
  2 GiB. Never start Connect while a Spark JVM runs (the tasks refuse) or vice versa.
- **The replication slot holds WAL while Connect is stopped:** after `cdc-e2e` the slot `dockwatch_ops` stays
  (inactive) so a later `connect-up` resumes where it stopped; WAL is cluster-wide, so it also keeps the
  `iceberg_catalog` database's WAL (~1.3 MB right after the run). `cdc-reset` drops it; there is no
  `max_slot_wal_keep_size` cap on ops-db.
- **Hard deletes and replay:** a replay that splits a deleted row's insert and delete into different batches
  re-inserts the row for one batch, then deletes it again; the end state is identical (checked by `cdc-verify`).
- **Type changes are not applied:** a changed Postgres column type, or a Connect type the apply does not know (e.g.
  `numeric`, `date`), stops the apply with `SchemaError`. Columns are add-only; a dropped Postgres column would stay
  in Iceberg (and `cdc-verify` would still compare only Postgres's columns).
- **Partition evolution** not done (moved to M4 with compaction).
- **Small files:** each streaming query writes ≥ 1 file per minute (silver ≈ 45 KB/file). Compaction arrives in M4.
- `live.json` is ~200 KB uncompressed; fine for now, worth trimming or gzip-serving later (F5).
- **AWS steps are manual** and not done; `checkpoint_root` on AWS will need `s3a://` + hadoop-aws jars (not in the image yet).
- Some stations report a `last_reported` weeks old → `offline`. 35 stations have no `region_id` → placed by coordinates.
- `station_information` rows arrive in a different order on every poll (the content hash sorts them).
- Bay Wheels currently reports 0 scooters at every station; the evolved scooter columns are 0 for new rows, null for old.
