# DockWatch: current state

> **What this file is:** a description of DockWatch *exactly as it is right now* and what is being worked on.
> It is rewritten whenever the code changes. If something is removed from the code, it is removed from here too.
> For the full history, including what was tried and removed, see [`DEVLOG.md`](DEVLOG.md).
> For the plan, see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).

**Last updated:** 2026-10-19 · **Milestones:** M0 ✅ (except manual AWS steps) · M1 ✅ · M2 ✅ (partition
evolution done in M4a, on a lab copy) · F1 ✅ · F2 ✅ (plus F-1 / F-2 / touch-target fixes, the paused state and
44 px touch targets everywhere) · F3 alerts rail ✅ · **F4 Insights ✅** (Loop K: insights format 2 and the Insights
page) · **F4b Pipeline ✅** (Loop N: with the Great Expectations results and the full lineage card) · **M5 pipeline metrics ✅**
(`pipeline_health` → `metrics.pipeline_minutely` + `health.json`) · **M5 Great Expectations ✅** (Loop L: suite
`silver_station_status` inside `marts_daily`'s quality gate) · **M5 OpenLineage ✅** (Loop M: the Airflow batch DAGs; Loop N: the Spark listener in `catchup`; both
replayed into Marquez on demand) · F5 "Replay a day" ✅, launch tests ✅ (no Lighthouse) · M6 CI (`ci.yml`, on Node 24, with an offline `dbt compile`) ✅ · M3 ✅ (CDC: Postgres ops database → Debezium → Kafka → Spark `MERGE INTO` Iceberg; hardened: WAL cap, lost-slot check, CDC topic retention) · **M4a ✅** (Airflow 2.11.2 + Trino 483 in the `batch` profile:
`iceberg_maintenance`, `partition_evolution_demo`, `cdc_slot_keepalive`, retries / alerts, path-casing guard) · **M4b ✅**
(`trip_history_monthly` + a 12-month backfill into `bronze.trips` with pyiceberg, the dbt project on Trino,
`marts_daily` → `web/data/insights/*.json`) · **Local measurements in `docs/METRICS.md`** (Loop O, each number with its command; the Athena row waits for AWS) · DESIGN.md v1.0.1

---

## Working on now

| What | Why | Status |
|---|---|---|
| **A local Lighthouse run (Loop P):** F5's Lighthouse on the Live page from a local static server (accessibility 100 as the check, performance as a measured metric), fixing what it finds. | F5's last local item; the deployed-site run stays with the user. | Next. |
| **Needs the user:** the AWS account (M6: S3 + Glue, Athena, Lambda + SNS, `terraform plan` in CI, `deploy.yml`, the Athena row of `docs/METRICS.md`), pushing Loop K, Loop L, Loop M, Loop N and Loop O and checking their GitHub runs, Lighthouse on the deployed site, the human-eye checks (greyscale, a look at the 48 launch screenshots, the live alert timing, how the expectations card, the dark Marquez screenshot of the full graph (also `docs/img/lineage.png`) and the Spark job list in the lineage card look), the design calls in DESIGN_BACKLOG (#58 latency scale, #62 heatmap minimum coverage, #63 / #65 the lineage card, #64 the nav overlay's fade), optional approvals (renaming `web/img/lineage-batch.png`, which a frozen test pins; Spark lineage from `stream` too), deleting the 1.8 GB WSL crash dump of Loop M's crashed OpenLineage child and Loop O's lab data (`lake.lab_bronze.*` / `lake.lab_silver.*`, `gbfs.alerts.lab`, `dockwatch.station_state.lab`, the replayed records in `gbfs.station_status.replay`, `/checkpoints/lab/`), optionally a shorter trigger for lower latency (a design call) | Accounts, approvals and judgment only the user can give. | Waiting for the user. |

Done most recently: **Loop O: local measurements in `docs/METRICS.md`, each with its command, and the lab mode
of the streaming job** (see *Running right now*, *Iceberg tables*, *Kafka topics* and *Tooling and tests* below).
- **Latency, an hour of `stream`** (`measure window`, 63 min, 60 micro-batches): fetch to silver p50 / p95 59.3 s /
  60.9 s, fetch to queryable (the committed snapshot) p50 / p95 64.1 s / 68.7 s, micro-batch p95 9.6 s, 647
  events/min, fetch to Kafka p50 0.3 s (the clock check). The producer's 60 s poll and the 60 s trigger were in
  phase, so each snapshot waited almost a full trigger.
- **Throughput, the archived day 2026-10-16 replayed into lab tables:** 10× (its first 240 snapshots) 6,294
  events/min, batch max 9.7 s; 50× (all 782) 30,757 events/min, batch max 11.2 s; both sustained within the 60 s
  trigger, fetch to silver p95 58.7 s / 56.9 s, achieved speeds 9.81× / 48.0×.
- **Compaction** (`measure files` on fresh Trino JVMs around `iceberg_maintenance`): silver 419 → 11 files, the
  station query's median 287 → 163 ms; bronze 79 → 11 files.
- **Crash-restart** (`python tasks.py crash-restart`): SIGKILL with a silver micro-batch in flight, then catchup and
  `verify-lake`: 0 duplicates, 0 missing offsets.
- **The lab mode:** `stream-lab` / `stream-lab-stop` run the same job on `lake.lab_bronze` / `lake.lab_silver`, the
  `.lab` topics and `/checkpoints/lab/<UTC>` (Settings `namespace_prefix`, `starting_offsets`; `run_mode.table()`,
  `run_mode.lab_refusal()`); `replay` paces by schedule with `--skip` / `--limit`; `measure window` / `measure files`
  read Trino over HTTP; `stream` now refuses while any status-stream container runs.
- The README has a Results section; the Athena bytes-scanned row waits for the AWS account. `tests/test_measure.py`
  (11 host tests).

Before that: **Loop N: the OpenLineage Spark listener in catch-up runs, one naming with the Airflow
lineage, and the full graph from `gbfs.station_status` to every mart** (see *Lineage*, *Local stack* and *Website*
below). Its first Checker run stopped on two host-memory gates (see *Known gaps*), and Loop N2 restored the tree
byte-exact and re-verified it.
- **openlineage-spark 1.54.0** is a sha1-checked layer of the Spark image in `/opt/spark/jars` (the tag stays
  `dockwatch-spark:3.5.5`). The listener runs **only in `catchup`**: `tasks.py catchup` sets
  `DOCKWATCH_SPARK_LINEAGE_DIR` and mounts `data/lineage` with `compose run -v`, and each catch-up writes one
  `data/lineage/spark-dockwatch-status-stream-<UTC>.jsonl` in job namespace `dockwatch-spark`. `stream`,
  `verify-lake`, `inspect`, `sql` and the CDC apply emit nothing.
- **One naming:** `lineage-replay` renames each Iceberg table in Spark's events to its Trino name through its
  REST-catalog symlink, so Spark's outputs are the datasets dbt reads, and posts the newest catch-up with lineage
  after the Airflow runs (only runs that read and write; each streaming `write_to_data_source_v2` named by its table).
- **The full graph:** `lineage-shot` takes `marts_daily.dbt_build` at depth 3 in Marquez (Kafka through Spark and dbt
  to the marts, the gate and both exports) into `web/img/lineage-batch.png` and `docs/img/lineage.png`. The Lineage
  card's alt text, caption and takeaway describe it, a list of the Spark jobs follows the batch table, and the
  table's job names stay on one line (the house 16rem cap on row headers broke them mid-word).
- **Leftover Marquez containers block like the API:** a `marquez-db` or `marquez-web` left running makes the Trino /
  Spark / Connect / keepalive tasks refuse too.
- **Measured** (Loop N's Builder runs, all 223 checks passed): the catch-up JVM with the listener peaked at 980,
  1,227 and 1,283 MiB of 2,048 in three catch-ups (archived catch-ups without it: 1,129–1,414 MiB); a catch-up wrote
  56–57 events / 272,647–275,004 B, of which the replay posts 27; Marquez api 242–275 MiB, db 69–71 MiB, web
  20–29 MiB; the shot's graph 33 nodes; the PNG 156,889 B. 91 task runs, 0 incomplete, 0 new crash dumps.
- `tests/test_spark_lineage.py` (7 host tests on a trimmed real catch-up file) and
  `tests/web/pipeline-lineage-full.spec.js` (3 Playwright tests).

Before that: **Loop M: OpenLineage from Airflow, replayed into Marquez on demand, and the batch lineage card
on the Pipeline page**, finished in **Loop M2** with two root-cause fixes and restored and verified in **Loop M3**
(see *Lineage*, *Local stack* and *Website* below).
- **OpenLineage on in Airflow** (provider 2.11.0, openlineage-python 1.44.1, already in the image) with the **file
  transport**: every Airflow run writes one JSON file per event to `data/lineage/ol-<time>.json`, while Marquez is
  down. `batch-up` and `dag-test` create `data/lineage` first (the transport never creates it).
- **Inlets / outlets** (OpenLineage `Dataset`s from `batch/lineage.py`) on the 8 batch tasks of
  `trip_history_monthly`, `marts_daily` and `pipeline_health`; `marts_daily.quality_gate` reads
  `lake.silver.station_status` (the Great Expectations batch) besides the three marts. dbt's lineage is carried by
  `marts_daily.dbt_build` (every dbt source in, the marts out): openlineage-dbt would turn the ephemeral staging
  models into phantom datasets.
- **Marquez on demand** (compose profile `obs`: `marquezproject/marquez:0.51.1`, `marquez-web:0.51.1` and a
  `postgres:16-alpine` db, no volume, so it always starts empty), refused beside Trino, Spark or Connect and the other
  way round. `lineage-up`, `lineage-replay` (the newest run of each DAG), `lineage-shot`, `lineage-stop`.
- **The Pipeline page's Lineage card:** the Marquez screenshot `web/img/lineage-batch.png` and the same graph as a
  table (its text alternative).
- **Forked OpenLineage emitters are safe** (Loop M2): the provider sends every task event from an `os.fork()` of the
  Airflow process, and a GC pass in such a child once freed an inherited pyarrow `S3FileSystem` (SIGSEGV, a lost
  event). `batch/lineage.py` registers `os.register_at_fork(before=gc.freeze, after_in_parent=gc.unfreeze)`.
- **The nav overlay fades in** (Loop M2): `nav-menu-in` is opacity only (still 320 ms). The old 8 px slide from above
  put the top row's touch targets partly off-screen for its first ~25 ms, which the touch-target test could catch.
- **Measured** (Loop M2's Builder run of all 205 checks, OpenLineage on): 91 task runs from the first dag-test on
  (the 12-month backfill included), every one with a START and a COMPLETE, and no new crash dump; `marts_daily`
  47.3 s, `airflow-cli` 617 MiB, Trino 1,733 MiB; `pipeline_health` 27.8 s, 379 MiB; `trip_history_monthly` 545 MiB;
  the backfill 725 MiB. Marquez: api 275 MiB, db 71 MiB, web 34 MiB. The newest runs of the six DAGs: 84 events
  (`data/lineage`: 452 files, 3.2 MB).
- `tests/test_lineage.py` (6 host tests), `tests/airflow/test_loopm_lineage.py` (4 in the Airflow image) and
  `tests/web/pipeline-lineage.spec.js` (4 Playwright tests).

Before that: **Loop L: Great Expectations inside `marts_daily`'s quality gate, its results on the Pipeline
page, a readable latency chart and a fresh Trino JVM for `marts_daily`** (see *Great Expectations*, *Pipeline
health* and *Website* below).
- **Great Expectations 1.24.0** in its own pinned venv `/opt/gx-venv` in the Airflow image (41 packages,
  `infra/airflow/gx-requirements.txt`; SQLAlchemy 2.1.4, pandas 3.0.6, numpy 2.5.4, while Airflow keeps SQLAlchemy
  1.4.54; `GX_ANALYTICS_ENABLED=False`). The image grew from 3.28 to 3.86 GB and rebuilds when the file changes.
- **Suite `silver_station_status`** (10 expectations: schema, row count, not-null keys, the six states, no negative
  counts, bikes + docks within capacity, uniqueness of station × snapshot, and freshness) on the newest 24 hours of
  `lake.silver.station_status`, run as the checkpoint `marts_daily_quality` inside the `quality_gate` task. Blocking
  failures stop the insights export; freshness only warns. First real run: 382,143 rows, 9 passed, freshness a
  warning (the newest row was 32 min old: Spark runs on demand), 2.7 s; in Loop L's Checker run 382,254 rows,
  3.2 s.
- **Results:** `data/quality/expectations.json`, an `expectations` summary in `data/dbt/quality_gate.json`, the whole
  document in `web/data/health.json` (`quality.expectations`) and a new **Great Expectations on silver** card on the
  Pipeline page.
- **Latency readability:** the latency chart's y axis reads in one duration unit (`30 s`, `20 min`, `12 h`) under
  the title "Latency", and its left margin is measured from the widest label, so labels never overlap the title; the
  p95 tile's sub-line starts with the p95 as a duration ("21 h 4 min · fetch to silver, …").
- **Fresh Trino for `marts_daily`:** the checks restart Trino (`batch-stop` then `batch-up`) right before
  `marts_daily`; it peaked at 1,694–1,768 MiB in the DAG, up to 1,885 MiB with the checks' queries, with Metaspace at
  about 180 / 185 MiB used / committed after the marts.
- `tests/test_expectations.py` (6 host tests), `tests/airflow/test_loopl_gx.py` (3 Airflow-image tests) and
  `tests/web/pipeline-quality.spec.js` (5 Playwright tests).

Before that: **Loop K: insights format 2, the Insights and Pipeline pages, pipeline health, maintenance of
the M4b tables, `dbt compile` in CI and the launch tests** (see *dbt and marts*, *Pipeline health*, *Iceberg
maintenance* and *Website* below).
- **Insights format 2:** each `web/data/insights/*.json` file lists its stations once (`stations`, a lookup) and its
  per-file constants once (`window`); a row starts with the station's index. On the same data the files went from
  973,010 / 79,694 / 1,886,746 B to 272,808 / 66,768 / 514,417 B. At 1,000 stations and 30 dates the daily file is
  1.67 MB with every float at full 3 digits (host test), far from the 2,000,000-byte budget it would have hit at ~18
  dates.
- **The Insights page** (`insights.html`, F4): KPI tiles (time empty, time full, worst hour, trips per day), a
  "% of time empty" heatmap of the 25 stations empty most often by Pacific hour, the 20 stations that needed the most
  rebalancing in the last 7 days, and a trips vs availability scatter, with skeletons, a "not published yet" state
  and a 10 s read timeout with Retry. `tests/web/insights.spec.js`: 8 tests on a real export.
- **`iceberg_maintenance` covers 16 tables:** the 9 core tables plus `bronze.trips`, `bronze.station_information`,
  `metrics.trip_loads`, the three marts and `metrics.pipeline_minutely` (`skipped` while it does not exist).
  `bronze.trips` is optimized one month partition per run: one optimize of the whole table (64 → 13 files, 230 MB)
  took Trino to 2,024 MiB of its 2 GiB.
- **`dbt compile` in CI:** `python tasks.py dbt-compile` compiles the 7 models and 28 tests offline
  (`--no-populate-cache`, no Trino), and the CI `Python lint + tests` job runs it after the host tests.
- **Pipeline health (M5 metrics):** the hourly DAG `pipeline_health` writes the 1,440 minutes before each run to
  `lake.metrics.pipeline_minutely` (events, processed, p50 / p95 fetch-to-silver latency, consumer lag) and
  `web/data/health.json` (overall status, tiles, the minute series, table freshness, the dbt tests and quality gate,
  the latest compaction). Last run: success in 20 s, health.json 78,862 B, Trino 1,374 MiB.
- **The Pipeline page** (`pipeline.html`, F4b): a status banner with icon + word,
  four stat tiles, latency and events-per-minute charts, the freshness table, the dbt tests with the quality gate,
  and files before / after the latest compaction. `tests/web/pipeline.spec.js`: 9 tests on a real export.
- **Launch tests** (`tests/web/launch.spec.js`, F5 without Lighthouse): 48 full-page screenshots (4 pages × 6
  widths × 2 themes) with no horizontal scroll, keyboard-only Tab order equal to reading order on every page with
  the nav overlay trapping and returning focus, and nothing animating under reduced motion.

Before that: **M4b, trip history, dbt marts and insights** (see *Batch and orchestration* and *dbt and marts*
below).
- **`trip_history_monthly`:** one Bay Wheels trip month per run, both file schemas (legacy 2019-01: 192,082 rows; ride
  2025-10 to 2026-09). It is written to `lake.bronze.trips` by **pyiceberg** in `airflow-cli`, in one commit per
  month, and every attempt is recorded in `lake.metrics.trip_loads`. The 12-month backfill (`dags backfill -s
  2025-10-01 -e 2026-09-01`) loaded **5,789,432 rows** in 131 s of loading, and a rerun of 2026-09 replaces the
  month.
- **dbt** (dbt-core 1.10.23 + dbt-trino 1.10.6 in `/opt/dbt-venv`): three marts and 28 tests on Trino.
- **`marts_daily`:** stations → dbt build → quality gate (`data/dbt/quality_gate.json`) → the three
  `web/data/insights/*.json` files plus `manifest.json`.
- **Trino Metaspace cap raised 192M → 256M** (user decision). M4a maintenance and M4b now run in one Trino JVM, with
  197.6 MiB committed after the marts.

Before that: **M4a, Airflow + Trino on demand** (see *Batch and orchestration* and *Iceberg maintenance* below).
- **Batch stack:** Airflow 2.11.2 (LocalExecutor, its own `airflow-db`) and Trino 483 (`trinodb/trino:483`, catalog
  `lake`) in the `batch` profile, started on demand (`batch-up`, `dag-test`, `batch-stop`), never beside Spark or
  Connect.
- **Three DAGs**, paused at creation: `iceberg_maintenance` (bronze 71 → 4 files, silver 405 → 4, rows unchanged,
  into `lake.metrics.maintenance_runs`), `partition_evolution_demo` (day vs hour on
  `lab.status_partition_evolution`: production stays `day()`) and `cdc_slot_keepalive` (an idle slot from 77.7 MiB
  retained to 56 bytes in 43 s of Connect).
- **Retries with exponential backoff and alerts** (`data/airflow/alerts.jsonl`), `verify-lake` ignoring `replace`
  snapshots, and the path-casing guard in `tasks.py`.

Before that: **CI off Node 20** (see *CI* below).
- Both Playwright jobs run on **Node 24** (Active LTS, end of life 2028-04-30) instead of Node 20 (end of life
  2026-04-30). Proved locally in `node:24-bookworm` (v24.21.0): 49 tests, and 72 in the repeat job, all pass.
- Every action that ran on the deprecated `node20` action runtime is bumped to a major on `node24`:
  `actions/setup-python@v7`, `actions/setup-node@v7`, `actions/cache@v6`, `actions/upload-artifact@v7`,
  `astral-sh/setup-uv@v10.3.0`; `actions/checkout@v5` was already on node24 and stays.
- `package.json` (and the lock's root entry) declares `"engines": {"node": ">=20"}`, the real floor. This machine
  stays on Node v20.18.0 (see *Known gaps* for the upgrade step).

Before that: **M3 hardening** (see *CDC* below).
- **WAL cap:** ops-db runs with `max_slot_wal_keep_size=1024MB`, so the CDC slot can pin at most ~1 GB of WAL while
  Connect is stopped (it had no limit before). Its `safe_wal_size` is now ~1 GiB.
- **Lost-slot check:** `connect-up` reads the slot's `wal_status` first. On `lost` it refuses to start Connect and
  points to `python tasks.py cdc-reset`; on `unreserved` it warns and starts Connect.
- **Retention:** the five `ops.public.*` topics keep their events forever (`retention.ms=-1`, `cleanup.policy=delete`),
  so a rebuild by replay never misses old rows; every other topic is unchanged.

Before that: **M3, CDC from the operational database** (see *Ops database* and *CDC* below).
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

Next: a local Lighthouse run (Loop P); M6 (including the Athena row of `docs/METRICS.md`) and the deployed-site
parts of F5 need the user (see *Working on now*).

### Running right now on this machine
| Process | How it was started | Stop with |
|---|---|---|
| GBFS producer | `uv run python -m dockwatch.producer run` (log: `data/producer-run.log`) | Ctrl+C / kill the process |
| Exporter | `uv run python -m dockwatch.exporter` (log: `data/exporter.log`) | Ctrl+C / kill the process |
| Stream stack | `python tasks.py up stream` | `python tasks.py down` |
| Spark streaming job | **Not running** (on demand only). Container `status-stream` has `restart: "no"`. | — |
| Spark lab stream (`dockwatch-stream-lab`) | **Not running** (on demand only: `python tasks.py stream-lab`, one `compose run -d --rm` container of the `status-stream` service in lab mode; removed when stopped). | `python tasks.py stream-lab-stop` |
| Kafka Connect (Debezium) | **Not running** (on demand only: `python tasks.py connect-up` or `cdc-e2e`). Container `connect` has `restart: "no"`. | `python tasks.py connect-stop` |
| Spark CDC apply (`cdc-apply`) | **Not running**; started by `cdc-catchup` / `cdc-verify` / `cdc-reset` with `docker compose run`, exits when done. | — |
| Trino + `airflow-db` | **Not running** (on demand only: `python tasks.py batch-up`; `restart: "no"`). | `python tasks.py batch-stop` |
| Airflow scheduler | **Not running** (on demand: `python tasks.py scheduler-up`; DAGs are paused at creation). One-shot Airflow CLI runs (`dag-test`, `airflow …`) exit when done. | `python tasks.py scheduler-stop` |
| Marquez (`marquez-db`, `marquez-api`, `marquez-web`) | **Not running** (on demand only: `python tasks.py lineage-up`, then `lineage-replay`; `restart: "no"`, no volume). | `python tasks.py lineage-stop` (removes the containers) |

Spark on demand: `python tasks.py catchup` processes everything in Kafka since the last run (`availableNow`) and
exits (~30 s for a few minutes of backlog; Kafka keeps 7 days). `python tasks.py stream` keeps it running (one
micro-batch per minute) until `python tasks.py stream-stop`. Run one Spark JVM at a time: stop `stream` before
`catchup`, `verify-lake`, `inspect`, `sql` or `test-spark`; `stream`, `catchup`, `stream-lab` and `crash-restart` refuse while any
status-stream container (a lab run included) runs. `live.json` only moves forward while Spark runs (the exporter
skips rewriting it while `data_as_of` stands still), so run `catchup` before looking at the exporter output.
`catchup` also runs the OpenLineage Spark listener (one `spark-*.jsonl` per run in `data/lineage`, see *Lineage*);
`stream` does not.

Memory: the streaming job uses about 1.5–1.9 GB (limit 2 GB); in Loop O's hour of `stream` it peaked at 2,022 MiB in docker stats (~1,855 MiB RSS, the rest page cache), and the lab stream at 2,033 MiB at 10× and 2,005 MiB at 50×, none OOMKilled. The rest of the stack is under 1 GB at start (s3 grows with the data: 750–1,087 MiB in Loop O's campaign, redpanda 516–620). The host often has
only ~1–3 GB free; the 24/7 job stalled there (driver heartbeat timeout), which is why it is now on demand.
Kafka Connect peaks at ~520 MiB (`mem_limit: 1g`, heap `-Xmx512m`) and the CDC apply at ~1.2 GB (`mem_limit: 2g`);
Connect and a Spark JVM never run together (the CDC tasks and `catchup` refuse). Trino (`mem_limit: 2g`, heap 1 GB;
measured peaks 1,438–1,845 MiB in M4a, up to 2,013 MiB in an M4b `marts_daily` run that followed other batch work;
1,694–1,768 MiB in the DAG, up to 1,885 MiB with the checks' queries, for `marts_daily` with Great Expectations on a fresh JVM) never runs beside a Spark JVM or Connect: `batch-up` refuses beside them, and `catchup`,
`stream`, `inspect`, `verify-lake`, `sql` and the CDC tasks refuse while Trino runs. Marquez (~380 MiB for its
three containers) runs only when none of Trino, Spark or Connect does, and while it runs all of their tasks refuse.

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

Batch (on demand; Trino and Airflow never beside Spark or Connect):
Airflow (LocalExecutor, metadata in airflow-db) ──(DAG tasks, HTTP)──► Trino catalog `lake` (same REST catalog + SeaweedFS)
        iceberg_maintenance      → optimize / expire_snapshots / remove_orphan_files on 16 lake tables
                                   (9 core + 7 M4b / metrics tables; a missing batch table is `skipped`)
                                 → lake.metrics.maintenance_runs   (one row per table per run)
        partition_evolution_demo → lake.lab.status_partition_evolution → data/maintenance/partition_evolution.json
        cdc_slot_keepalive       → docker start / stop of the existing `connect` container → data/metrics/slot_health.jsonl
        trip_history_monthly     → Bay Wheels trip ZIP (s3.amazonaws.com/baywheels-data, cached in data/trips/zips)
                                 → pyiceberg in airflow-cli → lake.bronze.trips (one commit per month)
                                 → Trino count-back → lake.metrics.trip_loads
        marts_daily              → newest raw GBFS station_information → pyiceberg → lake.bronze.station_information
                                 → dbt build on Trino → lake.marts.mart_* → quality gate (data/dbt/quality_gate.json):
                                   dbt results + the Great Expectations checkpoint on the newest 24 h of
                                   lake.silver.station_status (GE venv subprocess → data/quality/expectations.json)
                                 → web/data/insights/*.json (format 2, manifest.json last; only when the gate passed)
        pipeline_health          → per-minute GROUP BYs on lake.silver.station_status → pyiceberg (one commit)
                                 → lake.metrics.pipeline_minutely (the 1,440 minutes before the run)
                                 → web/data/health.json (status, tiles, minutes, table freshness, quality incl. the
                                   Great Expectations results, compaction)
        any task failure / SLA miss → data/airflow/alerts.jsonl (+ SNS when DOCKWATCH_ALERTS_TOPIC_ARN is set)
        every DAG and task run   → OpenLineage events (file transport) → data/lineage/ol-<time>.json, one file each

python tasks.py catchup ──(openlineage-spark listener, file transport)──► data/lineage/spark-dockwatch-status-stream-<UTC>.jsonl

Lab (Loop O, on demand; never production): raw archive ──(python tasks.py replay --date … --speed N [--skip --limit],
        paced by the feed's schedule)──► Kafka gbfs.station_status.replay
gbfs.station_status.replay ──(status_stream in lab mode: python tasks.py stream-lab, startingOffsets latest,
        checkpoints /checkpoints/lab/<UTC>)──► lake.lab_bronze.station_status, lake.lab_silver.station_status /
        availability_5m / station_episodes, Kafka gbfs.alerts.lab, dockwatch.station_state.lab
Trino (HTTP) ──(python tasks.py measure window / measure files)──► data/metrics/*.json ──► docs/METRICS.md

Lineage (on demand; Marquez never beside Trino, Spark or Connect):
data/lineage/ol-*.json + spark-*.jsonl ──(python tasks.py lineage-replay: the newest run of each DAG, then the newest
        catch-up with lineage, Iceberg tables renamed to their Trino names; each in event-time order)──►
        Marquez API :15000 (empty at every lineage-up) ──► Marquez web :13000
        ──(python tasks.py lineage-shot, Playwright CLI)──► web/img/lineage-batch.png (the Pipeline page's Lineage card)
                                                            + docs/img/lineage.png (the same file)
```

---

## What exists right now

### Documents (`docs/`)
| File | What it is |
|---|---|
| `DOCKWATCH_PLAN.md` | The original project plan (what and why). Unchanged. |
| `IMPLEMENTATION_PLAN.md` | Milestones M0–M7 / F1–F5, decisions (incl. M2 decisions), progress checkboxes. |
| `DESIGN.md` | DockWatch design system **v1.0.1**, frozen until launch (F5); v1.0.1 = accessibility fix (44 px touch targets in the list table). |
| `DESIGN_BACKLOG.md` | Gaps and judgment calls found while building against the frozen DESIGN.md (14 from F1, 18 from F2, 3 from the F2 review, 9 from the paused state / Replay a day loop, 6 from F3: #46–#51, 5 from F4 Insights: #52–#56, 4 from F4b / launch: #57–#60, 2 from Loop L: #61 the expectations card, #62 a proposed heatmap minimum coverage, 1 from Loop M: #63 the lineage card, 1 from Loop M2: #64 the nav overlay's fade, 1 from Loop N: #65 the lineage card's full graph); #35 (touch targets) is resolved. |
| `CONCEPTS.md` | Kafka, stream processing, Iceberg, change data capture, table maintenance / orchestration, dbt / marts / backfills, lookup tables / size budgets / compaction in steps, expectations vs tests (suite, checkpoint, batch, blocking vs warning), and lineage (job, run, dataset, namespaces, facets, the file transport and the replay, manual inlets / outlets) and lineage across producers (the Spark listener, symlinks, one dataset identity) explained through this codebase. |
| `reference/DESIGN_SOURCE_transitpulse.md` | The TransitPulse design file that was here before; reference only. |
| `CURRENT_STATE.md` / `DEVLOG.md` | This file / append-only history. |

### Python package `src/dockwatch/`
Host code runs on Python 3.12 (uv). Code under `streaming/` and `cdc/` also runs in the Spark image (Python 3.10), so it avoids 3.11+ features.

| Module | What it does | Why it's built this way |
|---|---|---|
| `config.py` | One `Settings` object from env / `.env` (prefix `DOCKWATCH_`): target, GBFS, Kafka topics, S3, Iceberg, checkpoints, trigger seconds and `trigger_mode` (`processing_time` / `available_now`), watermark, `spark_lineage_dir` (the OpenLineage Spark listener's folder; empty = off, only `catchup` sets it), `namespace_prefix` ("" = production tables; `lab_` = `lake.lab_bronze` / `lake.lab_silver`, set by `stream-lab`) and `starting_offsets` (`earliest`, or `latest` for a lab run), ops database / CDC (`ops_db_dsn`, `connect_url`, `cdc_namespace`, `cdc_topic_pattern`), batch (`trino_url`, `alerts_topic_arn`), state thresholds, exporter. | Switching to AWS is a config change. |
| `gbfs/models.py`, `gbfs/client.py` | Typed GBFS records (unknown fields kept); discovery → GBFS 2.3; re-discover on 404; custom User-Agent. | Feeds move and grow. |
| `producer/` | `messages.py` (explode per station, DLQ rejects, order-insensitive content hash), `archive.py` (gzip JSON to S3 or a folder), `kafka.py` (topic specs, idempotent publisher), `poller.py` (ttl-paced poll loop), `__main__.py` (`run`, `once`, `setup`, `replay`: an archived day to `<topic>.replay` at N× speed, paced by the feed's schedule, `--skip` / `--limit` for a slice in time order). | Keyed by station for per-station ordering; idempotent to avoid retry duplicates. |
| `streaming/episodes.py` | **Plain Python:** `Thresholds`, `classify()` (station state rule), `EpisodeState` + `advance()` (episode/alert state machine). | Unit-tested without Spark; the Spark rule is tested to give the same answers. |
| `streaming/transforms.py` | Spark: `parse_kafka()`, `with_state()`, `to_silver()`, `availability_windows()`. | Pure DataFrame functions, tested on tiny inputs. |
| `streaming/lake.py` | Spark/Iceberg session config (`local` = REST catalog + SeaweedFS, `aws` = Glue) plus `run_mode.lineage_conf()`, table DDL (`ddl(c, prefix)`: the lab namespaces with prefix `lab_`), `migrate()` for schema evolution (`EVOLUTIONS` list, same prefix). | Tables evolve in place; fresh and old installs share one history. |
| `streaming/run_mode.py` | `trigger_options()`: `processingTime` every `trigger_seconds`, or `availableNow`; `lineage_conf(s, app, now)`: `{}` without `spark_lineage_dir`, else the OpenLineage listener, the file transport at `<dir>/spark-<app>-<UTC>.jsonl` and namespace `dockwatch-spark`; `table(s, name)`: `lake.<namespace_prefix><name>`; `lab_refusal(s)`: refuses a prefix not matching `^[a-z][a-z0-9]*_$`, a lab run with production's checkpoint root or any production topic, and production with `latest`. No pyspark import. | Host-testable. |
| `streaming/status_stream.py` | The streaming job (four queries, see flow above). It refuses to start on `lab_refusal()` before any JVM, reads `startingOffsets` from the settings and names every table through `run_mode.table()`. In `available_now` mode it waits for every query to drain Kafka, then exits (non-zero if a query failed). | One app keeps memory low; separate checkpoints recover independently; both modes share them. |
| `streaming/inspect_tables.py`, `streaming/sql.py` | Health/consistency report (counts, freshness, duplicates, Kafka-offset gaps, states, episodes, file sizes); `--check` (`verify-lake`) asserts it and exits 1 on failure (duplicates, offset gaps or missing partitions, silver > 5 min behind bronze, no Iceberg commit in 60 min; `replace` snapshots, i.e. compaction, do not count as commits, so maintenance alone cannot make a stopped stream look fresh). One-off SQL. | Proof for the restart test, the catalog fix and day-to-day checks. |
| `measure.py` | **Stdlib + `dockwatch.config`** (Loop O): a small Trino HTTP client (`trino()`: POST `/v1/statement`, follow `nextUri`, retry 502 / 503 / 504, raise `TrinoError`; rows and the final stats), `window <table> <from> <to>` (the rows with `processed_ts` in the window: micro-batches paired with their committed `append` snapshots by `added-records`, nearest-rank p50 / p95 / max of fetch to silver, fetch to queryable and batch duration, events per minute over the Kafka span, fetch to Kafka p50, `sustained`) and `files <table> [--runs N]` (files / bytes / partitions, one discarded warm-up and N timed runs of one GROUP BY, the median `elapsedTimeMillis`). One JSON line, saved to `data/metrics/`, exit 1 on a Trino error. | The numbers in `docs/METRICS.md` are reproducible from the lake with one command each. |
| `exporter/build.py`, `exporter/__main__.py` | Builds `live.json` (641 stations with name, code, lat/lon, region, map view, bikes/e-bikes/docks, state, state since, last reported; counts per view) and `alerts.json` (open alerts longest first, last 20 resolved). Atomic writes. `data_as_of` = newest feed snapshot in the export; `generated_at` = time of the last export whose `data_as_of` advanced (`carry_generated_at`, `has_new_data`). Exports without new data are not written; at start the previous `live.json` is read (`read_previous`), so a restart does not make old data look new. `alerts.json` gets the same `generated_at`. | The static site only reads these files, and its freshness pill reads `generated_at`, so it must not move when the data doesn't. Stations without a `region_id` are placed in a view by coordinates. Edge: with no previous `live.json` (first run), the first export is written with `generated_at` = now, so the site shows Live for up to 2 min on old Kafka data, then goes stale. |
| `exporter/replay.py` | `python tasks.py replay-export [--date YYYY-MM-DD] [--step 60] [--out web/data/replay.json]`: reads one **Pacific** day of `raw/gbfs/station_status` snapshots from the archive (`FsArchive` / `S3Archive`; both UTC `dt=` partitions, filtered by Pacific date), keeps the last snapshot per `step` bucket, computes each station's state with `classify()` / `capacity_of()` and its view with `view_for()`, and writes `replay.json` atomically (format v1: `stations` with name, code, lat/lon, view, capacity; `frames` of `[index, state, bikes, docks]` groups, frame 0 full, later frames only changed stations; busiest stations get the smallest indices). Station names come from the day's last `station_information` snapshot. Default day: the latest complete Pacific day with snapshots (today if there is none). Prints size, stations and frames; exits non-zero if the day has no snapshots. | Uses the same rule as Spark without a JVM, and the archive the producer already writes. Snapshot by snapshot: only each station's previous value is kept. |
| `geo/build.py`, `geo/__main__.py` | Map-shape build step: Census cartographic boundary counties (`cb_2023_us_county_500k`, downloaded once to `data/geo/`) → per-view projection, rectangle clip (land kept 320 units beyond the square view box for wide/tall map cards), Douglas–Peucker simplify → `web/geo/<view>.json`. `pyshp` is in uv group `geo`, imported lazily. | Run once; output committed. Stations are projected in the browser with the same params, so new stations need no rebuild. |
| `ops_sim/model.py` | **Pure** simulator rules: `Model.step(tick, now_ts, alerts)` → `Op`s (action, table, Debezium op letter c/u/d, values). An empty episode open ≥ 20 min with no unfinished job at the station opens a job (the first idle van by plate, else it waits); its resolved alert closes the job `done` with `bikes_moved` and frees the van; full alerts open nothing; unfinished jobs are cancelled with a seeded chance per minute and deleted 5–60 min later; tickets open on random docks (dock → `out_of_service`), go `in_progress`, close (dock → `ok`); stations are switched off / on now and then. After migration 001 every job insert / update carries `priority` (1–3). `parse_alert()` reads a `gbfs.alerts` message. | One seeded RNG and sorted iteration: the same seed and alerts always give the same operations, so the CDC run can be compared exactly. |
| `ops_sim/synthetic.py` | Seeded stations (`sim-0001`…, capacities 11–35), vans (`DW-001`…) and a `gbfs.alerts` stream on a simulated clock (tick = 1 min from 2026-10-01 00:00 UTC; alert after 15 min, resolved at the episode end, the M2 payload); `SeededSimulation` (setup ops, `ops_for(tick)`, migration at tick `events // 2`). | Replaces "an hour of live simulation" with a bounded, repeatable run. |
| `ops_sim/db.py` | psycopg executor: one transaction per simulated minute; the SQL as module constants; finds "the unfinished job at a station" (`OPEN_JOB_AT_STATION`) and "the unfinished ticket on a dock" (`OPEN_TICKET_ON_DOCK`) before each job / ticket write and raises `Conflict` if the database disagrees with the model; counts calls per constant and the change events Debezium should emit per table and op (from `rowcount`); `apply_schema()` (schema.sql + migrations). | Expected CDC counts come from what Postgres actually changed. |
| `ops_sim/explain.py`, `ops_sim/__main__.py` | `explain`: scratch database `ops_bench` (created and dropped on ops-db) → seeded run to count calls → reload with the two hot indexes dropped and 200,000 jobs / tickets (`setseed` + `generate_series`) → `EXPLAIN (ANALYZE, BUFFERS)` before / after re-creating each index from schema.sql → `sql/ops/PLANS.md`. CLI: `schema [--base-only]`, `seeded --seed --events [--stations]` (refuses non-empty tables), `live` (consumes `gbfs.alerts`, group `dockwatch-ops-sim`, 60 s tick; stations from `gbfs.station_information`), `explain [--out]`. | The `ops` database and `iceberg_catalog` are never touched by the bench. |
| `cdc/checksum.py` | Shared table checksum (Python 3.10-safe, for the host and the Spark image): canonical text per value (None `\N`, bool t/f, float `repr`, datetime UTC ISO µs with naive = UTC, Decimal normalised), sha256 per row (fields joined with `\x1f`), sha256 of the sorted row hashes; `_`-prefixed CDC columns dropped. | Postgres and Iceberg rows are compared with one function. |
| `cdc/guard.py` | `refusal(task, running)`: `cdc-catchup`, `cdc-reset`, `cdc-verify` (and `catchup`) refuse while Kafka Connect or `status-stream` runs, `connect-up` while `cdc-apply` or `status-stream` runs; `cdc_topics()`: a CDC reset may only delete `ops.public.*` and `dockwatch-connect-*`; `slot_refusal(slot, wal_status)`: refuses Connect on a `lost` slot (the message names `max_slot_wal_keep_size` and `python tasks.py cdc-reset`), allows none / `reserved` / `extended`; `slot_warning()`: a warning for `unreserved`. Pure and unit-tested; `cdc/runner.py` injects a `docker ps` probe and the slot's `wal_status`. | One heavy JVM at a time on this host; a lost slot fails clearly instead of as a Debezium stack trace. |
| `cdc/events.py` | **Pure** (Python 3.10): parses one Debezium envelope (JSON with schemas) into an `Event` (table from the topic, op, `source.lsn`, Kafka offset, primary key from the key schema, the row from `after`, or from `before` for a delete); maps Connect types to Iceberg (int16/int32 → int, int64 → bigint, double → double, boolean, string, `io.debezium.time.ZonedTimestamp` / `MicroTimestamp` → timestamp; anything else raises `SchemaError`); `latest_per_key()` by (lsn, offset); `merged_columns()` / `plan_evolution()` (add-only, a type change is refused); the CREATE / ADD COLUMN / MERGE SQL. | Envelope logic is unit-tested on the host with records captured from the real connector. |
| `cdc/apply.py` | Spark job (service `cdc-apply`): Kafka `subscribePattern` `ops\.public\..*`, earliest, `maxOffsetsPerTrigger` 1000, availableNow; `foreachBatch` (`make_batch_fn`) collects the batch, and per table creates `lake.ops.<table>` (format v2, unpartitioned), adds new columns, keeps the newest event per key and runs `MERGE INTO` (`WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd'` → DELETE, `WHEN MATCHED AND s._lsn > t._lsn` → UPDATE, `WHEN NOT MATCHED AND s._op <> 'd'` → INSERT). Metadata columns `_lsn`, `_op`, `_source_ts`, `_kafka_offset`, `_applied_ts`. Checkpoint `/checkpoints/cdc/ops`; `--replay` = fresh checkpoint `/checkpoints/cdc/replay-<ts>` from earliest, deleted afterwards; `--reset` = DROP TABLE `lake.ops.*` PURGE and delete `/checkpoints/cdc`. Prints `CDC_RESULT {json}`. | Stale or repeated events change nothing, so any replay converges to the same table. |
| `cdc/iceberg_checksums.py` | Spark: rows, columns and checksum of each `lake.ops.*` table over Postgres's column list (passed by the host), plus non-null counts of `rebalancing_jobs.priority`; prints `CDC_ICEBERG {json}`. | The Iceberg side of `cdc-verify`. |
| `cdc/runner.py`, `cdc/__main__.py` | Host CLI `python -m dockwatch.cdc`: `connect-up` (lost-slot check via `slot_status()`, retention on existing `ops.public.*` via `ensure_cdc_topic_retention()`, compose up `connect`, PUT `infra/connect/ops.json`, wait until the connector is RUNNING and the slot active), `connect-stop`, `reset`, `catchup [--replay]` (→ `data/cdc/catchup.json`), `verify` (→ `data/cdc/verify.json`, exit 1 on any mismatch), `e2e`, `kafka-counts`. Its `guard()` also consults `batch/guard.py` (no Spark or Connect beside Trino). Spark work runs with `docker compose --profile cdc-apply run --rm --no-deps cdc-apply …`; the output is echoed and the marker lines parsed. `e2e` records each step's time, the compose services running, and peak MiB per container (`docker stats` every ~3 s) in `data/cdc/e2e-report.json`. | One entry point for the tasks; the report is the evidence. |
| `alerts/handler.py` | `format_alert()` (plain English, Pacific time) and `lambda_handler()` (SNS if `DOCKWATCH_ALERTS_TOPIC_ARN` is set, else prints). | Ready for M6; not deployed. |
| `batch/guard.py` | **Pure, stdlib only** (tasks.py imports it by path): `refusal(task, running)`: `batch-up`, `scheduler-up`, `iceberg_maintenance`, `partition_evolution_demo`, `trip_history_monthly`, `marts_daily` and `pipeline_health` refuse while `status-stream`, `cdc-apply` or `connect` runs; `stream`, `catchup`, `inspect`, `verify-lake`, `sql`, `connect-up` and the CDC tasks refuse while `trino` runs (the message names Trino and `python tasks.py batch-stop`); `cdc_slot_keepalive` refuses beside `status-stream`, `cdc-apply` or `trino`; `lineage-up` (`LINEAGE_TASKS`) refuses beside `trino`, `status-stream`, `cdc-apply` or `connect` (`LINEAGE_BLOCKERS`), and every one of the tasks above refuses while any Marquez container (`marquez-api`, `marquez-db` or `marquez-web`) runs (the message names Marquez and `python tasks.py lineage-stop`). `running_services()`: `docker ps` by compose labels. | One heavy JVM family at a time on this host; Marquez is a JVM too. |
| `batch/lineage.py` | **Stdlib only** (Loop M): `LINEAGE`, what each of the 8 batch tasks reads and writes as OpenLineage (namespace, name) pairs (`table()`: `trino://trino:8080` + `lake.<schema>.<table>`; `repo_file()`: `file` + the repo-relative path; `s3()`: `s3://<bucket>` + key or prefix); `annotate(dag)` (the DAG files' last line) sets each planned task's `inlets` / `outlets` to `openlineage.client.event_v2.Dataset`s (imported lazily; a planned task missing from the DAG raises at parse time); the replay: `dag_run_of()` (a task event's DAG run from its `parent` facet, a DAG event's from itself, else not an Airflow event), `newest_runs()` (per DAG the run with the latest parsed `eventTime`, sorted by time), `replay()` (POSTs them to `/api/v1/lineage`; the first non-2xx answer or a connection error stops it with a `ReplayError` naming the status, body and file, or `lineage-up`); Loop N: `spark_dataset()` (an Iceberg table with a TABLE symlink in `http://iceberg-rest:8181` takes its Trino name), `spark_replay_events()` (the application run plus every run that reads and writes, a single output appended to the job name), `read_json_lines()` (a broken line names `file:line`) and `newest_spark_runs()` (per Spark application the newest `spark-*.jsonl` with lineage), posted by `replay()` after the Airflow runs. | `airflow.lineage.entities.Table` would be silently ignored by the provider; the names join with dbt-trino's and a later dbt or Spark integration. |
| `batch/maintenance.py` | **Pure:** the 16 tables (`TABLES`: the 9 core tables; `BATCH_TABLES`: `bronze.trips`, `bronze.station_information`, `metrics.trip_loads`, the three marts and `metrics.pipeline_minutely`; `ALL_TABLES` = both), the retention / size checks (below `1d` refused), the Trino SQL (`ALTER TABLE … EXECUTE optimize / expire_snapshots / remove_orphan_files`), the measurement query (files, bytes, delete files, rows, snapshots, partitions from `$files` / `$snapshots` / `$partitions`), `maintain()` (measure → three steps → measure; a changed row count raises; a batch table missing from `information_schema` returns a `skipped` row instead, while a missing core table still fails; `IDENTITY_PARTITIONS` tables, i.e. `bronze.trips`, optimize only the `trip_month` partition with the most files, and nothing when every partition has one file), the row and the one-INSERT SQL for `lake.metrics.maintenance_runs`; `orphan_files_removed` comes from the `deleted_files_count` metric row Trino 483 returns. | Tested with a fake Trino. |
| `batch/evolution.py` | **Pure:** lab-table DDL, one INSERT per UTC hour, the evolve and optimize SQL, `pick_days()` (two newest UTC days with ≥ 20 hours, else the two biggest), `summarize()` of `$files` rows per spec, the report. | Tested on fixtures. |
| `batch/slot.py` | `decide()` (pure: `lost` → fail, never reset; retained ≥ `catch_up_above_mb` or no catch-up for `max_hours_between_catchups` → catch up; else skip), `inspect_slot()` (psycopg on `pg_replication_slots`), `catch_up()` (`docker start` the existing compose `connect` container found by labels, wait for the connector RUNNING and the slot to confirm the start LSN with < 16 MiB retained, always `docker stop`), `slot_health.jsonl` lines. | Never creates containers or runs compose. |
| `batch/alerts.py`, `batch/airflow_defaults.py` | One JSON line per alert in `data/airflow/alerts.jsonl` (`at`, `kind` = `task_failed` / `sla_miss` / `slot_unhealthy`, `dag_id`, `task_id`, `run_id`, `try_number`, `max_tries`, `exception`, `log_url`), an ERROR log line, SNS when a topic is set (publisher injected in tests). `default_args()`: `retries=3`, `retry_delay` 1 min, exponential backoff, `max_retry_delay` 30 min, `execution_timeout`, `on_failure_callback`; `dag_kwargs()`: `catchup=False`, `max_active_runs=1`, `sla_miss_callback`. | Importable without Airflow, so host tests cover them. |
| `batch/trips.py` | Trip history. On the host: the bucket listing parse; the month key by `YYYYMM-` prefix (file names are irregular); `month_status` (published / missing / pending); the ZIP cache, reused only when its size equals the listing's; the one-CSV-member check; `schema_of` (legacy / ride; header names must match `^[a-z0-9_]+$`); `local_time` (America/Los_Angeles, `fold=0`, then UTC; text that does not parse is a bad timestamp); `map_row` (the `bronze.trips` columns); `month_batches` (a streamed `csv.reader`, `utf-8-sig`); and the Trino DDL, count and `trip_loads` SQL. In the Airflow image only: the Arrow tables, the pyiceberg REST catalog, `replace_month` (one transaction: delete the month, append 100k-row Arrow tables) and `load_month`. | pyiceberg and pyarrow are imported inside the writer, so host tests and CI never need them. |
| `batch/marts.py` | `station_rows` (the GBFS `station_information` document → dimension rows), the dimension DDL, `quality_gate` (from `run_results.json`, mart rows and freshness), `plain` (dates as ISO text, floats rounded to 3 digits), `insights_v2` (a mart's rows → format 2: the station lookup, the `window` constants, rows starting with the station index; refuses a window column that is not constant), `export_insights` (every file checked against 2,000,000 bytes first, then written through a temp file + `os.replace`, the manifest last; a table may carry extra top-level keys). | Pure, host-tested. |
| `batch/health.py` | **Pure** rules for `pipeline_health` (plan D5): `window()` (the 1,440 minutes before the run's start minute), the four silver GROUP BYs (`events_sql` by `kafka_ts`, `processed_sql` by `processed_ts` with `approx_percentile` p50 / p95 of `processed_ts - fetched_ts`, and `lag_sql` twice: rows processed since the window start by minute of `kafka_ts` and of `greatest(kafka_ts, processed_ts)`), `minutely_rows()` (the series; lag = the difference of the two running counts, exact with no clock-order assumption), `tiles()` (the last active hour = the 60 minutes ending at the newest minute with processed > 0), the status rules (stream ≤ 300 s ok, ≤ 1 day warn, else fail; batch tables ok ≤ 48 h; quality from `quality_gate.json`; maintenance from the newest run), `health_doc()` and `write_health()` (≤ 500,000 B, temp file + replace). In the Airflow image only: `minutely_arrow()` and `replace_window()` (one pyiceberg transaction: delete minutes ≥ the window start, append). | Host-tested; the check recomputes the same rules from silver. |
| `batch/expectations.py` | **Pure, stdlib only** (Great Expectations, Loop L): the suite `silver_station_status` (`EXPECTATIONS`: GE type, column key, kwargs, blocking, plain-English label), `gx_kwargs()` / `column_key()`, the batch (`window()` = the 24 hours ending at silver's newest `snapshot_ts`; `query()` = the query asset with `bikes_plus_docks` and `age_s` against a fixed `now` literal), `results()` (GE results in any order → one row per suite entry: `ok`, else `fail` if blocking, else `warn`; a missing result never passes; a min reported as text becomes a number), `results_doc()` (`data/quality/expectations.json`, format 1), `read_results()` and `merge_gate()` (the quality-gate rule below). | Host-tested without GE; importable by both Airflow's python and the GE venv. |
| `batch/gx_run.py` | Run by the GE venv only (`/opt/gx-venv/bin/python -m dockwatch.batch.gx_run`): an ephemeral GE context, an SQL datasource `trino://dockwatch@trino:8080/lake` with `create_temp_table=False`, the query asset, the suite, a ValidationDefinition and the Checkpoint `marts_daily_quality` (`SUMMARY`), then the results document written atomically. `frame` mode runs the same suite on small pandas frames (the Airflow-image test). It works around a GE 1.24 gap: GE finds no columns in a Trino query asset (its Trino fallback looks the FROM table up in `information_schema`, so derived columns are missing and every column check fails), so `reflect_trino_queries()` reads the names from `SELECT * FROM (<query>) LIMIT 0`. Imports GE, never `dockwatch.config` (the GE venv has no pydantic-settings). | GE's dependencies (SQLAlchemy 2, pandas 3) cannot share Airflow's environment. |
| `batch/dag_tasks.py`, `batch/runstate.py`, `batch/airflow_logging.py` | Run inside the Airflow image: the DAG callables (Trino client, Docker SDK, `AirflowFailException` for guard failures); the real state of a `dags test` run (`DAG_TEST_STATE` line); Airflow's logging config with alembic's INFO lines off (they were printed to stdout ahead of `-o json`). | DAG files stay thin. |
| `batch/host.py`, `batch/__main__.py` | Host CLI `python -m dockwatch.batch`: `batch-up`, `batch-stop`, `airflow`, `dag-test`, `test-airflow`, `scheduler-up`, `scheduler-stop`, `trino-sql`, `lineage-up`, `lineage-replay`, `lineage-shot [out]`, `lineage-stop` (see *Lineage*). Every compose call runs from the inherited working directory with `--no-deps`. `batch_up()` and `airflow_db_up()` (so `dag-test` and `scheduler-up`) create `data/lineage` for the OpenLineage file transport. `dag-test` refuses while another `airflow-cli` container runs. `ensure_image()` rebuilds the Airflow image when the hash of its build inputs (`BUILD_INPUTS`: the Dockerfile, `dbt-requirements.txt` and `gx-requirements.txt`) differs from the image label `dockwatch.build-hash`. | Like `dockwatch.cdc`. |

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
| `metrics.maintenance_runs` | none | one row per table per `iceberg_maintenance` run | `run_id`, `table_name`, `started_at` / `finished_at`, `status` (`ok` / `error` / `skipped`), `error`, files / bytes / rows / snapshots / delete files before and after, `orphan_files_removed`, `partitions_after`. Written by Trino in one INSERT per run. |
| `bronze.trips` | identity `trip_month` | one row per Bay Wheels trip | `trip_month`, `schema_version` (`legacy` / `ride`), `source_key`, `ride_id` / `rideable_type` (ride only), `started_at` / `ended_at` (local time read as America/Los_Angeles, stored UTC), `duration_s`, start / end station code, name, lat / lon, `member_type` (`member` / `casual`), `bike_id` (legacy only), `loaded_at`. Created by Trino DDL and written by pyiceberg, one commit per month. 13 months (2019-01 and 2025-10 to 2026-09): 5,981,514 rows. `iceberg_maintenance` compacts one month per run: the first 16-table run took it 64 → 13 files (230 → 179 MB); after the latest 12-month reload it had 64 again, and the next run took month 2026-09 to one file (58 files). |
| `bronze.station_information` | none | one row per current station | `station_id`, `short_name`, `name`, `region_id`, `lat`, `lon`, `capacity`, `feed_last_updated`, `loaded_at`. `marts_daily.stations` replaces it from the newest raw GBFS snapshot with one pyiceberg overwrite (645 stations). |
| `metrics.trip_loads` | none | one row per trip-month load attempt | `run_id`, `trip_month`, `source_key`, `source_bytes`, `zip_sha256`, `schema_version`, `header`, `csv_rows`, `loaded_rows` (Trino's count after the commit), `bad_timestamps`, `status` (`ok` / `missing` / `error`), `error`, `started_at` / `finished_at`. Written by Trino. |
| `metrics.pipeline_minutely` | none | one row per minute | `minute` (UTC), `events`, `processed`, `latency_p50_s`, `latency_p95_s`, `lag_records`, `run_id`, `computed_at`. Created by Trino DDL, written by pyiceberg in `pipeline_health` (each run replaces its 1,440-minute window and anything later in one commit, so no minute is there twice). |
| `marts.mart_station_availability_daily`, `marts.mart_rebalancing_need`, `marts.mart_trips_vs_availability` | none | see *dbt and marts* | Built by dbt (`CREATE OR REPLACE TABLE … AS`) on each `marts_daily` run. |
| `lab_bronze.station_status`, `lab_silver.station_status`, `lab_silver.availability_5m`, `lab_silver.station_episodes` | as production | as production | Loop O's lab mode (`stream-lab`): the same DDL and evolutions with prefix `lab_`, written only from `gbfs.station_status.replay`; never maintained, so measured windows stay reproducible. The campaign's 10× and 50× replays of 2026-10-16 wrote 660,212 rows to lab bronze and lab silver; each `accept-o-lab-replay` run adds 38,760 (698,972 after the Builder's). Lab data, on the user's deletion list. |
| `lab.status_partition_evolution` | `day(snapshot_ts)`, then evolved to `hour(snapshot_ts)` | silver's rows for two days | Lab copy for `partition_evolution_demo` (dropped and re-created by each run); not created yet. |
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
(`connect-up`), persists (inactive) while Connect is stopped, and is dropped only by `cdc-reset` / `cdc-e2e`;
`ops-schema` owns only the publication. While it is inactive it holds the WAL written since the last catch-up, for
the whole cluster (so also the `iceberg_catalog` database's commits), up to the `max_slot_wal_keep_size=1024MB` cap.
Its `wal_status` is `reserved` today (after the last e2e: ~1.36 MB retained, `safe_wal_size` ~1,075 MB, `pg_wal`
272 MiB). Past the cap it would become `unreserved`, then `lost`, and `connect-up` would refuse until `cdc-reset`.
The tables hold the seeded run's result (80 stations, 1,774 docks, 4 vans, 111 jobs, 123 tickets; `priority` set on
55 jobs).

### CDC (Debezium → Kafka → Iceberg)
- **Kafka Connect** (`connect`, profile `cdc`, `quay.io/debezium/connect:2.7.3.Final`): `mem_limit: 1g`,
  `KAFKA_HEAP_OPTS` / `HEAP_OPTS` `-Xms256m -Xmx512m` (effective `-Xmx512m`), `restart: "no"`, REST API on
  http://localhost:18083. Started only on demand.
- **Connector** `infra/connect/ops.json` (`ops-postgres`): pgoutput, database `ops`, the five `public.*` tables,
  publication and slot `dockwatch_ops` (`publication.autocreate.mode=disabled`), `snapshot.mode=initial`,
  `tombstones.on.delete=false`, `JsonConverter` with schemas for key and value, heartbeat every 10 s, topics with 1
  partition and replication factor 1. Topic-creation group `cdc` (include regex `ops\.public\..*`): the topics it
  creates get `retention.ms=-1` and `cleanup.policy=delete`; no default retention keys, so the heartbeat topic keeps
  the broker default.
- **`connect-up`**, in order: refuse while `cdc-apply` / `status-stream` runs (memory guard); read the slot's
  `wal_status` from `pg_replication_slots` and refuse on `lost` (`cdc connect-up: replication slot dockwatch_ops is
  lost ...`, naming `max_slot_wal_keep_size` and what `python tasks.py cdc-reset` drops; exit 1, Connect is not
  started), or print a warning to stderr on `unreserved` and go on; set `retention.ms=-1` / `cleanup.policy=delete`
  on every existing `ops.public.*` topic (`incremental_alter_configs`, idempotent; upgrades topics created with the
  7-day default); then compose up `connect`, register the connector and wait until it is RUNNING with the slot
  active.
- **Apply** (`cdc-apply`, see `cdc/apply.py` above) → `lake.ops.*`; **verify** compares rows, columns and the
  shared checksum per table and writes `data/cdc/verify.json`.
- **`python tasks.py cdc-e2e --seed 42 --events 1500`** (~85 s): preflight (stops a leftover Connect) → reset-spark →
  reset-pg-kafka (slot, tables re-created with the base schema, CDC topics) → connect-up → sim-a (ticks 0–749) →
  schema-change (migration 001) → sim-b (ticks 750–1499) → connect-catch-up (Kafka counts per table and op = the
  simulator's) → connect-stop → apply → verify → `data/cdc/e2e-report.json` (exit 0 only if everything matched).
  Last run (2026-10-09, under the WAL cap): ok in 89 s, 3,049 events, all five tables equal; peak `connect`
  513 MiB, `cdc-apply` 1,075 MiB. `cdc-catchup --replay` then re-applied the 3,049 events (23.5 s) and `cdc-verify`
  matched.

### Batch and orchestration (M4a: Airflow 2.11.2 + Trino 483, on demand)
- **Engines:** Airflow 2.11.2 (LocalExecutor, parallelism 2, one task per DAG at a time, metadata in `airflow-db`)
  drives Trino (`trinodb/trino:483`) over HTTP; Trino's catalog `lake` is the same Iceberg REST catalog and SeaweedFS
  warehouse Spark uses, so `lake.silver.station_status` is the same table in both. Writes from Trino to SeaweedFS
  work without any S3 checksum setting, and Spark reads and appends after Trino's commits (`catchup` +
  `verify-lake` pass after a maintenance run).
- **pyiceberg** 0.11.1 is the third writer, only for `bronze.trips`, `bronze.station_information` and
  `metrics.pipeline_minutely`. It is in the Airflow image, pinned by Airflow's constraints.
  - Its pyarrow S3 client needs `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` and
    `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required`, set in the `x-airflow` environment.
  - Without them, SeaweedFS stores the `aws-chunked` framing inside the objects.
- **Lifecycle:** `python tasks.py batch-up` (refuses beside Spark / Connect; builds the Airflow image if missing;
  `up -d --wait --no-deps trino airflow-db`; `airflow db migrate`) → `python tasks.py dag-test <dag_id> [key=value …]`
  → `python tasks.py batch-stop` (stops every batch service; volumes kept). `dag-test` starts `airflow-db` if needed
  (never Trino); when no `airflow-scheduler` container runs it first marks the DAG's stale `running` / `queued`
  dag_runs `failed` with their unfinished task instances and prints each run_id (`runstate fail-stale`: a run left
  by a killed `dags test` has no job, so zombie detection never ends it, and with `max_active_runs=1` it would block
  the scheduler); then it runs one bounded `airflow dags test` in `airflow-cli`, reads the run's real state from the metadata
  DB (`dags test` exits 0 on a failed run), samples `docker stats` every ~3 s and writes
  `data/airflow/dag-test-<dag_id>.json` (`dag_id`, `conf`, `logical_date`, `state`, `started_at`, `finished_at`,
  `seconds`, `peak_mem_mib` per service, `containers_running`, task states); exit 1 unless `success`.
  `python tasks.py scheduler-up` / `scheduler-stop` run the scheduler for a smoke test; DAGs are paused at creation
  (`DAGS_ARE_PAUSED_AT_CREATION` and `is_paused_upon_creation=True` on each DAG; `airflow dags test` never creates or
  unpauses the DAG rows), so nothing runs until a DAG is unpaused on purpose. Smoke test: scheduler healthy, the
  three DAGs listed with `is_paused` true, no import errors, scheduler 382 MiB.
- **DAGs** (`airflow/dags/`, thin; logic in `src/dockwatch/batch/`), all `catchup=False`, `max_active_runs=1`, the
  shared `default_args` (`retries=3`, 1 min doubling to at most 30 min, a per-task `execution_timeout`,
  `on_failure_callback`) and `sla_miss_callback`, with an `sla` on each last task:
  - `iceberg_maintenance` (`@daily`; params `file_size_threshold` 100MB, `expire_older_than` 1d,
    `orphan_older_than` 1d, below `1d` refused): `guard` → `maintain` (mapped over `maintenance.ALL_TABLES`, the 16
    tables, one at a time: measure, `optimize`, `expire_snapshots`, `remove_orphan_files`, measure; a changed row count
    fails the table; a batch table that does not exist yet is `skipped` and does not fail the run) →
    `record` (one INSERT into `lake.metrics.maintenance_runs`, runs even after a failed table, then fails the run if
    any table failed).
  - `partition_evolution_demo` (manual; params `days` = two `YYYY-MM-DD`, default picked from silver):
    `guard` → `prepare` (drop and re-create `lake.lab.status_partition_evolution` by `day(snapshot_ts)`) →
    `daily_load` (D1, one INSERT per hour) → `evolve` (`hour(snapshot_ts)`) → `hourly_load` (D2) → `optimize` →
    `report` (`data/maintenance/partition_evolution.json`: table, source, days, generated_at, stages `daily_load` /
    `hourly_load` / `optimized`, each with files / rows / bytes / partitions per spec id). Last run in
    *Iceberg maintenance* below.
  - `cdc_slot_keepalive` (`@hourly`; params `catch_up_above_mb` 256, `max_hours_between_catchups` 24, `timeout_s`
    600, `alert_below_safe_mb` 512): `inspect_slot` → `keepalive` (lost → `failed` line, `slot_unhealthy` alert,
    `AirflowFailException`, never a reset; catch up → refuse beside `status-stream` / `cdc-apply` / `trino`, `docker
    start` the existing `connect` container, wait for the connector `RUNNING`, **touch one captured row** (`UPDATE
    vans SET updated_at = now() WHERE van_id = (SELECT min(van_id) FROM vans) RETURNING pg_current_wal_lsn()`,
    autocommit; an empty `vans` fails with a clear message), wait until `confirmed_flush_lsn` ≥ max(LSN at start,
    touch LSN) and retained < 16 MiB, touching again (at most every 20 s) while a confirmed touch still leaves
    retained ≥ 16 MiB, `docker stop` in `finally`; skip otherwise). Connector start, touches and confirmation all
    count against `timeout_s`; on timeout Connect is stopped, a `failed` line names the stuck `confirmed_flush_lsn`
    and the target, and the task raises `AirflowFailException` (no retries). One line per run in
    `data/metrics/slot_health.jsonl` (`at`, `dag_run_id`, `action` caught_up / skipped / failed, `reason`, slot
    status / retained bytes / `safe_wal_size` before and after, `connect_seconds`, and `touch_lsn` when a row was
    touched); alert when `safe_wal_size` stays under 512 MB. The Iceberg apply stays `python tasks.py cdc-catchup`.
    Last run (2026-10-12 06:45 UTC, `catch_up_above_mb=16` after a 77.8 MiB WAL probe): retained 81,579,280 B →
    56 B, `safe_wal_size` 1,006 MB → 1,090 MB, two touches, Connect up 45.4 s, `dag-test` 61 s; peaks `connect`
    443 MiB, `airflow-cli` 298 MiB.
  - `trip_history_monthly` (`@monthly`, `start_date` 2019-01-01, **`catchup=True`**; param `month` = `YYYY-MM`,
    empty = the month of the run's data interval):
    - `guard`.
    - `published`: a `PythonSensor` (`reschedule` every 6 h, timeout 20 days). A published month goes on. A month
      missing while later months exist (e.g. 2020-04) gets a `missing` row in `trip_loads` and is skipped. A month
      not yet published waits.
    - `download`: the key must match `^\d{6}-[a-z]+-tripdata(\.csv)?\.zip$`; the ZIP is cached by size.
    - `load`:
      - sha256, and exactly one `.csv` member of at most 2 GB;
      - the mapped rows go in one pyiceberg transaction;
      - Trino's `count(*)` of the month must then equal the CSV records;
      - bad timestamps or a mismatch write an `error` row and fail the task.

    History is loaded with **`python tasks.py airflow dags backfill --reset-dagruns -y -s 2025-10-01 -e 2026-09-01
    trip_history_monthly`**: 12 runs, one at a time, 257 s wall. **Keep the DAG paused.** Unpaused, the scheduler
    would start every month since 2019-01 (~90 runs, ~0.9 GB of ZIPs).

    Last runs:
    - 2019-01 (legacy): 192,082 rows in 6.1 s;
    - the backfill: 5,789,432 rows, 7–15 s a month, `airflow-cli` peak 725 MiB;
    - rerun of 2026-09: 607,780 rows again.
  - `marts_daily` (`@daily`, `catchup=False`): `guard` → `stations` → `dbt_build` (a failed dbt test fails without
    retry) → `quality_gate` (the dbt gate plus the Great Expectations checkpoint, see *Great Expectations*; a failed
    blocking expectation fails without retry, a crashed GE run retries) → `export` (`trigger_rule` all_success;
    insights format 2). Last runs (2026-10-16, each on a freshly restarted Trino): Loop L's Checker run 44.0 s,
    `airflow-cli` 618 MiB, Trino 1,768 MiB, the GE run 3.2 s; with OpenLineage on (Loop M2's Builder run) 47.3 s,
    `airflow-cli` 617 MiB, Trino 1,733 MiB. Trino's range in this DAG is 1,694–1,768 MiB in the DAG, up to 1,885 MiB
    with the checks' queries right after it.
    **Operating rule:** run `marts_daily` on a fresh Trino JVM (`batch-stop` then `batch-up`), as its checks do
    (`accept-l-trino-restart`); after the trips stage Trino had sat at up to 2,024 of 2,048 MiB. No limit or JVM
    setting changed.
  - `pipeline_health` (`@hourly`, `catchup=False`, paused at creation): `guard` → `minutely` (the four GROUP BYs,
    the series in Python, one pyiceberg commit into `lake.metrics.pipeline_minutely`, counted back: 1,440 rows) →
    `export` (`sla` 30 min; reads the window back, silver's newest `processed_ts`, each existing maintenance table's
    newest non-`replace` commit / files / partitions, `data/dbt/quality_gate.json` and the dbt tests, the newest
    maintenance run → `web/data/health.json`). See *Pipeline health* below.
- **`dag-test` refuses beside another Airflow CLI run** (no concurrent runs). If an `airflow-cli` container is already running (another
  `dag-test` or a `dags backfill`), it stops with a message naming `docker ps` / `docker stop`. Its stale-run cleanup
  would otherwise fail the live run.
- **Alerts:** every task failure (after its last retry) and SLA miss appends one line to `data/airflow/alerts.jsonl`
  and logs at ERROR; with `DOCKWATCH_ALERTS_TOPIC_ARN` set it is also published to SNS. Guard failures (Spark or
  Connect beside Trino) fail at once without retries.
- **Docker socket:** `/var/run/docker.sock` is mounted only into the three Airflow services, to list running
  containers (the guard) and to `docker start` / `docker stop` the existing `connect` container. The Airflow user
  (uid 50000, group 0) reaches it through group 0 on Docker Desktop; no `user:` or `group_add` override.
- **Path-casing guard:** `tasks.py` `main()` compares `os.getcwd()` exactly with the
  `com.docker.compose.project.working_dir` label of every existing dockwatch container before any task and refuses
  with the recovery steps when they differ; it allows when Docker is unreachable or there are no containers (CI).

### dbt and marts (M4b)
- **dbt:** dbt-core 1.10.23 + dbt-trino 1.10.6, in their own venv `/opt/dbt-venv` in the Airflow image.
  - The pins are in `infra/airflow/dbt-requirements.txt`. dbt needs `protobuf>=6`, and Airflow's constraints pin
    5.29.
  - The project is `dbt/dockwatch/`, mounted read-only.
  - `profiles.yml` has two targets: `local` (Trino, catalog `lake`, schema `marts`, `threads: 1`) and `aws` (Athena;
    declared, not installed or run).
  - There is no `packages.yml`, so no `dbt deps`.
  - Results go to `data/dbt/target/run_results.json`.
  - **`python tasks.py dbt-compile`** compiles the project with no Trino: `uv run --no-project --python 3.12
    --with-requirements infra/airflow/dbt-requirements.txt dbt compile --no-populate-cache --target local`, with
    `DBT_SEND_ANONYMOUS_USAGE_STATS=false`, into `data/dbt-compile/target/` (7 models, 28 tests, ~8 s with uv's
    cache). `--no-populate-cache` keeps dbt from listing Trino's schemas, so the unresolvable host `trino` does not
    matter. The CI `Python lint + tests` job runs it after `uv run pytest -q`.
- **Sources:** `silver.station_status`, `silver.availability_5m`, `silver.station_episodes`, `bronze.trips` and
  `bronze.station_information`. **Staging** (`stg_*`) is ephemeral, so dbt creates no views.
- **Marts** in `lake.marts`, built from the current stations only:
  - `mart_station_availability_daily`: one row per station and Pacific service date, from `availability_5m`. It has
    share empty / full, minutes empty / full, average bikes and coverage. 3,860 rows.
  - `mart_rebalancing_need`: one row per station over the last 7 Pacific days. It has hours empty / full,
    `need_rank`, episodes, alerted episodes and the longest empty episode. 645 rows.
  - `mart_trips_vs_availability`: one row per station and Pacific hour of day. 15,461 rows.
    - Trips started / ended per day come from the newest 12 loaded months. The share empty / full comes from all
      live days.
    - Trip history ends where live data starts, so the two are compared as profiles.
    - For 99.3 % of the trips with a station code, the code matches a current station.
- **Tests (28):**
  - `accepted_values` on the state, `member_type` and `schema_version`;
  - `not_null` on keys;
  - `unique` on station ids;
  - `relationships` to `bronze.station_information`;
  - singular tests for the two composite grains and for shares in [0, 1].
  - There is no `unique` test on `ride_id`: a GROUP BY over ~6 M strings needs more than Trino's 512 MB per query.
- **The build** takes 7.3 s.
- **Quality gate:** `data/dbt/quality_gate.json` has `passed`, the test counts, warnings, mart rows, and the newest
  daily date next to silver's. The gate fails on:
  - any `error` / `fail` / `skipped` node;
  - an empty mart;
  - a daily mart older than silver's newest Pacific day − 1.

  Since Loop L the gate also holds the Great Expectations result (next section): `passed` needs the dbt part and
  no failed blocking expectation.
- **Insights** (`web/data/insights/`, git-ignored), **format 2** (`marts.insights_v2`):
  - Each file is `{format: 2, generated_at, source, window, stations: {columns: [station_id, name, region_id],
    rows}, columns, rows}`, with floats rounded to 3 digits. A row's first value `station` is an index into
    `stations.rows`; every listed station is used once or more.
  - Moved out of the rows: the station id, name and region (to `stations`); the per-file constants to `window`
    (`rebalancing_need`: `window_first_day` / `window_last_day`; `trips_vs_availability`: `trip_days`,
    `trip_first_month` / `trip_last_month`); in the daily file `service_date` is an index into
    `window.service_dates`. `first_window` / `last_window` are dropped (the page does not use them).
  - Row counts are the marts' (the daily file: the newest 30 service dates).
  - Each file must be at most 2,000,000 bytes, or the export fails before writing anything.
  - The manifest (`format: 2`, `dag_run_id`, `quality_gate`, `files` rows / bytes, `windows`, `source`) is written
    last.
  - Last run (645 stations, 9 service dates):
    - `station_availability_daily.json`: 272,808 B (39.6 B a row; ~1.26 MB projected at 1,000 stations × 30 dates);
    - `rebalancing_need.json`: 66,768 B;
    - `trips_vs_availability.json`: 514,417 B (~0.80 MB projected at 1,000 stations);
    - `manifest.json`: 711 B.
  - A copy of this export is the Playwright fixture `tests/web/fixtures/insights/`.

### Great Expectations (M5, Loop L)
- **Where it runs:** inside `marts_daily`'s `quality_gate` task, which runs `/opt/gx-venv/bin/python -m
  dockwatch.batch.gx_run trino` as a subprocess (timeout 900 s) after reading silver's newest `snapshot_ts`. The DAG
  keeps its five tasks. GE 1.24.0 lives only in `/opt/gx-venv` (the full `pip freeze` in
  `infra/airflow/gx-requirements.txt`); Airflow's own python cannot import it. `GX_ANALYTICS_ENABLED=False`.
- **Suite `silver_station_status`** (`batch/expectations.py`), blocking unless noted:
  - the batch has at least one row; silver has its 17 columns (`exact_match` false, so a new column passes);
  - `station_id` and `snapshot_ts` are never null; `state` is one of `offline`, `empty`, `full`, `low`, `high`, `ok`;
  - `num_bikes_available` and `num_docks_available` are never negative;
  - `capacity` ≥ `bikes_plus_docks` (available bikes + docks; capacity also counts disabled ones);
  - **uniqueness** of (`station_id`, `snapshot_ts`);
  - **freshness**, a warning only: the minimum `age_s` (seconds from `processed_ts` to the run's start) is at most
    300. During `marts_daily` Spark never runs beside Trino, so it always warns, which matches the stream status.
- **Batch:** a query asset on `lake.silver.station_status` with `snapshot_ts` in (newest − 24 h, newest], plus the
  derived `bikes_plus_docks` and `age_s` (against a fixed `now` literal). No temp or real tables are created in the
  lake (`create_temp_table=False`).
- **`data/quality/expectations.json`** (format 1, written atomically): suite, checkpoint `marts_daily_quality`,
  `dag_run_id`, `run_at`, `gx_version`, `seconds`, the batch (table, window, `now`, rows), one result per
  expectation (type, column, label, blocking, success, status `ok` / `warn` / `fail`, observed value for the row
  count and the minimum age, unexpected and element counts as GE reports them, kwargs) and the counts.
- **Gate rule:** `quality_gate.json` gains `expectations` (suite, evaluated, passed, warnings, failed, path). Each
  failed blocking expectation adds a `gx:` problem and fails the gate, so nothing is exported; a missing or unusable
  results document adds `gx: no results (…)` and fails it too. The dbt keys (`tests`, `tests_passed`, `warnings`)
  keep their meaning.
- **Last checked run** (Loop L's Checker run, 2026-10-16): 382,254 rows, the 9 blocking expectations passed with 0
  unexpected rows, freshness warned at 2,001 s, 3.2 s inside the task (225 MiB RSS for the GE process in a separate
  measurement). The check recomputed every result on Trino and found no GE table in the lake.

### Lineage (M5 OpenLineage: Airflow and the Spark listener → Marquez, Loops M and N)
- **Events:** the three Airflow services run with `AIRFLOW__OPENLINEAGE__DISABLED=false`, namespace `dockwatch` and
  the **file transport** `{"type": "file", "log_file_path": "/opt/dockwatch/data/lineage/ol", "append": false}`
  (the image's own default stays disabled, so `test-airflow` runs without it). Every DAG run and task run writes its
  events while Marquez is down, one JSON file each (`data/lineage/ol-<time>.json`, ~7 KB): START and COMPLETE per
  task and the DAG's COMPLETE (`dags test` emits no DAG START). A task event names its DAG run in its `parent` facet.
  The transport never creates the folder and loses events without it, so `batch-up` / `airflow_db_up` create it.
  The other three DAGs (`iceberg_maintenance`, `partition_evolution_demo`, `cdc_slot_keepalive`) emit job events
  without datasets: they rewrite files in place, write lab tables or touch the ops db.
- **Forked emitters and the at-fork GC freeze (Loop M2):** provider 2.11.0 sends every task event from an `os.fork()`
  of the multi-threaded Airflow process (the child emits, then `os._exit(0)`); only its 10 s timeout is configurable,
  and the parent logs nothing when the child dies. Once a GC pass in that child freed a pyarrow `S3FileSystem` that a
  pyiceberg write had left as cyclic garbage; its curl destructor joined a resolver thread that exists only in the
  parent: SIGSEGV, a lost START event and a ~1.8 GB WSL crash dump. So importing `batch/lineage.py` (every annotated
  DAG file does, and `dags test` parses the whole folder) registers
  `os.register_at_fork(before=gc.freeze, after_in_parent=gc.unfreeze)`: a forked child never collects objects it
  inherited (its own new objects are collected as usual), and the parent unfreezes right after the fork and collects
  its garbage as before. Both calls are O(1); Windows has no `register_at_fork`, so the host skips it. Keep it while
  the provider forks.
- **Spark events (Loop N):** the openlineage-spark 1.54.0 jar sits in the Spark image's `/opt/spark/jars` (an image
  layer: with `--jars` the listener cannot reach the Iceberg / Kafka class loader). `run_mode.lineage_conf()` turns on
  `io.openlineage.spark.agent.OpenLineageSparkListener` only when `DOCKWATCH_SPARK_LINEAGE_DIR` is set, which only
  `tasks.py catchup` does (with `data/lineage` mounted by `compose run -v`; compose is unchanged). Each catch-up writes
  one JSON-lines file, `data/lineage/spark-dockwatch-status-stream-<UTC %Y%m%dT%H%M%SZ>.jsonl`, job namespace
  `dockwatch-spark`; the application run `dockwatch_status_stream` is the parent of one run per query execution
  (`dockwatch_status_stream.<operation>[.<table>]`). `stream` emits nothing (it would write ~16 MB an hour of the same
  edges into its 1.5–1.9 GB JVM), and neither do `verify-lake`, `inspect`, `sql` or the CDC apply (its MERGE from a
  collected DataFrame would show `lake.ops.*` outputs with no input).
- **What the Spark jobs read and write** (the replayed jobs; the Pipeline page lists them under the table):

  | Job (replayed name) | Reads | Writes |
  |---|---|---|
  | `dockwatch_status_stream.write_to_data_source_v2.lake_bronze_station_status` | `gbfs.station_status` (`kafka://redpanda:9092`) | `lake.bronze.station_status` |
  | `dockwatch_status_stream.write_to_data_source_v2.lake_silver_station_status` | `gbfs.station_status` | `lake.silver.station_status` |
  | `dockwatch_status_stream.write_to_data_source_v2.lake_silver_availability_5m` | `gbfs.station_status` | `lake.silver.availability_5m` |
  | `dockwatch_status_stream.replace_data.lake_silver_station_episodes` | `lake.silver.station_episodes` (the MERGE's own target) | `lake.silver.station_episodes` |

  Spark names an Iceberg table by its location (`s3://dockwatch` + `warehouse/<schema>/<table>`) with a TABLE symlink
  in `http://iceberg-rest:8181` (`<schema>.<table>`); the replay renames it to `trino://trino:8080` +
  `lake.<schema>.<table>`, the Airflow name, and drops the symlink facet, so the graph joins at bronze / silver.
  **The foreachBatch gap:** inside `foreachBatch` Spark sees no input, so the episodes MERGE reads only its own table
  and the alerts / station-state Kafka sinks show no input; the stateful step (`flat_map_groups_in_pandas_with_state`)
  reads Kafka and writes nothing. The three streaming writes share the raw job name `write_to_data_source_v2`
  (OpenLineage adds no table to micro-batch writes), hence the suffix.
- **What each batch task reads and writes** (`batch/lineage.py`, set by `lineage.annotate(dag)`; the guards and the
  `published` sensor have none):

  | Job | Reads | Writes |
  |---|---|---|
  | `trip_history_monthly.download` | `s3://baywheels-data` `tripdata` | `data/trips/zips` |
  | `trip_history_monthly.load` | `data/trips/zips` | `lake.bronze.trips`, `lake.metrics.trip_loads` |
  | `marts_daily.stations` | `s3://dockwatch` `raw/gbfs/station_information` | `lake.bronze.station_information` |
  | `marts_daily.dbt_build` | every dbt source: `lake.silver.station_status`, `silver.availability_5m`, `silver.station_episodes`, `bronze.trips`, `bronze.station_information` | the three marts, `data/dbt/target/run_results.json` |
  | `marts_daily.quality_gate` | the three marts, `lake.silver.station_status` (the GE batch), `silver.availability_5m` (freshness), `run_results.json` | `data/dbt/quality_gate.json`, `data/quality/expectations.json` |
  | `marts_daily.export` | the three marts, `quality_gate.json` | `web/data/insights` |
  | `pipeline_health.minutely` | `lake.silver.station_status` | `lake.metrics.pipeline_minutely` |
  | `pipeline_health.export` | `metrics.pipeline_minutely`, `silver.station_status`, `metrics.maintenance_runs`, `quality_gate.json`, `expectations.json`, `run_results.json` | `web/data/health.json` |

  Lake tables are named the Trino way (`trino://trino:8080`, `lake.<schema>.<table>`), files by repo path (`file`),
  S3 by bucket and key.
- **dbt without openlineage-dbt:** its processor takes a model's inputs from dbt's `parent_map` and does not resolve
  ephemeral models, and the four `stg_*` models are ephemeral, so each mart would read phantom `lake.marts.stg_*`
  datasets nobody writes. `dbt_build` carries the dbt sources and marts instead, and an Airflow-image test compares
  them with `dbt/dockwatch` (`sources.yml`, `models/marts/*.sql`). The dbt venv and the image are unchanged.
- **Marquez** (compose profile `obs`, on demand): `marquez-db` (`postgres:16-alpine`, 256m), `marquez-api`
  (`marquezproject/marquez:0.51.1`, `MARQUEZ_CONFIG=marquez.dev.yml`, `SEARCH_ENABLED=false` so no OpenSearch JVM,
  port 15000, 768m, default JVM heap) and `marquez-web` (`marquezproject/marquez-web:0.51.1`, port 13000, 256m); no
  volume, so it starts empty every time. Never beside Trino, Spark or Connect, both ways round (`batch/guard.py`).
- **Tasks:**
  - `python tasks.py lineage-up`: refuses beside Trino / Spark / Connect, removes any old obs containers, starts the
    db (`--wait` on its healthcheck), then the api and web, and waits up to 180 s for the API (~10 s here).
  - `python tasks.py lineage-replay`: reads `data/lineage/ol-*.json`, keeps the **newest run of each DAG** (events
    grouped by DAG run; the run with the latest `eventTime`, compared as datetimes) and POSTs it in event-time order
    to `/api/v1/lineage`; the first non-2xx answer (status, body, file) or a connection error (run `lineage-up`)
    exits 1. Then, per Spark application, the newest `spark-*.jsonl` that has a run reading and writing a dataset
    (an empty catch-up does not blank the graph): the application run and every run with an input and an output,
    Iceberg tables renamed as above, a run with one output named `<job>.<output, dots → underscores>` unless it
    already ends so; DDL, the stateful step and the Kafka sinks are not posted. A broken line exits 1 naming
    `file:line`.
  - `python tasks.py lineage-shot [out]`: one Playwright CLI screenshot, 1600 × 1000, of
    `http://localhost:13000/lineage/job/dockwatch/marts_daily.dbt_build?depth=3` (Kafka through Spark and dbt to the
    marts, the gate and both exports) once the Kafka node is drawn (`text=/gbfs\.station/`), then 3 s; with no
    argument into `web/img/lineage-batch.png` (156,889 B) and a copy at `docs/img/lineage.png` (the same bytes), with
    an argument only into that file.
  - `python tasks.py lineage-stop`: removes the three obs containers and only their anonymous volumes; idempotent.
- **Last replay** (2026-10-17, Loop N's Builder run): 111 events: the newest run of each of the six DAGs (84:
  `iceberg_maintenance` 37, `partition_evolution_demo` 15, `marts_daily` 11, `trip_history_monthly` 9,
  `pipeline_health` 7, `cdc_slot_keepalive` 5) and the newest catch-up (27 of its 56 events). 30 jobs in `dockwatch`
  (the 8 annotated tasks with exactly their datasets; the other tasks and the DAG jobs without) and 5 in
  `dockwatch-spark` (the application and the 4 jobs above); the graph around `marts_daily.dbt_build` at depth 3 has
  33 nodes, with a directed path from `gbfs.station_status` to every mart. Memory: `marquez-api` 242 MiB,
  `marquez-db` 70 MiB, `marquez-web` 20 MiB.
- **Cost in Airflow:** within run-to-run noise. `airflow-cli` with OpenLineage on (Loop M2's Builder run) vs off (its
  baseline at 2029cfa): `marts_daily` 617 / 628 MiB, `pipeline_health` 379 / 433 MiB, `trip_history_monthly` 545 /
  616 MiB, `iceberg_maintenance` 301 / 292 MiB; seconds 47.3 / 42.2, 27.8 / 24.8, 32.9 / 31.0 and 42.4 / 36.5 (one
  `fork` per task event).
- **Cost in Spark:** the catch-up JVM with the listener peaked at 980, 1,227 and 1,283 MiB of 2,048 in Loop N's
  Builder runs (archived catch-ups without it: 1,129–1,414 MiB; the `verify-lake` JVM after each, without it,
  1,400–1,636 MiB), and at 1,595 and 1,430 MiB in its Checker run (above the archived range, on one short run of 5
  and 3 samples; under the 1,700 MiB flag line); Loop N2's Checker 1,395 and 1,531 MiB (`verify-lake` 1,468 / 1,627). Loop O's
  campaign: catch-ups 1,455, 1,695 (after the SIGKILL) and 1,261 MiB, `verify-lake` 1,459; the hour of `stream` (no
  listener) 2,022 MiB and the lab stream 2,033 / 2,005 MiB, page cache included. One catch-up (18 min to an hour of backlog): 20 runs, 56–57 events, 272,647–275,004 B (the
  plan-time probe: ~39 events / ~190 KB per micro-batch).

### Pipeline health (`pipeline_health`, latest run 2026-10-20 03:24 UTC, `dag-test`, 37 s)
- **`health.json`** (`format: 1`, 82,596 B of its 500,000 budget): `generated_at`, `dag_run_id`, `status`
  (`overall` = the worst of `stream` / `quality` / `maintenance`, each with a detail), `tiles` (`latency_p95_s`,
  `events_per_min`, `lag_records`, `freshness_s`), `minutely` (1,440 rows: minute, events, processed, p50, p95,
  consumer lag), `tables` (every existing maintenance table: last non-`replace` commit, age, files, partitions,
  status), `quality` (the gate, the 28 dbt tests, and `expectations`: `data/quality/expectations.json` unchanged, or
  null before the first run), `compaction` (the newest maintenance run's files / bytes before and after). A copy of
  it is the Playwright fixture `tests/web/fixtures/health.json` (regenerated in Loop L after a real `marts_daily`).
- **Last run** (Loop O, right after an hour of `stream`): status **Degraded** (stream `warn`: the newest row
  reached silver 320 s before, after `stream-stop`; quality and maintenance `ok`); tiles p95 61.2 s, 636.2
  events/min, consumer lag 0, freshness 320 s, health.json 82,674 B. With the stream running every minute processes
  one micro-batch and the p95 is about one trigger; after a `catchup` alone it is the backlog's age (hours).
  `dag-test` 37 s, Trino 1,349 MiB, `airflow-cli` 345 MiB.
- **Trino restart rule:** after the full M4a + M4b sequence (maintenance, evolution, trips, backfill, marts) Trino
  sits at 1.8–2.0 GiB of 2 GiB, so both `marts_daily` and `pipeline_health` are run on a restarted Trino
  (`batch-stop` then `batch-up`), as their checks do. No limit or JVM setting changed.

### Iceberg maintenance (latest run, 2026-10-20 03:22 UTC, `dag-test iceberg_maintenance`, 50 s)
16 tables: the 9 core tables and the 7 batch tables (`maintenance.BATCH_TABLES`). Fresh Trino JVM, right after Loop
O's hour of `stream` and its crash-restart (run `manual__2026-10-20T03:22:09+00:00`, cited in `docs/METRICS.md`).

| Table | Files before → after | Snapshots before → after | Rows |
|---|---|---|---|
| `bronze.station_status` | 79 → 11 | 76 → 77 | 3,915,131 |
| `silver.station_status` | 419 → 11 | 87 → 88 | 3,915,131 |
| `silver.availability_5m` | 27 → 11 | 87 → 88 | 799,126 |
| `silver.station_episodes` | 11 → 11 | 73 → 73 | 6,746 |
| `ops.*` (5 tables) | 1 → 1 each | 8 → 8 each | 80 / 1,774 / 4 / 111 / 123 |
| `bronze.trips` | 64 → 58 (one `trip_month` partition) | 173 → 174 | 5,981,514 |
| `bronze.station_information` | 1 → 1 | 6 → 6 | 647 |
| `metrics.trip_loads` | 15 → 1 | 30 → 31 | 344 |
| `marts.mart_*` (3 tables) | 1 → 1 each | 3 → 3 each | 7,735 / 647 / 15,528 |
| `metrics.pipeline_minutely` | 2 → 1 | 2 → 2 | 5,479 |

On the same files the station query `SELECT station_id, count(*), max(snapshot_ts) … GROUP BY station_id` went from
a 287 to a 163 ms median (`measure files`, a fresh JVM each side). Peak memory: Trino 1,970 MiB (2 GiB limit; above
the 1,950 flag line, flag only), `airflow-cli` 301 MiB. In the full check run of 2026-10-15 13:31 UTC (after the
12-month reload) the same rule took `bronze.trips` 64 → 58 files (month 2026-09 only) and `metrics.pipeline_minutely`
2 → 1, in 31 s with Trino at 1,680 MiB. The first 16-table run of that day optimized all
of `bronze.trips` in one statement (64 → 13 files, 485 → 1 snapshots, 16 s) and took Trino to **2,024 MiB**; a
run with nothing to compact peaks at 1,273 MiB. So `bronze.trips` is now optimized **one partition per run** (the
month with the most files, `… EXECUTE optimize(…) WHERE trip_month = DATE '…'`): one month in Trino's writers and
one `replace` commit per run. After a 12-month reload it takes 13 daily runs to compact every month.

**A compacting run ends with one more snapshot when nothing is older than a day:** `optimize` commits exactly one new
`replace` snapshot, and `expire_snapshots` (`retention_threshold => '1d'`, the catalog floor
`iceberg.expire-snapshots.min-retention=1d`; Trino has no `retain_last`) only removes snapshots older than a day and
never the current one, which is that `replace`. That is why `bronze.trips` and `metrics.trip_loads` went +1 here, and
untouched tables stayed equal. When older snapshots exist they go: the first run of the day took `bronze.trips` 485 →
1, `bronze.station_information` 16 → 2 and `metrics.trip_loads` 91 → 1 snapshots.

**Partition evolution** (`partition_evolution_demo`, 2026-10-10 13:14 UTC, days 2026-10-08 and 2026-10-09 of
`silver.station_status`, one INSERT per hour), on `lab.status_partition_evolution`:

| Stage | Spec 0 `day(snapshot_ts)` | Spec 1 `hour(snapshot_ts)` | Total files / rows |
|---|---|---|---|
| `daily_load` (D1) | 1 partition, 15 files, 551,260 rows | — | 15 / 551,260 |
| `hourly_load` (D2, after evolving) | 1 partition, 15 files | 10 partitions, 10 files, 270,029 rows | 25 / 821,289 |
| `optimized` | — (rewritten into the current spec) | 25 partitions, 25 files, 2.6 MB | 25 / 821,289 |

`optimize` rewrites every file into the table's current spec, so after compaction each hour holds one file: the hourly
spec keeps ~one file per hour with data (up to 24 a day), where the daily spec compacts a day to one file. Production
tables stay `day()`.

### Kafka topics (Redpanda)
| Topic | Partitions | Policy | Written by |
|---|---|---|---|
| `gbfs.station_status` | 6 | 7 days | producer (~641 msg/min) |
| `gbfs.station_information` | 1 | compacted | producer (only on change) |
| `gbfs.dlq` | 1 | 28 days | producer (0 rows so far) |
| `gbfs.station_status.replay` | 6 | 7 days | `producer replay` (Loop O's lab runs: 660,212 replayed records of 2026-10-16, plus 38,760 per `accept-o-lab-replay` run); read only by the lab stream |
| `gbfs.alerts.lab`, `dockwatch.station_state.lab` | 1 each | default (auto-created on first write) | the lab stream's episodes query; lab data |
| `gbfs.alerts` | 1 | 28 days | Spark episodes query (key `alert_id`, at-least-once) |
| `dockwatch.station_state` | 1 | compacted | Spark episodes query (key `station_id`) |
| `ops.public.stations`, `ops.public.docks`, `ops.public.vans`, `ops.public.rebalancing_jobs`, `ops.public.maintenance_tickets` | 1 each | unlimited: `retention.ms=-1`, `cleanup.policy=delete` (from the connector's topic-creation group; set on existing topics by `connect-up`) | Debezium (key = primary key, Debezium envelope as JSON with schemas); deleted by `cdc-reset` |
| `dockwatch-connect-configs`, `dockwatch-connect-offsets` (25), `dockwatch-connect-status` (5) | 1 / 25 / 5 | compacted | Kafka Connect's own state; deleted by `cdc-reset` |
| `__debezium-heartbeat.ops` | 1 | default | Debezium heartbeats (every 10 s while Connect runs); not read by anything, kept by `cdc-reset` |

### Local stack (`docker-compose.yml`)
| Service | Profile | Port | Purpose |
|---|---|---|---|
| `redpanda` | stream | 19092, 18081 | Kafka API. |
| `console` | stream | 8088 | Redpanda Console. |
| `s3` (SeaweedFS 3.80) | stream | 8333 | Local S3; bucket `dockwatch`. |
| `iceberg-rest` | stream | 8181 | Iceberg REST catalog; its table pointers live in Postgres database `iceberg_catalog` on `ops-db`. |
| `ops-db` (Postgres 16) | stream | 5434 | Database `ops` (the M3 ops schema, see *Ops database*) and database `iceberg_catalog` (catalog). `infra/postgres/init/` creates the catalog DB on a fresh volume. `command`: `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`, `max_slot_wal_keep_size=1024MB` (a slot may retain at most 1 GB of WAL). |
| `connect` | cdc | 18083 | Kafka Connect + Debezium 2.7.3 (`quay.io/debezium/connect:2.7.3.Final`, same tag as `tasks.py` `CONNECT_IMAGE`); 1 GiB `mem_limit`, heap `-Xmx512m`, `restart: "no"`, healthcheck on `/connectors`; on demand only (`connect-up` / `cdc-e2e`). Its compose calls pass `--profile stream --profile cdc` (it depends on redpanda and ops-db). |
| `cdc-apply` | cdc-apply | — | The CDC Spark apply / verify / reset (`dockwatch-spark:3.5.5`, `local[1]`, driver 1g, `TZ=UTC`, `src/` read-only, checkpoints volume); 2 GiB `mem_limit`, `restart: "no"`; only started with `docker compose run` by the CDC tasks, not part of the `spark` profile. |
| `trino` | batch | 18080 | `trinodb/trino:483` (same tag as `tasks.py` `TRINO_IMAGE`); config in `infra/trino/etc` (single node, heap `-Xmx1G`, `query.max-memory-per-node` 512 MB + heap headroom 256 MB, `task.max-writer-count=1`); catalog `lake` = Iceberg REST at `iceberg-rest:8181`, native S3 on SeaweedFS (path style), `expire-snapshots` / `remove-orphan-files` min retention `1d`; `jvm.config` `-XX:MaxMetaspaceSize=256M`; `mem_limit: 2g`, `restart: "no"`, healthcheck; no `depends_on` (started with `--no-deps`). |
| `airflow-db` | batch | — | `postgres:16-alpine`, database `airflow`, volume `airflow-db`, `mem_limit: 256m`, `restart: "no"`. Airflow's metadata only; never `ops-db`. |
| `airflow-scheduler` | batch | — | `dockwatch-airflow:2.11.2` (`infra/airflow/Dockerfile`: `apache/airflow:2.11.2-python3.12` + `trino`, `pydantic-settings`, `pytest` through Airflow's constraints, `psycopg[binary]==3.3.6`, the dbt venv `/opt/dbt-venv` and the Great Expectations venv `/opt/gx-venv`; label `dockwatch.build-hash`; 3.86 GB). The `x-airflow` services mount `./dbt` read-only and `./web/data`, and get the S3 settings. `mem_limit: 1g`; only via `scheduler-up`. |
| `airflow-webserver` | batch | 18090 | Same image, `AIRFLOW__WEBSERVER__WORKERS=1`, `mem_limit: 1g`; never started by the checks. |
| `airflow-cli` | airflow-cli | — | Same image, one-shot `docker compose run --rm --no-deps` (`dag-test`, `airflow …`, `db migrate`); `mem_limit: 1g`. The three Airflow services share the `x-airflow` environment, which turns OpenLineage on with the file transport into `data/lineage` (see *Lineage*). |
| `marquez-db` | obs | — | `postgres:16-alpine`, database / user / password `marquez` (Marquez's dev config hard-codes them; local only), `pg_isready` healthcheck, `mem_limit: 256m`, `restart: "no"`, no volume. |
| `marquez-api` | obs | 15000 | `marquezproject/marquez:0.51.1`, `MARQUEZ_CONFIG=marquez.dev.yml`, `POSTGRES_HOST=marquez-db`, `SEARCH_ENABLED=false`; `mem_limit: 768m`, `restart: "no"`, no volume. Flyway 8.5 warns that Postgres 16 is newer than it supports but migrates cleanly. |
| `marquez-web` | obs | 13000 | `marquezproject/marquez-web:0.51.1` (`MARQUEZ_HOST=marquez-api`, `MARQUEZ_PORT=5000`, `WEB_PORT=3000`); `mem_limit: 256m`, `restart: "no"`. One (dark) theme. Only started by `lineage-up`. |
| `status-stream` | spark | 4040 (Spark UI) | The M2 streaming job; image `dockwatch-spark:3.5.5` (`infra/spark/Dockerfile`: Spark 3.5.5, Java 17, Iceberg 1.6.1 + AWS bundle, Kafka connector, the openlineage-spark 1.54.0 jar (sha1-checked; only `catchup` turns the listener on), pandas, pyarrow, pytest); `src/` mounted read-only; checkpoints in volume `dockwatch_checkpoints`; 2 GB memory limit; `restart: "no"` (on demand only). |

### Website (`web/`), static, no build step
| Path | What it is |
|---|---|
| `index.html` (Live), `insights.html`, `pipeline.html`, `about.html` | Shared shell: skip link, sticky 56 px nav (wordmark, links, freshness pill, theme toggle), full-screen nav overlay below 1120 px (it fades in: `nav-menu-in`, 320 ms, opacity only, so its top row's touch targets never leave the screen while it opens), footer with Bay Wheels attribution, "Not affiliated with Lyft or Bay Wheels", GitHub link, "Data through <date>". Live = title + freshness pill, paused banner (with "Replay a day" when `data/replay.json` exists) and the replay banner, KPI tiles, the map card at `#map` (F2) and the alerts rail `[data-alerts-rail]` beside it (F3), plus the phone peek bar after `.live-layout` and the alerts sheet `[data-alerts-sheet]`; Insights (F4) and Pipeline (F4b), below; About has sources and licences. On touch (`pointer: coarse`) every button and link is a ≥ 44 × 44 px hit area (footer links 44 px tall; icon buttons keep their corners via a square `::after`); in-sentence links in About's text are the WCAG inline exception. |
| `insights.html` (F4) | Page header with "See live station status" (hidden while the "not published yet" card shows its own link), then: four KPI tiles with ⓘ definitions (**Time empty** = Σ `minutes_empty` / (Σ `coverage_hours` × 60) over the daily file; **Time full** likewise; **Worst hour** = the Pacific hour with the highest mean `share_empty` over stations, e.g. "1–2 PM"; **Trips per day** = Σ `trips_started_per_day`) and a caption naming the marts; chart cards (`card`: title, a takeaway computed from the data, the chart, a source caption with the window): the **% of time empty heatmap** (a `<table>` of the 25 stations with the highest mean share empty × 24 Pacific hours, each cell's rounded % as text on its `seq-empty` step, a labelled scale 0–5 / 5–15 / 15–30 / 30–50 / > 50 %, scrolling sideways inside a focusable region with the station column sticky), the **rebalancing table** (need_rank 1–20 of the last 7 days: station, rank, hours empty, hours full, episodes, longest empty) and the **trips vs availability scatter** (SVG, one point per station: trips started per day × % of time empty, `role="img"` with a summary; the takeaway compares the busiest and quietest quarter of stations). No status colours. States: skeletons + `aria-busy`; any 404 → "The insights are not published yet." card with a link to Live; any other failure, or a read with no full answer after 10 s → "The insights couldn’t be loaded. Check your connection, then try again." + Retry. No period control: the marts fix the windows (DESIGN_BACKLOG). |
| `pipeline.html` (F4b) | Page header with "See live station status", then from `data/health.json`: a **status banner** (a status badge = Lucide `circle-check` / `triangle-alert` / `circle-x` + the word Healthy / Degraded / Failing in the status colour, then one line per component: Stream, Data quality, Maintenance with its badge and detail, and "Health as of …"); four stat tiles with ⓘ definitions (**p95 latency**, its sub-line starting with the p95 as a duration, e.g. "21 h 4 min · fetch to silver, …"; **Events per minute**, **Consumer lag**, **Freshness**); chart cards: **latency** (p50 `series-primary` solid, p95 `series-secondary` dashed, the 5 min SLA line in `threshold` with its label, a legend; a minute with no neighbour is drawn as a dot; the y axis "Latency" in one duration unit picked from its maximum: seconds below 2 min, minutes below 2 h, else hours; the takeaway in durations), **events per minute**, **freshness per table** (table, last commit, age, files, status badge), **data quality** (the gate line and every dbt test with Passed / Warning / Failed), **Great Expectations on silver** (a takeaway such as "9 of 10 checks passed; freshness is a warning: Spark runs on demand", a table of every expectation: Check, Column, Result badge Passed / Warning / Failed, Found = unexpected rows, the row count or the newest row's age; a caption with the GE version, suite, rows and when `marts_daily` checked; "No Great Expectations results yet." in a `card-soft` when `quality.expectations` is null or missing) and **compaction** (files before and after per table, as bars with the numbers as text; `skipped` tables say so), then the static **Lineage** card (no data, no JS, no `aria-busy`, inside the same content container, so it hides with the rest on a 404 or error): a takeaway, a `<figure>` with the Marquez screenshot of the full graph `img/lineage-batch.png` (`width` / `height` 1600 × 1000, `loading="lazy"`, an alt text naming the path from the Kafka topic `gbfs.station_status` through Spark and dbt to the export, and the table and list below) and a caption naming OpenLineage from Airflow and the Spark listener, Marquez 0.51.1 and the capture date, then the batch half as a table (Job, Reads, Writes; one row per batch job, datasets by their OpenLineage names, job names on one line; scrolls inside its focusable region) and the Spark half as a lead-in and a list (`ul.lineage__stream`: each replayed Spark job in `code`, breaking after a dot, then what it reads and writes). Status colours appear only in the badges. States as on Insights: skeletons + `aria-busy`, a 404 → "Pipeline health is not published yet." with a link to Live, other failures or 10 s → message + Retry. |
| `img/lineage-batch.png` | The full lineage graph from Marquez web (`python tasks.py lineage-shot` after a real replay of the Airflow runs and a catch-up): from `gbfs.station_status` through the Spark jobs and dbt to the marts, the gate and both exports; 1600 × 1000, 156,889 B (budget 400,000), dark (Marquez web has one theme); the same file as `docs/img/lineage.png`, under the name the frozen `pipeline-lineage.spec.js` pins. It truncates long node labels, which the table and list under it spell out. |
| `css/tokens.css`, `base.css`, `layout.css`, `components.css`, `map.css`, `charts.css` | Tokens from the DESIGN.md front matter (light default; dark via `prefers-color-scheme` or the toggle's `data-theme`); `@font-face`, reset, focus ring, reduced motion; page frame; components (incl. stat tile, segmented control, chip, banner, skeleton, data table with 44 px touch targets, details, bottom sheet, and the alerts rail / alert row / peek bar / alerts sheet with the alert row's container query); the map card, markers, tooltip and legend, and `.live-layout`'s two columns from 1120 px; `charts.css` (Insights): chart cards, the heatmap and its scale (cell text ink, or on-ink on the strongest steps, each ≥ 4.5:1 in both themes), SVG chart gridlines / ticks / points / lines / threshold; and for Pipeline the status badge, status banner, chart legend, compaction bars and the lineage figure (full-width image, `height: auto`), table (wrapping dataset lists; job names on one line: `max-width: none` and `nowrap` instead of the house 16rem row-header cap) and Spark job list. |
| `js/main.js`, `insights.js`, `pipeline.js`, `about.js` | One ES-module entry per page; each calls `shell.js` (`theme.js`, `nav.js`, `freshness.js`, `util/time.js`). `freshness.js` is the page's only `live.json` poll (60 s; a read that hasn't answered in 10 s is aborted and counts as failed, so the pill says "Paused · No data yet" instead of staying on "Loading…", and Live shows its error with Retry) and broadcasts each result as a `dockwatch:live` event (or `dockwatch:live-error`); every pill render (15 s tick and each refresh) also broadcasts `dockwatch:freshness` with the state, so the Live banner flips with the pill. On pages without the banner it adds a visually hidden `aria-live="polite"` region that announces only transitions: "Live data paused — showing the state at 3:42 PM." and "Live data is back." "Data as of" carries the date when the data is from an earlier Pacific day. |
| `js/live/live.js` + `states.js`, `kpis.js`, `list.js`, `details.js`, `replay.js` | Live page controller: region (remembered in `localStorage`, mirrored in the URL with `view` and `problems`), `Map | List`, "Show only problems" (every state except `ok`), legend counts from `live.json` `counts[view]`, polite `aria-live` summary only when counts change, paused banner (`freshnessState(generated_at) === "paused"`, > 5 min) with the reason sentence and "Replay a day", skeleton / error (Retry) / empty states; KPI tiles; the sortable list table (rows updated in place by station id, so focus and scroll survive a refresh); tooltip (≥ 600 px) or bottom sheet (phones). `alerts.json` is read at start (independently of `live.json`) and re-read with each refresh, with a 10 s read timeout; an older read never overwrites a newer one. **Replay mode** (`replay.js`): `HEAD data/replay.json` once when the banner first shows (no button if it's missing); on click it fetches the file (30 s read timeout; then the banner says "The replay couldn’t be loaded. Try again later." and the button can be pressed again), decodes frames incrementally into `live.json`-shaped stations (`state_since` / `last_reported` null, "—" in the list, "Replay, state at 7:35 PM" in details) and steps one frame every `step_s × 1000 / 10` ms with `setInterval`, looping at the end. Focus moves to Exit, and back to "Replay a day" on Exit; Escape does not exit. KPI tiles show the frame's Empty / Full / Bikes, Open alerts "—" ("Alerts aren’t part of the replay") and a caption naming the replayed day. Live refreshes during replay update the pill only; the alerts rail shows its replay note. Not in the URL. |
| `js/live/alerts.js` | The alerts rail (F3): "Open now" (count badge, open alerts by `started`, longest-running first) and "Recently resolved" (by `resolved`, newest first, at most 20); stable sorts. Each row is a `button.alert-row` (`data-episode`, `data-alert-station`): a 12 px state marker, the name (`translate="no"`), "Empty for 34 min" (page clock, counting on with the 15 s tick, also while paused) or "Empty, resolved after 22 min", and "San Francisco · started 3:32 PM" (date added when not today, Pacific). Stations missing from `live.json` get a static `div`. Rows are keyed by `episode_id` and updated in place: text changes only when it differs, rows move only when out of place, a resolving alert moves to the resolved list as the same node, focus goes back to the same episode (or its neighbour) and the body's scroll is kept. States: skeleton rows + `aria-busy` while loading, "No open alerts right now.", "Alerts couldn’t be loaded. Check your connection, then try again." + Retry (only when there is no earlier copy), and the replay note. Desktop (≥ 1120 px): sticky rail under the nav, rows scroll inside, height capped to fit between the nav and the footer. Tablet (600–1119 px): card under the map, body scrolls inside, `@container (min-width: 40rem)` → two columns. Phone: the section isn't shown; the **peek bar** ("61 open alerts" / "1 open alert" / "No open alerts" / "Loading alerts…" / "Alerts couldn’t be loaded"; hidden in replay) opens the alerts sheet (`showModal()`, `is-scroll-locked`), into which the one list body moves; Escape / "Close alerts" return focus to the peek bar; the page gets bottom padding and `scroll-padding-bottom` for the bar. Selecting a row: closes the sheet on phones, switches List → Map, awaits `setRegion()` if the station is in another region, turns "Show only problems" off if needed, then focuses the marker and opens its details. A visually hidden `aria-live="polite"` region in the rail announces new open alerts ("New alert: … is empty." / "3 new alerts."), never on the first read or during replay. |
| `js/pipeline.js` | The Pipeline controller: one `health.json` read (10 s timeout, a newer read wins), badges cloned from the page's `<template>` icons, the tiles, both line charts (each card is marked ready before it is drawn, so the chart can measure its labels), the four tables / bars, ⓘ toggles. |
| `js/insights.js`, `js/insights/model.js`, `js/charts.js` | The Insights controller (reads the three files in parallel with a 10 s timeout, a newer read wins, renders the tiles, heatmap, table and scatter, ⓘ toggles); `model.js`: pure decoding of format 2 (`decode`) and the tile / heatmap / table / takeaway numbers; `charts.js`: shared SVG helpers (`svgEl`, round `ticks`, `durationTicks` (round ticks in one unit: s / min / h), `drawOnResize` with a `ResizeObserver` and one redraw once the web fonts have loaded, `scatter`, `lineChart` with null gaps, an optional threshold line, optional `yTicks`, and a left margin computed from the measured widest y label plus the rotated title), drawn at the container's pixel width so text stays 12 px (fewer ticks below 600 px). |
| `js/util/read.js` | `readJson(url, {timeoutMs})`: `fetch` + JSON with an `AbortController` read timeout (`setTimeout`, cleared in `finally` after the body, so the tests' fake clock drives it); an HTTP error carries `error.status`. Used by `loadAlerts()` (10 s), `loadReplay()` (30 s) and the Insights page (10 s, 404 = not published); `freshness.js` keeps its own proven copy for `live.json`. |
| `js/map/map.js` | SVG map: land + landmarks, one `<circle>` per station keyed by id and updated in place (fill cross-fade + one pulse on a state change, none under reduced motion and none in replay mode), tier groups for draw order, screen-pixel sizes via `--u` / `--r-ok` from a `ResizeObserver`, nearest-station hit testing (10 px pointer, 22 px touch), roving tabindex with arrow keys. No wheel/pinch handling. |
| `fonts/` | IBM Plex Sans 400/500/600 + Mono 400/500, Latin `woff2`, and `OFL.txt`. |
| `licenses/lucide-LICENSE.txt` | Licence for the inline Lucide v1.52.0 icons (sun, moon, menu, x, info, triangle-alert, history, chevrons-up-down, arrow-up, arrow-down, circle-check, circle-x). |
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
  (`terraform fmt -check -recursive -diff`, for CI), `tf-validate`; batch (M4a): `batch-up`, `batch-stop`,
  `airflow <args>`, `dag-test <dag_id> [key=value …]`, `test-airflow`, `scheduler-up`, `scheduler-stop`,
  `trino-sql "<query>"`; `dbt-compile` (the dbt project compiled offline, also in CI); lineage (Loops M and N; `catchup` also writes the Spark listener's events):
  `lineage-up`, `lineage-replay`, `lineage-shot [out]`, `lineage-stop`; measurements (Loop O, `docs/METRICS.md`): `stream-lab`, `stream-lab-stop`, `replay --date … --speed N --skip N --limit N`, `crash-restart` (SIGKILL the stream mid-batch, `catchup`, `verify-lake`, `data/metrics/crash-restart-<UTC>.json`; exit 0 only on zero-diff), `measure window <table> <from> <to>`, `measure files <table> [--runs N]`. Before any task, `tasks.py` refuses to run from a path whose casing differs from the
  containers' (`compose_cwd_refusal`).
- **Host tests:** 157 pass, 7 skipped (`python tasks.py test`; the skips are the two `tests/spark` modules and the five `tests/airflow` modules): measurements and the lab mode (`tests/test_measure.py`, 11, no pyspark / Airflow / OpenLineage / Trino import, Trino and docker faked: nearest-rank percentiles over weighted groups; each micro-batch paired with the snapshot holding its rows, no-data snapshots never, an unpaired batch counted; the window's latencies, batch durations and events per minute from Trino-shaped text; sustained only when every batch is paired and under the trigger; the files measure drops its warm-up run; the Trino client follows `nextUri`, retries a 503 and raises on a query error, and the CLI exits 1; lab names `lake.lab_*`, the production constants equal the Settings defaults, production outputs, unsafe prefixes and production with `latest` refused; `stream-lab` makes one named `compose run` with exactly the lab settings, waits for every query's `sources/0/0`, stops the container when they never appear and refuses beside Spark / Connect / Trino / Marquez; `catchup`, `stream` and `crash-restart` refuse while a lab run holds the JVM; `replay --skip / --limit` on an `FsArchive` keeps its schedule with a fake clock and slow publisher; the crash-restart checkpoint / rpk / verify-lake parsers and the zero-diff rule), Spark lineage (`tests/test_spark_lineage.py`, 7, no pyspark / Airflow / OpenLineage import, on a trimmed real catch-up file `tests/fixtures/lineage/spark-dockwatch-status-stream-20261017T064202Z.jsonl`: the listener off without a lineage dir; one JSON-lines file per run in `dockwatch-spark`; Iceberg tables take the Trino name through the REST symlink while Kafka keeps its name; only runs that read and write are replayed, each streaming write named by its table; the newest catch-up with lineage is posted after the Airflow runs, and a broken line names `file:line`; only `catchup` sets `DOCKWATCH_SPARK_LINEAGE_DIR` and mounts `data/lineage`; a leftover `marquez-db` or `marquez-web` blocks like the API), lineage (`tests/test_lineage.py`, 6, no Airflow / OpenLineage import, Marquez's HTTP answers monkeypatched: the 8 jobs' datasets follow the Trino / S3 / file naming and match the code's table names; `quality_gate` reads `lake.silver.station_status` besides the marts; the replay keeps the newest run of each DAG by parsed event time (also across UTC offsets) in time order and skips events with neither facet; it posts every event and stops with a clear message on a refused connection (naming `lineage-up`) and on a 422 (status, body, file), also through the CLI's exit 1; Marquez refused beside Trino / Spark / Connect and every Trino / Spark / Connect / keepalive task refused beside Marquez, `catchup` before any compose call, the old messages unchanged; `lineage-up` / `lineage-stop` name only the obs services and refuse before any compose call, `batch-up` refuses beside Marquez, and `batch_up` / `airflow_db_up` create `data/lineage`), Great Expectations (`tests/test_expectations.py`, 6, no GE import: the suite covers schema, ranges, uniqueness and freshness with only freshness non-blocking; the batch is the newest 24 hours with a fixed `now`; a real GE result captured on silver (`tests/fixtures/gx/raw_results.json`) becomes ok / warn / fail in any order; a failed blocking expectation fails the gate while freshness only warns; a missing, foreign or incomplete results document fails the gate; health.json carries the expectations block within its budget), pipeline health (`tests/test_health.py`, 5: 1,440 consecutive minutes with events, processed and latency (none when nothing was processed), consumer lag with rows carried into the window and a broker clock ahead of Spark's, the tiles of the last active hour, the stream / table / quality / maintenance thresholds and the worst-of status, every health.json section within its 500,000-byte budget), insights format 2 (`tests/test_insights.py`, 3, through the real `marts_export` task with Trino faked: station and window columns moved out of the rows, every file decodes back to the mart rows, and synthetic marts with every real column at 1,000 stations × 30 dates × 24 hours fit the real 2,000,000-byte budget, daily 1.67 MB, while the format-1 layout does not), maintenance of the batch tables (`tests/test_maintenance_batch.py`, 4: the 16 tables and the DAG mapping, a missing batch table `skipped` without any `EXECUTE` while a missing core table fails, `bronze.trips` optimized one partition per run and not at all when compacted, `skipped` rows not failing `record` while an `error` or a missing row does), trips (`tests/test_trips.py`, 9 plus 1 network test: month keys by `YYYYMM-` prefix, published / missing / pending, legacy and ride headers, a missing or unsafe column refused, both schemas mapped to `bronze.trips` columns, the streamed CSV reader with quotes, CRLF, quoted newlines and a BOM, Pacific times to UTC across DST, the ZIP cache), marts (`tests/test_marts.py`, 8: the quality gate on failed / errored nodes and empty marts, the atomic export with the manifest last, the 2 MB budget and floats rounded to 3 digits, station rows from a GBFS document, SQL literals, `dag-test` refusing beside another `airflow-cli`, the image rebuilt when its inputs change), batch (`tests/test_batch.py`, 22: the 9 maintenance tables, the SQL with its retention parameters, retention below 1 day refused, before / after rows, a changed row count refused, Trino refused beside Spark / Connect and Spark / Connect / `catchup` refused beside Trino, the slot decision (catch up / lost never resets / healthy skips), the keepalive always stopping Connect, touching one captured row only after the connector runs (and again while a confirmed touch leaves the slot pinned; an empty `vans` refused), a keepalive timeout writing a `failed` line with the stuck `confirmed_flush_lsn` and target and failing without retry, the alert line and SNS publish, `default_args`, the evolution summary and day choice, the casing guard refusing a different casing and allowing the creating path or no containers, `dag-test` conf parsing), ops simulator (`tests/test_ops_sim.py`, 7: same seed same ops, job only after 20 min empty, resolved alert closes the job, cancelled jobs deleted, full alerts open nothing, the seeded run touches every table with ≥ 10 job deletes, real `gbfs.alerts` messages parse), CDC (`tests/test_cdc.py`, 16: Connect schema → Iceberg types, a new column plans an ADD COLUMN, a type change is refused, the newest event per key wins by LSN, a delete takes its key from the before image — all on records captured from the real connector in `tests/fixtures/cdc/` — plus checksum order-free and canonical, the Connect / Spark guard, the reset topic list, a lost slot refused with `cdc-reset` in the message, none / reserved / extended allowed, unreserved warns, `connect_up` refuses before compose up and the topic alter, the CLI exits 1 on a lost slot, retention set only on the five ops topics, and `ops.json`'s group covering exactly `ops.public.*` with `retention.ms=-1`), GBFS client, messages/archive, poller, episodes/state rule, exporter (incl. `generated_at` kept without new data and across restarts),
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
  the rail, card, peek bar and sheet in both themes).
  `tests/web/insights.spec.js`: 8 pass (KPI tiles equal the values recomputed from the fixture files, ⓘ
  definitions; the heatmap's 25 stations, cells, buckets and labelled scale, scrolling inside its region at 390 px
  with no page scroll; the rebalancing ranks 1–20 and the 7-day window in the caption; one scatter point per
  station with the computed takeaway and `role="img"` summary; the "not published yet" card on a 404; a 500 →
  message + Retry that recovers; a read that never answers → the message after 10 s; axe at 390 / 1440 px in light
  and dark with data).
  `tests/web/pipeline.spec.js`: 9 pass (the banner's overall badge with its icon and word and the three component
  lines; the four tiles from `health.json`; the latency chart's p50, p95, SLA line and legend; every table in the
  freshness table with its status; the dbt tests and the gate line; compaction before / after; the not-published
  card on a 404; a read that never answers → message + Retry that recovers; axe at 390 / 1440 px, light and dark).
  `tests/web/pipeline-quality.spec.js`: 5 pass (the expectations card lists every result with its badge, column,
  found value, takeaway and caption, and a failed check; "No Great Expectations results yet." when
  `quality.expectations` is null or missing; the latency axis in one duration unit with every y label clear of the
  axis title, by measured boxes, at 320 / 390 / 768 / 1440 px for the fixture, a p95 scaled past 100,000 s and one
  under 120 s; the tile's duration sub-line; axe on the card with and without results in light and dark).
  `tests/web/pipeline-lineage.spec.js`: 4 pass (the static Lineage card with no `aria-busy`, the screenshot loaded
  with `width` / `height` equal to its real size, an alt text naming the graph's path and the table, the figcaption
  naming OpenLineage and Marquez 0.51.1, and the card hidden with the content on a 404; the table's 8 jobs with
  exactly what each reads and writes; no page scroll at 320 / 390 px with the image inside the card at its aspect;
  axe on the card at 390 / 1440 px in light and dark).
  `tests/web/pipeline-lineage-full.spec.js`: 3 pass (the image's alt, caption and takeaway name the full graph from
  `gbfs.station_status` through Spark; the list after the table names the 4 Spark jobs with exactly what each reads
  and writes; every table job name on one line at 320 / 390 / 768 / 1440 px with no page scroll and the Spark names
  inside the card).
  `tests/web/launch.spec.js`: 3 pass (48 full-page screenshots, `<page>-<width>-<theme>.png` in the test's output
  dir, each with no horizontal scroll, no pixel comparison; Tab reaches every control in DOM reading order on all
  four pages at 1440 px, one stop per radio group, and the nav overlay traps Tab and returns focus on Escape; with
  reduced motion nothing animates for longer than 1 ms after load or after a 60 s refresh).
  81 Playwright tests in total. Tests route `data/*.json` to `tests/web/fixtures/` (a real
  641-station export, `replay.json` built from the real archive with `--step 600`, 51 frames, and in
  `fixtures/insights/` a real format-2 export of `dag-test marts_daily`, 645 stations, and `health.json` from a real
  `dag-test pipeline_health`) and fake time with
  `page.clock`; `data/replay.json` is a 404 unless a test asks for the fixture.
- **Airflow tests:** 22 pass in the Airflow image (`python tasks.py test-airflow`; 4 in `tests/airflow/test_loopm_lineage.py`: every task of every DAG carries exactly its planned inlets / outlets as OpenLineage `Dataset`s, read through the provider's `ExtractorManager.convert_to_ol_dataset` (the others none); `dbt_build`'s inputs are every table in `sources.yml` and its outputs the marts in `models/marts/*.sql` in the profile's `lake.marts` (staging stays ephemeral, no `stg_*` dataset); in a subprocess with OpenLineage on and the file transport into a temp dir, the `OpenLineageListener` is registered, the DagBag has no import errors and every DAG round-trips through `SerializedDAG` with its tasks and edges; a fresh interpreter that imports only `batch/lineage.py` leaves a cyclic object with a finalizer and forks: the child's GC pass never finalizes it, the child exits 0 and the parent still finalizes it after its own pass, while the provider's `_fork_execute` still calls `os.fork()`; 3 in `tests/airflow/test_loopl_gx.py`: `/opt/gx-venv` has exactly the pinned set (GE 1.24.0, SQLAlchemy 2.1.4) while Airflow's python cannot import GE and keeps SQLAlchemy 1.4; the suite in the GE venv passes a clean pandas frame and flags a duplicate row, a negative count, an unknown state, an over-capacity row and an old batch each in exactly their expectation; `marts_quality_gate` runs the checkpoint subprocess and merges it, fails without retry on a failed blocking expectation and retries on a crashed run; 3 in `tests/airflow/test_loopk_dags.py`: `iceberg_maintenance` maps over the 16 tables of `maintenance.ALL_TABLES`, one at a time; `pipeline_health` is `@hourly`, paused, `guard` → `minutely` → `export` with an `sla`; the minute series becomes the Arrow table with `pipeline_minutely`'s types; 5 in `tests/airflow/test_m4b_dags.py`: the two M4b DAGs' shape, the trip month from the param or the data interval, `dbt parse` with `/opt/dbt-venv`, and the Arrow table with `bronze.trips` types; 7 in `tests/airflow/test_dags.py` with a
  throwaway SQLite metadata DB): the DagBag imports the six DAGs without errors; every task retries 3 times with
  exponential backoff (1 min, cap 30 min), a timeout and the failure callback, and every last task has an `sla`;
  schedules and `catchup=False`; real `TaskInstance.next_retry_datetime` delays grow 60–120 s, 120–240 s, … up to
  1,800 s; a real `dag.test()` of a failing task writes one `task_failed` line; an SLA miss writes a `sla_miss` line; `runstate.fail_stale` fails a
  DAG's `running` run and its task instance, leaving a `success` run and another DAG's run untouched.
  The modules skip on the host through `pytest.importorskip("airflow.models")`.
- **Ops DB integration tests** (`tests/test_ops_db.py`, marked `integration`, not in CI): 6 pass after `python tasks.py ops-schema` (foreign keys, CHECK constraints, `updated_at` trigger, REPLICA IDENTITY FULL, the publication's five tables, each in a transaction that is rolled back, so they leave no rows and no CDC events; and `slot_status()` reading `reserved` from a temporary physical slot the test creates and drops, never touching `dockwatch_ops`).
- `inspect`, `verify-lake` and `sql` run Spark with whole-stage codegen **off** (a JVM crash otherwise; see DEVLOG).
- Lint: ruff, line length 120.

### CI (`.github/workflows/ci.yml`, GitHub Actions)
Runs on pushes to `main`, on pull requests and by hand (`workflow_dispatch`). Top-level `permissions: contents: read`;
a newer run on the same ref cancels the older one (`concurrency: ci-<ref>`). Every job runs on `ubuntu-24.04`; actions
are pinned to version tags (major tags, and the exact `v10.3.0` for setup-uv, which has no major tags):
`actions/checkout@v5`, `actions/setup-python@v7`, `actions/setup-node@v7`, `actions/cache@v6`,
`actions/upload-artifact@v7`, `astral-sh/setup-uv@v10.3.0`, all on the `node24` action runtime; no matrix, so the check names stay fixed. Five jobs:

| Job | Runs | Timeout |
|---|---|---|
| `Python lint + tests` | `astral-sh/setup-uv` (uv 0.12.11, Python 3.12), `uv sync --locked`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run pytest -q` (the local commands), then the step "dbt compile (offline, no Trino)": `uv run python tasks.py dbt-compile` | 10 min |
| `Spark transform tests` | `docker build -t dockwatch-spark:3.5.5 infra/spark`, then `python tasks.py test-spark` (same image and command as locally) | 20 min |
| `Terraform fmt + validate` | `python tasks.py tf-fmt-check`, `python tasks.py tf-validate` (both in `hashicorp/terraform:1.9`, `init -backend=false`, no AWS credentials) | 10 min |
| `Playwright suite` | Node 24, `npm ci`, `npx playwright install --with-deps chromium`, `npx playwright test --retries=0` (all 81 tests); uploads `test-results/` (traces) on failure | 30 min |
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
  `node:24-bookworm`, `CI=true`) on a copy of the working tree; the `spark` and `terraform` commands are the local
  `test-spark` / `tf-fmt-check` / `tf-validate` checks.
- First run on GitHub: **passed** — run 37875703602 on commit 9a42ba4 (2026-10-09 02:40–02:44 UTC), all five jobs green: Python lint + tests (12 s), Terraform fmt + validate (18 s), Spark transform tests (56 s), Playwright suite (2 min 28 s), Playwright repeat (alerts + shell) (3 min 22 s). https://github.com/braaaeeedyn/dockwatch/actions/runs/37875703602
- Latest run on Node 24 with the bumped actions: **passed** — run 38015779233 on commit 4f0dd72, all five jobs green, no deprecation annotations. https://github.com/braaaeeedyn/dockwatch/actions/runs/38015779233
- **Not in CI:** `deploy.yml` (publishing `web/` to S3 needs the AWS account), `terraform plan` (needs the AWS
  account and the GitHub OIDC role), the integration and network tests, the Kafka
  stack and `verify-lake`, and Airflow / Trino (`test-airflow`, `dag-test`; the host `tests/test_batch.py` does run
  in the Python job).

---

## Known gaps and facts to remember
- **Trino Metaspace headroom:** `jvm.config` caps Metaspace at **256 MiB**. The user raised it from 192 MiB for
  M4b. `mem_limit` is unchanged at 2 GiB, and Metaspace is native memory inside that limit.
  - **The full batch run in one JVM** (`iceberg_maintenance` → `partition_evolution_demo` → trips 2019-01 → the
    12-month backfill → rerun → three `marts_daily` builds) ended at 192.4 / 197.6 MiB used / committed in Loop J,
    which leaves 58.4 MiB of headroom. Since Loop L `marts_daily` (with Great Expectations) runs on a freshly
    restarted JVM: 179.3 / 184.1 and 179.8 / 184.7 MiB used / committed after it in the two runs.
  - A full GC frees almost nothing, and each extra dbt build adds about 1–3 MiB.
  - **Container memory:** Trino peaked at 2,013 MiB of 2,048 (`docker stats`). The cgroup reached its limit on page
    cache, RSS peaked at 1,838 MiB, and there was no OOM kill.
  - The next Trino-heavy milestone needs the user before any further limit change.
- **Insights size headroom:** format 2 projects the daily file at ~1.26 MB for 1,000 stations × 30 dates (1.67 MB in
  the host test with every float at full 3 digits) and the trips file at ~0.80 MB; the budget is 2,000,000 B per
  file. The page reads all three files (~0.85 MB today) on every visit; there is no gzip yet.
- **Pipeline latency reflects on-demand Spark:** silver only gets rows when someone runs `catchup` / `stream`, so most
  minutes process nothing and a catchup minute's p95 is the backlog's age (hours). With `stream` running (Loop O's
  hour, `docs/METRICS.md`) fetch to silver was p50 / p95 59.3 s / 60.9 s and fetch to queryable 64.1 s / 68.7 s: the
  60 s poll and the 60 s trigger were in phase, so a snapshot waits up to one trigger, then ~6–10 s to commit. `fetched_ts` comes from the
  Windows host's clock and `processed_ts` from the Docker VM's; a skew between them shifts every latency by that
  many seconds. Consumer lag counts only records already in silver (published before a minute's end, processed
  after it); records not processed at all yet are invisible to the lake, and the freshness tile shows that case.
- **`bronze.trips` compaction is spread over runs:** `iceberg_maintenance` optimizes one `trip_month` partition per
  run (the month with the most files), because one optimize over all 13 months took Trino to 2,024 MiB of 2 GiB.
  After a 12-month reload it takes 13 daily runs to compact every month; until then the table has a few small files
  per month (~3.5 MB each), which only costs Trino some extra file opens.
- **pyiceberg is a third Iceberg writer** after Spark and Trino, but only for `bronze.trips`,
  `bronze.station_information` and `metrics.pipeline_minutely`.
- **Great Expectations works around a GE 1.24 gap on Trino:** GE finds no columns in a Trino query asset, so
  `gx_run.reflect_trino_queries()` replaces GE's column-reflection fallback (module global) with a `LIMIT 0` read of
  the query. Re-check it on every GE upgrade and drop it once GE reflects query assets itself.
- **Freshness always warns in `marts_daily`:** Spark never runs beside Trino, so silver is never under 5 min old
  while the checkpoint runs; the warning is honest, not a fault.
- **dbt's `aws` target (Athena) is declared but untested:** there is no AWS account yet (M6).
- **Trip history gaps:** 2020-04 is missing from the bucket, and the 2017 monthlies are not published per month.
  `published` records both as `missing` and skips them.
- **Trip times are read as Pacific local time.** That is an assumption, but the weekday peak hours (17, 18 and 8
  Pacific) support it.
- **Leftovers in SeaweedFS:** the files of the first, broken `bronze.trips` (orphans under
  `warehouse/bronze/trips-c13d9cb6…/`), plus Loop I's `hive-metastore/` and `raw/trips/` objects. Nothing reads
  them.
- Airflow's backfill runner can log `DeadlockDetected` on `dag_run` and carry on; the runs still finish.
- Git: commits are made at the end of each loop, not by tooling during it (latest: `0aa1288`, Loop N as finished in Loop N2; Loop O's work sits in the working tree until its commit).
- **Spark lineage comes from catch-up runs only:** the listener runs in `catchup` (the same queries and tables as
  `stream`), so the graph shows what the streaming job does without `stream` writing events every minute; `stream`
  and the CDC apply emit nothing. Inside `foreachBatch` Spark sees no input, so the episodes MERGE reads only its own
  table and the alerts / station-state Kafka sinks are not in the graph. Spark names Iceberg tables by location; the
  replay renames them through their REST-catalog symlink, so a table written without that symlink would stay a
  separate node.
- **Marquez is empty until replayed:** it has no volume, so each `lineage-up` starts empty and `lineage-replay`
  rebuilds it from `data/lineage` in seconds. A Marquez left running (any of `marquez-api`, `marquez-db`, `marquez-web`) makes `batch-up`, `catchup`, `stream` and the
  DAG guards refuse; `python tasks.py lineage-stop` first.
- **`data/lineage` grows by one ~7 KB file per Airflow event** (~25 per `marts_daily` + `pipeline_health` +
  `trip_history_monthly` set of runs; the 18-task `iceberg_maintenance` writes ~37), plus one
  `spark-dockwatch-status-stream-<UTC>.jsonl` per `catchup` (272,647 B for ~18 min of backlog). Nothing prunes it; it is
  safe to delete between sessions, since the replay only needs the newest run of each DAG and the newest catch-up
  with lineage. Two runs with the same logical date
  (a backfill rerun) share OpenLineage run ids; the replay still picks the latest events and Marquez upserts.
- **The lineage screenshot is dark on both site themes** (Marquez web has one theme) and truncates long node labels;
  the table and the Spark job list under it are the text alternative (DESIGN_BACKLOG #63 / #65).
- **Trino on the first JVM of a full check run** (maintenance, partition evolution, the trips stages) reached
  2,022 of 2,048 MiB in Loop M's baseline sampler and 2,021 MiB in Loop M2's Builder run (the maintenance checks),
  above the 1,950 MiB flag line; OpenLineage runs in the Airflow
  processes, not in Trino. `marts_daily` and `pipeline_health` run on a restarted JVM. No limit changed; any change
  needs the user.
- **A crashed Linux process under WSL leaves a ~1.8 GB dump** in `%LOCALAPPDATA%\Temp\wsl-crashes` (named
  `wsl-crash-<epoch>-<pid>-<executable>-<signal>.dmp`), and the capture stalls the run for ~6 s. Loop M's crashed
  OpenLineage child left `wsl-crash-1792180714-62-_usr_local_bin_python3.12-11.dmp` (1.8 GB); deleting it is the
  user's call. `metric-m2-events` counts the python3.12 dumps and those after the run's first dag-test.
- **Path casing (Docker on Windows):** the folder is `DockWatch` on disk, but every container was created from Git Bash
  as `/c/Users/brady/coding/dockwatch`. Running Docker Compose from a shell that sees a different casing (cmd.exe,
  PowerShell, Python `shell=True`) makes Compose recreate `ops-db` mid-run, which drops iceberg-rest's catalog
  connections and fails the CDC apply (`UncheckedSQLException ... iceberg_tables`). Always run tasks from Git Bash in
  the lowercase path. Recovery (keeps the volume): `docker compose --profile stream up -d --wait --no-deps ops-db &&
  docker compose --profile stream restart iceberg-rest`. `tasks.py` now refuses every task when the shell's path
  differs from the containers' `working_dir` label (it cannot protect raw `docker compose` commands).
- **Local / CI Node split:** CI runs Node 24; this machine runs Node **v20.18.0**, which is past end of life
  (2026-04-30) and outside Playwright 1.64's documented support ("latest 22.x, 24.x or 26.x"). It still installs and
  runs the suite (Playwright itself declares `>=20`), so `engines` is `>=20`. **Upgrade step (the user's):** install
  Node 24 LTS (the nodejs.org installer, or nvm-windows `nvm install 24` + `nvm use 24`), then `npm ci` and
  `npx playwright install chromium`, rerun `env-preflight` and the web checks, then raise `engines` to `>=24` and the
  env-preflight floor to 24.
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
- **Windows port-allocation fault in local Playwright runs:** on this host a local navigation can rarely fail with
  `net::ERR_NO_BUFFER_SPACE`, when Windows fails to allocate an ephemeral port for one connect (Tcpip event 4231, "all
  such ports being in use"; logged on this host since 2025-10-28, before DockWatch). Seen 2 in 34 runs of
  `accept-alerts-stable` (Loop I, Loop M2); CI (Linux) is unaffected. The loop runner drains TIME_WAIT on :5179
  with `.loop/checks/m3/envnet.py wait 180`, a local loop-runner helper that is not in the repo, right before `accept-alerts-stable` and `accept-web-full-suite`, the
  two densest checks; that lowers the odds, it does not remove them. U1, decided by the user: a host Playwright check
  that fails only with `net::ERR_NO_BUFFER_SPACE` is re-run once, at once, and the re-run must pass completely (both
  logs kept); any other error is a real failure. The fix options (the `tests/web/serve.py` reset-on-close edit, a
  wider port range, quitting Logitech G HUB or Lenovo's network filter during runs) wait on the user.
- **Full check runs need host memory outside Docker:** the memgates need 1,536 MiB host free with Trino at
  ~1.8–1.9 GiB. On this 16 GB host Chrome (~3.5 GB) plus the Docker VM's page cache (vmmemWSL keeps it until the VM
  idles) left 969–1,315 MiB at the trips and health gates in Loop N's Checker run. The loop runner reads host free
  with `.loop/checks/n2/hostmem.py`, a local loop-runner helper outside the repo, before the gated chunks (3,300 MiB
  before `batch-up`, 1,700 before the other memgates) and pauses for the user instead of running into a gate. The
  permanent fixes (closing Chrome for runs, `.wslconfig` memory reclaim, dropping the VM's cache, any `mem_limit` /
  JVM change) are the user's (U2). Confirmed by Loop N2's Checker with Chrome closed: 6,482 MiB at the start and
  2,779 / 3,171 MiB at the trips / health gates, every wait reached on its first call. A pause with Trino up now
  stops Trino first (an idle session at low memory is when the reaper killed the producer / exporter).
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
- **The slot can be lost after > 1 GB of WAL with Connect stopped:** the slot `dockwatch_ops` stays (inactive)
  between CDC runs so a later `connect-up` resumes where it stopped; WAL is cluster-wide, so it also holds the
  `iceberg_catalog` database's WAL. `max_slot_wal_keep_size=1024MB` bounds that, at a price: if more than 1 GB of
  WAL is written before the next `connect-up`, Postgres invalidates the slot (`wal_status = 'lost'`), `connect-up`
  refuses, and CDC has to be rebuilt with `python tasks.py cdc-reset` (drops lake.ops.*, the slot, the ops tables,
  re-created empty, and the CDC topics) and a fresh snapshot. `cdc_slot_keepalive` catches the slot up and alerts
  on a low `safe_wal_size`, but only while the scheduler runs (it does not run 24/7) or when run by hand.
- **Each keepalive catch-up writes to the ops data:** after a restart Debezium confirms nothing until it streams a
  captured change past its stored offset, and WAL from other databases, empty transactions and
  `pg_logical_emit_message` never reach it (the `heartbeat.action.query` experiment changed nothing). So each
  catch-up bumps `updated_at` of the lowest-id van, usually twice: one or two `u` events on `ops.public.vans` per
  catch-up (at most 24 a day hourly). `cdc-catchup` applies them like any other change, and `cdc-e2e` resets the
  topics first, so its per-op counts are unaffected; `cdc-verify` without a `cdc-catchup` after a keepalive sees
  that van's newer `updated_at` in Postgres.
- **The CDC topics grow without bound:** `ops.public.*` have `retention.ms=-1` so a replay rebuild never misses old
  rows; one e2e adds ~3k events (a few MB) and `cdc-reset` clears them. Decided in M4a: they stay
  `cleanup.policy=delete` (compaction would need tombstones, which are off, and would break the per-op CDC counts);
  revisit (compaction plus snapshot-based rebuilds) past ~1 GB or ~1M events. The Iceberg side (`lake.ops.*`) is
  compacted by `iceberg_maintenance`.
- **Hard deletes and replay:** a replay that splits a deleted row's insert and delete into different batches
  re-inserts the row for one batch, then deletes it again; the end state is identical (checked by `cdc-verify`).
- **Type changes are not applied:** a changed Postgres column type, or a Connect type the apply does not know (e.g.
  `numeric`, `date`), stops the apply with `SchemaError`. Columns are add-only; a dropped Postgres column would stay
  in Iceberg (and `cdc-verify` would still compare only Postgres's columns).
- **Small files:** each streaming query writes ≥ 1 file per minute (silver ≈ 45 KB/file); `iceberg_maintenance`
  compacts them (405 → 4 silver files in its first run; in Loop O 419 → 11 silver and 79 → 11 bronze files, the
  station query's median 287 → 163 ms, `docs/METRICS.md`), but only when someone runs it: the scheduler does not
  run 24/7 and the DAGs are paused by default.
- **Lab data stays until the user deletes it:** Loop O's replays left `lake.lab_bronze.*` / `lake.lab_silver.*`
  (698,972 rows each in `station_status` after the Builder's checks), `gbfs.alerts.lab`, `dockwatch.station_state.lab`, the replayed records in
  `gbfs.station_status.replay` (7-day retention) and `/checkpoints/lab/<UTC>` dirs in the checkpoints volume. No loop
  deletes them; each `stream-lab` adds a checkpoint dir and the Checker's lab replay adds ~39,000 rows.
- **`docker.sock` in the Airflow containers** is root-equivalent access to the Docker VM. Local only; the code only
  lists containers and starts / stops `connect`.
- `live.json` is ~200 KB uncompressed; fine for now, worth trimming or gzip-serving later (F5).
- **AWS steps are manual** and not done; `checkpoint_root` on AWS will need `s3a://` + hadoop-aws jars (not in the image yet).
- Some stations report a `last_reported` weeks old → `offline`. 35 stations have no `region_id` → placed by coordinates.
- `station_information` rows arrive in a different order on every poll (the content hash sorts them).
- Bay Wheels currently reports 0 scooters at every station; the evolved scooter columns are 0 for new rows, null for old.
