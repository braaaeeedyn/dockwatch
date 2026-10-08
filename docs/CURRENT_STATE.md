# DockWatch: current state

> **What this file is:** a description of DockWatch *exactly as it is right now* and what is being worked on.
> It is rewritten whenever the code changes. If something is removed from the code, it is removed from here too.
> For the full history, including what was tried and removed, see [`DEVLOG.md`](DEVLOG.md).
> For the plan, see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).

**Last updated:** 2026-10-08 · **Milestones:** M0 ✅ (except manual AWS steps) · M1 ✅ · M2 ✅ except
partition evolution (moved to M4) · F1 ✅ · F2 ✅ (plus F-1 / F-2 / touch-target fixes) · DESIGN.md v1.0.1

---

## Working on now

| What | Why | Status |
|---|---|---|
| Nothing in progress | The fixes from the F2 review (honest freshness, list focus, 44 px touch targets) are built and tested. | Next: M3. |

Done most recently: three fixes from the F2 review.
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
keyboard roving focus, in-place refresh every 60 s, stale banner. The M2 spot-check passed: 10/10 stations match
the public feed for the same snapshot. Gaps against the frozen DESIGN.md are listed in `docs/DESIGN_BACKLOG.md`.

Before that: **F1**, the site shell: four pages, design tokens in both themes, self-hosted IBM Plex, freshness
pill, nav overlay.

Before that: the Iceberg catalog on Postgres is **verified** (`python tasks.py verify-lake` passes: every Kafka
offset in bronze exactly once, 0 gaps, 0 silver duplicates, fresh commits on all four tables), and Spark now runs
**on demand** (`catchup` / `stream`), never restarted by Docker. Details in DEVLOG 2026-10-08.

Next loop: **M3** (ops DB + Debezium CDC). F3 (alerts rail beside the map) comes later.

### Running right now on this machine
| Process | How it was started | Stop with |
|---|---|---|
| GBFS producer | `uv run python -m dockwatch.producer run` (log: `data/producer-run.log`) | Ctrl+C / kill the process |
| Exporter | `uv run python -m dockwatch.exporter` (log: `data/exporter.log`) | Ctrl+C / kill the process |
| Stream stack | `python tasks.py up stream` | `python tasks.py down` |
| Spark streaming job | **Not running** (on demand only). Container `status-stream` has `restart: "no"`. | — |

Spark on demand: `python tasks.py catchup` processes everything in Kafka since the last run (`availableNow`) and
exits (~30 s for a few minutes of backlog; Kafka keeps 7 days). `python tasks.py stream` keeps it running (one
micro-batch per minute) until `python tasks.py stream-stop`. Run one Spark JVM at a time: stop `stream` before
`catchup`, `verify-lake`, `inspect`, `sql` or `test-spark`. `live.json` only moves forward while Spark runs (the exporter
skips rewriting it while `data_as_of` stands still), so run `catchup` before looking at the exporter output.

Memory: the streaming job uses about 1.5–1.9 GB (limit 2 GB); the rest of the stack under 1 GB. The host often has
only ~1–3 GB free; the 24/7 job stalled there (driver heartbeat timeout), which is why it is now on demand.

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
```

---

## What exists right now

### Documents (`docs/`)
| File | What it is |
|---|---|
| `DOCKWATCH_PLAN.md` | The original project plan (what and why). Unchanged. |
| `IMPLEMENTATION_PLAN.md` | Milestones M0–M7 / F1–F5, decisions (incl. M2 decisions), progress checkboxes. |
| `DESIGN.md` | DockWatch design system **v1.0.1**, frozen until launch (F5); v1.0.1 = accessibility fix (44 px touch targets in the list table). |
| `DESIGN_BACKLOG.md` | Gaps and judgment calls found while building against the frozen DESIGN.md (14 from F1, 18 from F2, 3 from the F2 review). |
| `CONCEPTS.md` | Kafka, stream processing and Iceberg explained through this codebase. |
| `reference/DESIGN_SOURCE_transitpulse.md` | The TransitPulse design file that was here before; reference only. |
| `CURRENT_STATE.md` / `DEVLOG.md` | This file / append-only history. |

### Python package `src/dockwatch/`
Host code runs on Python 3.12 (uv). Code under `streaming/` also runs in the Spark image (Python 3.10), so it avoids 3.11+ features.

| Module | What it does | Why it's built this way |
|---|---|---|
| `config.py` | One `Settings` object from env / `.env` (prefix `DOCKWATCH_`): target, GBFS, Kafka topics, S3, Iceberg, checkpoints, trigger seconds and `trigger_mode` (`processing_time` / `available_now`), watermark, state thresholds, exporter. | Switching to AWS is a config change. |
| `gbfs/models.py`, `gbfs/client.py` | Typed GBFS records (unknown fields kept); discovery → GBFS 2.3; re-discover on 404; custom User-Agent. | Feeds move and grow. |
| `producer/` | `messages.py` (explode per station, DLQ rejects, order-insensitive content hash), `archive.py` (gzip JSON to S3 or a folder), `kafka.py` (topic specs, idempotent publisher), `poller.py` (ttl-paced poll loop), `__main__.py` (`run`, `once`, `setup`, `replay`). | Keyed by station for per-station ordering; idempotent to avoid retry duplicates. |
| `streaming/episodes.py` | **Plain Python:** `Thresholds`, `classify()` (station state rule), `EpisodeState` + `advance()` (episode/alert state machine). | Unit-tested without Spark; the Spark rule is tested to give the same answers. |
| `streaming/transforms.py` | Spark: `parse_kafka()`, `with_state()`, `to_silver()`, `availability_windows()`. | Pure DataFrame functions, tested on tiny inputs. |
| `streaming/lake.py` | Spark/Iceberg session config (`local` = REST catalog + SeaweedFS, `aws` = Glue), table DDL, `migrate()` for schema evolution (`EVOLUTIONS` list). | Tables evolve in place; fresh and old installs share one history. |
| `streaming/run_mode.py` | `trigger_options()`: `processingTime` every `trigger_seconds`, or `availableNow`. No pyspark import. | Host-testable. |
| `streaming/status_stream.py` | The streaming job (four queries, see flow above). In `available_now` mode it waits for every query to drain Kafka, then exits (non-zero if a query failed). | One app keeps memory low; separate checkpoints recover independently; both modes share them. |
| `streaming/inspect_tables.py`, `streaming/sql.py` | Health/consistency report (counts, freshness, duplicates, Kafka-offset gaps, states, episodes, file sizes); `--check` (`verify-lake`) asserts it and exits 1 on failure (duplicates, offset gaps or missing partitions, silver > 5 min behind bronze, no Iceberg commit in 60 min). One-off SQL. | Proof for the restart test, the catalog fix and day-to-day checks. |
| `exporter/build.py`, `exporter/__main__.py` | Builds `live.json` (641 stations with name, code, lat/lon, region, map view, bikes/e-bikes/docks, state, state since, last reported; counts per view) and `alerts.json` (open alerts longest first, last 20 resolved). Atomic writes. `data_as_of` = newest feed snapshot in the export; `generated_at` = time of the last export whose `data_as_of` advanced (`carry_generated_at`, `has_new_data`). Exports without new data are not written; at start the previous `live.json` is read (`read_previous`), so a restart does not make old data look new. `alerts.json` gets the same `generated_at`. | The static site only reads these files, and its freshness pill reads `generated_at`, so it must not move when the data doesn't. Stations without a `region_id` are placed in a view by coordinates. Edge: with no previous `live.json` (first run), the first export is written with `generated_at` = now, so the site shows Live for up to 2 min on old Kafka data, then goes stale. |
| `geo/build.py`, `geo/__main__.py` | Map-shape build step: Census cartographic boundary counties (`cb_2023_us_county_500k`, downloaded once to `data/geo/`) → per-view projection, rectangle clip (land kept 320 units beyond the square view box for wide/tall map cards), Douglas–Peucker simplify → `web/geo/<view>.json`. `pyshp` is in uv group `geo`, imported lazily. | Run once; output committed. Stations are projected in the browser with the same params, so new stations need no rebuild. |
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

### Kafka topics (Redpanda)
| Topic | Partitions | Policy | Written by |
|---|---|---|---|
| `gbfs.station_status` | 6 | 7 days | producer (~641 msg/min) |
| `gbfs.station_information` | 1 | compacted | producer (only on change) |
| `gbfs.dlq` | 1 | 28 days | producer (0 rows so far) |
| `gbfs.station_status.replay` | 6 | 7 days | `producer replay` (unused so far) |
| `gbfs.alerts` | 1 | 28 days | Spark episodes query (key `alert_id`, at-least-once) |
| `dockwatch.station_state` | 1 | compacted | Spark episodes query (key `station_id`) |

### Local stack (`docker-compose.yml`)
| Service | Profile | Port | Purpose |
|---|---|---|---|
| `redpanda` | stream | 19092, 18081 | Kafka API. |
| `console` | stream | 8088 | Redpanda Console. |
| `s3` (SeaweedFS 3.80) | stream | 8333 | Local S3; bucket `dockwatch`. |
| `iceberg-rest` | stream | 8181 | Iceberg REST catalog; its table pointers live in Postgres database `iceberg_catalog` on `ops-db`. |
| `ops-db` (Postgres 16) | stream | 5434 | Database `ops` (for M3, empty) and database `iceberg_catalog` (catalog). `infra/postgres/init/` creates the catalog DB on a fresh volume. |
| `status-stream` | spark | 4040 (Spark UI) | The M2 streaming job; image `dockwatch-spark:3.5.5` (`infra/spark/Dockerfile`: Spark 3.5.5, Java 17, Iceberg 1.6.1 + AWS bundle, Kafka connector, pandas, pyarrow, pytest); `src/` mounted read-only; checkpoints in volume `dockwatch_checkpoints`; 2 GB memory limit; `restart: "no"` (on demand only). |

### Website (`web/`), static, no build step
| Path | What it is |
|---|---|
| `index.html` (Live), `insights.html`, `pipeline.html`, `about.html` | Shared shell: skip link, sticky 56 px nav (wordmark, links, freshness pill, theme toggle), full-screen nav overlay below 1120 px, footer with Bay Wheels attribution, "Not affiliated with Lyft or Bay Wheels", GitHub link, "Data through <date>". Live = title + freshness pill, stale banner, KPI tiles, the map card at `#map` (F2); Insights / Pipeline show "Arrives with M4 / M5" empty states; About has sources and licences. |
| `css/tokens.css`, `base.css`, `layout.css`, `components.css`, `map.css` | Tokens from the DESIGN.md front matter (light default; dark via `prefers-color-scheme` or the toggle's `data-theme`); `@font-face`, reset, focus ring, reduced motion; page frame; components (incl. stat tile, segmented control, chip, banner, skeleton, data table with 44 px touch targets, details, bottom sheet); the map card, markers, tooltip and legend. |
| `js/main.js`, `insights.js`, `pipeline.js`, `about.js` | One ES-module entry per page; each calls `shell.js` (`theme.js`, `nav.js`, `freshness.js`, `util/time.js`). `freshness.js` is the page's only `live.json` poll (60 s) and broadcasts each result as a `dockwatch:live` event (or `dockwatch:live-error`). |
| `js/live/live.js` + `states.js`, `kpis.js`, `list.js`, `details.js` | Live page controller: region (remembered in `localStorage`, mirrored in the URL with `view` and `problems`), `Map | List`, "Show only problems" (every state except `ok`), legend counts from `live.json` `counts[view]`, polite `aria-live` summary only when counts change, stale banner (`generated_at` > 5 min), skeleton / error (Retry) / empty states; KPI tiles; the sortable list table (rows updated in place by station id, so focus and scroll survive a refresh); tooltip (≥ 600 px) or bottom sheet (phones). `alerts.json` is re-read with each refresh. |
| `js/map/map.js` | SVG map: land + landmarks, one `<circle>` per station keyed by id and updated in place (fill cross-fade + one pulse on a state change, none under reduced motion), tier groups for draw order, screen-pixel sizes via `--u` / `--r-ok` from a `ResizeObserver`, nearest-station hit testing (10 px pointer, 22 px touch), roving tabindex with arrow keys. No wheel/pinch handling. |
| `fonts/` | IBM Plex Sans 400/500/600 + Mono 400/500, Latin `woff2`, and `OFL.txt`. |
| `licenses/lucide-LICENSE.txt` | Licence for the inline Lucide v1.52.0 icons (sun, moon, menu, x, info, triangle-alert, chevrons-up-down, arrow-up, arrow-down). |
| `data/live.json`, `data/alerts.json` | Written by the exporter (git-ignored). The freshness pill reads `generated_at` from `live.json` (time of the last export with new data): Live < 2 min, Delayed 2–5, Paused > 5. The map, legend, list and KPI tiles read the stations, `counts` and `bikes_available`; the Open alerts tile reads `alerts.json` `open`. |
| `geo/sf.json`, `eastbay.json`, `sj.json` | Map shapes per region view from `python tasks.py geo` (committed): projection params, `0 0 1000 1000` view box, simplified land paths, 3–4 landmarks. Drawn by `js/map/map.js`. |
| `js/map/project.js` | lon/lat → view box with a view's projection; mirrors `dockwatch.geo.build.project` (stations are projected in the browser). |

Serve locally with `python -m http.server 5179 --bind 127.0.0.1 --directory web`.

### Infrastructure (`infra/terraform/`), validated, **not applied**
S3 bucket (private, encrypted, lifecycle), Glue databases `dockwatch_{bronze,silver,ops,marts,metrics}`, Athena workgroup
(1 GB scan limit), SNS topic, $1 / $5 budgets, IAM roles (spark-writer, athena-reader, alert-lambda, github-ci via OIDC).
Remote state backend commented out until the AWS account exists.

### Tooling and tests
- `tasks.py` (`Makefile` forwards): `lint`, `fmt`, `test`, `test-integration`, `test-spark`, `up [profile]`, `down`, `ps`,
  `setup`, `produce`, `produce-once`, `replay`, `stream`, `stream-logs`, `stream-stop`, `catchup`, `inspect`,
  `verify-lake`, `sql "<query>"`, `export`, `geo`, `tf-fmt`, `tf-validate`.
- **Host tests:** 46 pass, 1 skipped (`python tasks.py test`): GBFS client, messages/archive, poller, episodes/state rule, exporter (incl. `generated_at` kept without new data and across restarts),
  alerts, run mode, geo (projection, simplify, clip, fixture stations inside their view). **Spark tests:** 4 pass in the Spark image (`python tasks.py test-spark`); skipped on the host.
- **Web tests (Playwright 1.64.0 + axe, Chromium):** `npm ci` then `npx playwright test` (serves `web/` on port 5179
  with `tests/web/serve.py`, 2 workers). `tests/web/shell.spec.js`: 9 pass (no horizontal scroll 320–2560 px, axe 0 violations at 320 / 768 /
  1440 px in both themes, skip link, theme toggle, nav overlay focus trap, freshness pill).
  `tests/web/live-map.spec.js`: 14 pass (one marker per station, region remembered, legend counts, sortable list,
  problems filter + empty state, details tooltip and phone sheet, keyboard roving focus, stale banner, in-place
  refresh, KPI tiles, error + Retry, axe with tooltip / list / sheet open, list keeps focus across a refresh).
  `tests/web/touch-targets.spec.js`: 1 pass (390 px, touch emulation, `pointer: coarse` asserted; hit-tests a 44 × 44 px
  square around every sort button and the first 10 station buttons). 24 Playwright tests in total. Tests route
  `data/*.json` to `tests/web/fixtures/` (a real 641-station export) and fake time with `page.clock`.
- `inspect`, `verify-lake` and `sql` run Spark with whole-stage codegen **off** (a JVM crash otherwise; see DEVLOG).
- Lint: ruff, line length 120.

---

## Known gaps and facts to remember
- Git: commits are made at the end of each loop, not by tooling during it (latest at the start of this loop:
  `4492d42`, F1 + F2).
- An unused Docker volume `dockwatch_iceberg-catalog` (old SQLite catalog) still exists; backup copy at
  `data/iceberg_catalog_backup.db`. The Postgres catalog is verified, so the volume can be deleted (left to the user).
- The Live page has no alerts rail yet (F3, a later loop): on desktop the map spans the content width;
  `.live-layout` in `web/index.html` / `css/map.css` is where the rail's column goes. "Replay a day" (stale banner
  link) is F5.
- Docker Compose warns that volume `dockwatch_checkpoints` "already exists but was not created by Docker Compose";
  harmless.
- **Partition evolution** not done (moved to M4 with compaction).
- **Small files:** each streaming query writes ≥ 1 file per minute (silver ≈ 45 KB/file). Compaction arrives in M4.
- `live.json` is ~200 KB uncompressed; fine for now, worth trimming or gzip-serving later (F5).
- **AWS steps are manual** and not done; `checkpoint_root` on AWS will need `s3a://` + hadoop-aws jars (not in the image yet).
- Some stations report a `last_reported` weeks old → `offline`. 35 stations have no `region_id` → placed by coordinates.
- `station_information` rows arrive in a different order on every poll (the content hash sorts them).
- Bay Wheels currently reports 0 scooters at every station; the evolved scooter columns are 0 for new rows, null for old.
