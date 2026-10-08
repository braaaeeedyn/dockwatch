# DockWatch: current state

> **What this file is:** a description of DockWatch *exactly as it is right now* and what is being worked on.
> It is rewritten whenever the code changes. If something is removed from the code, it is removed from here too.
> For the full history, including what was tried and removed, see [`DEVLOG.md`](DEVLOG.md).
> For the plan, see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).

**Last updated:** 2026-10-08 · **Milestones:** M0 ✅ (except manual AWS steps) · M1 ✅ · M2 ✅ except
partition evolution (moved to M4) · F1 ✅ · F2 ✅ (plus F-1 / F-2 / touch-target fixes, the paused state and
44 px touch targets everywhere) · F5 "Replay a day" ✅ · DESIGN.md v1.0.1

---

## Working on now

| What | Why | Status |
|---|---|---|
| Nothing in progress | "Pipeline off" is now a first-class state with Replay a day, and every pressable is ≥ 44 px on touch. | Next: F3 (alerts rail). |

Done most recently: **the paused state and Replay a day** (suggestions H1 and H3 of the run-2 review).
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

Next loop: **F3** (alerts rail beside the map), then CI and M3 (ops DB + Debezium CDC).

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

raw archive (one Pacific day = two UTC dt= partitions) ──(python tasks.py replay-export, by hand, host only)──►
        web/data/replay.json   (Replay a day: one frame per minute, only changed stations after frame 0)
```

---

## What exists right now

### Documents (`docs/`)
| File | What it is |
|---|---|
| `DOCKWATCH_PLAN.md` | The original project plan (what and why). Unchanged. |
| `IMPLEMENTATION_PLAN.md` | Milestones M0–M7 / F1–F5, decisions (incl. M2 decisions), progress checkboxes. |
| `DESIGN.md` | DockWatch design system **v1.0.1**, frozen until launch (F5); v1.0.1 = accessibility fix (44 px touch targets in the list table). |
| `DESIGN_BACKLOG.md` | Gaps and judgment calls found while building against the frozen DESIGN.md (14 from F1, 18 from F2, 3 from the F2 review, 9 from the paused state / Replay a day loop); #35 (touch targets) is resolved. |
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
| `exporter/replay.py` | `python tasks.py replay-export [--date YYYY-MM-DD] [--step 60] [--out web/data/replay.json]`: reads one **Pacific** day of `raw/gbfs/station_status` snapshots from the archive (`FsArchive` / `S3Archive`; both UTC `dt=` partitions, filtered by Pacific date), keeps the last snapshot per `step` bucket, computes each station's state with `classify()` / `capacity_of()` and its view with `view_for()`, and writes `replay.json` atomically (format v1: `stations` with name, code, lat/lon, view, capacity; `frames` of `[index, state, bikes, docks]` groups, frame 0 full, later frames only changed stations; busiest stations get the smallest indices). Station names come from the day's last `station_information` snapshot. Default day: the latest complete Pacific day with snapshots (today if there is none). Prints size, stations and frames; exits non-zero if the day has no snapshots. | Uses the same rule as Spark without a JVM, and the archive the producer already writes. Snapshot by snapshot: only each station's previous value is kept. |
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
| `index.html` (Live), `insights.html`, `pipeline.html`, `about.html` | Shared shell: skip link, sticky 56 px nav (wordmark, links, freshness pill, theme toggle), full-screen nav overlay below 1120 px, footer with Bay Wheels attribution, "Not affiliated with Lyft or Bay Wheels", GitHub link, "Data through <date>". Live = title + freshness pill, paused banner (with "Replay a day" when `data/replay.json` exists) and the replay banner, KPI tiles, the map card at `#map` (F2); Insights / Pipeline show "Arrives with M4 / M5" empty states; About has sources and licences. On touch (`pointer: coarse`) every button and link is a ≥ 44 × 44 px hit area (footer links 44 px tall; icon buttons keep their corners via a square `::after`); in-sentence links in About's text are the WCAG inline exception. |
| `css/tokens.css`, `base.css`, `layout.css`, `components.css`, `map.css` | Tokens from the DESIGN.md front matter (light default; dark via `prefers-color-scheme` or the toggle's `data-theme`); `@font-face`, reset, focus ring, reduced motion; page frame; components (incl. stat tile, segmented control, chip, banner, skeleton, data table with 44 px touch targets, details, bottom sheet); the map card, markers, tooltip and legend. |
| `js/main.js`, `insights.js`, `pipeline.js`, `about.js` | One ES-module entry per page; each calls `shell.js` (`theme.js`, `nav.js`, `freshness.js`, `util/time.js`). `freshness.js` is the page's only `live.json` poll (60 s; a read that hasn't answered in 10 s is aborted and counts as failed, so the pill says "Paused · No data yet" instead of staying on "Loading…", and Live shows its error with Retry) and broadcasts each result as a `dockwatch:live` event (or `dockwatch:live-error`); every pill render (15 s tick and each refresh) also broadcasts `dockwatch:freshness` with the state, so the Live banner flips with the pill. On pages without the banner it adds a visually hidden `aria-live="polite"` region that announces only transitions: "Live data paused — showing the state at 3:42 PM." and "Live data is back." "Data as of" carries the date when the data is from an earlier Pacific day. |
| `js/live/live.js` + `states.js`, `kpis.js`, `list.js`, `details.js`, `replay.js` | Live page controller: region (remembered in `localStorage`, mirrored in the URL with `view` and `problems`), `Map | List`, "Show only problems" (every state except `ok`), legend counts from `live.json` `counts[view]`, polite `aria-live` summary only when counts change, paused banner (`freshnessState(generated_at) === "paused"`, > 5 min) with the reason sentence and "Replay a day", skeleton / error (Retry) / empty states; KPI tiles; the sortable list table (rows updated in place by station id, so focus and scroll survive a refresh); tooltip (≥ 600 px) or bottom sheet (phones). `alerts.json` is re-read with each refresh. **Replay mode** (`replay.js`): `HEAD data/replay.json` once when the banner first shows (no button if it's missing); on click it fetches the file, decodes frames incrementally into `live.json`-shaped stations (`state_since` / `last_reported` null, "—" in the list, "Replay, state at 7:35 PM" in details) and steps one frame every `step_s × 1000 / 10` ms with `setInterval`, looping at the end. Focus moves to Exit, and back to "Replay a day" on Exit; Escape does not exit. KPI tiles show the frame's Empty / Full / Bikes, Open alerts "—" ("Alerts aren’t part of the replay") and a caption naming the replayed day. Live refreshes during replay update the pill only. Not in the URL. |
| `js/map/map.js` | SVG map: land + landmarks, one `<circle>` per station keyed by id and updated in place (fill cross-fade + one pulse on a state change, none under reduced motion and none in replay mode), tier groups for draw order, screen-pixel sizes via `--u` / `--r-ok` from a `ResizeObserver`, nearest-station hit testing (10 px pointer, 22 px touch), roving tabindex with arrow keys. No wheel/pinch handling. |
| `fonts/` | IBM Plex Sans 400/500/600 + Mono 400/500, Latin `woff2`, and `OFL.txt`. |
| `licenses/lucide-LICENSE.txt` | Licence for the inline Lucide v1.52.0 icons (sun, moon, menu, x, info, triangle-alert, history, chevrons-up-down, arrow-up, arrow-down). |
| `data/replay.json` | Written by hand with `python tasks.py replay-export` (git-ignored, like `live.json`); read only when a visitor presses "Replay a day". The real build of 2026-10-07 (archive starts ~3 PM Pacific): 401,833 bytes, 641 stations, 492 one-minute frames; budget 1.5 MB per full day. |
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
  `setup`, `produce`, `produce-once`, `replay` (re-publish an archived day to Kafka), `stream`, `stream-logs`,
  `stream-stop`, `catchup`, `inspect`, `verify-lake`, `sql "<query>"`, `export`, `replay-export` (Replay a day file
  from the archive), `geo`, `tf-fmt`, `tf-validate`.
- **Host tests:** 53 pass, 1 skipped (`python tasks.py test`): GBFS client, messages/archive, poller, episodes/state rule, exporter (incl. `generated_at` kept without new data and across restarts),
  replay builder (`tests/test_replay.py`, 7 tests: full first frame then deltas, the shared `classify()` rule, a
  Pacific day across two UTC partitions, decoded frames equal the snapshots, a synthetic full day of 641 stations ×
  1440 frames at 80 changes per frame fits the 1.5 MB budget with the real writer (1,411,600 bytes), the CLI on an
  `FsArchive`, the default day), alerts, run mode, geo (projection, simplify, clip, fixture stations inside their view). **Spark tests:** 4 pass in the Spark image (`python tasks.py test-spark`); skipped on the host.
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
  view with every sort and station button, phone sheet, nav overlay, Insights, Pipeline, About, focused skip link).
  Each is hit-tested at its centre, ±21 px and the corners of a 44 × 44 px square; only About's in-sentence links
  and map markers (22 px nearest-station radius) are exempt, and the test checks the exemptions.
  `tests/web/replay.spec.js`: 7 pass (banner says why and offers Replay a day; names the day for older data; no button
  without a file; plays frames 0 and 1 of the fixture at 10×; Exit restores live data and focus; no announcement per
  frame; axe on both banners at 390 / 1440 px, light and dark). 35 Playwright tests in total. Tests route
  `data/*.json` to `tests/web/fixtures/` (a real 641-station export, and `replay.json` built from the real archive
  with `--step 600`, 51 frames) and fake time with `page.clock`; `data/replay.json` is a 404 unless a test asks for
  the fixture.
- `inspect`, `verify-lake` and `sql` run Spark with whole-stage codegen **off** (a JVM crash otherwise; see DEVLOG).
- Lint: ruff, line length 120.

---

## Known gaps and facts to remember
- Git: commits are made at the end of each loop, not by tooling during it (latest at the start of this loop:
  `4492d42`, F1 + F2).
- An unused Docker volume `dockwatch_iceberg-catalog` (old SQLite catalog) still exists; backup copy at
  `data/iceberg_catalog_backup.db`. The Postgres catalog is verified, so the volume can be deleted (left to the user).
- The Live page has no alerts rail yet (F3, next loop): on desktop the map spans the content width;
  `.live-layout` in `web/index.html` / `css/map.css` is where the rail's column goes.
- **Replay file refresh is manual:** `web/data/replay.json` changes only when someone runs
  `python tasks.py replay-export` (no schedule, not published to AWS yet). The archive starts on 2026-10-07 ~3 PM
  Pacific, so the default day (Oct 7) is partial; the replay starts at its first archived frame. A day at 10× takes
  2.4 h; there is no pause, scrubber or speed choice (Exit is the stop control).
- **Replay size budget:** 1.5 MB uncompressed per full day (DESIGN_BACKLOG #45). The test's synthetic day uses 80
  changes per frame (1.5× the real mean of ~53 per minute) and is ~6% under; 100 per frame would be ~1.74 MB, so the
  budget holds up to ~84. A full real day is ~1 MB. If real days approach the cap: a larger default `--step`, or a
  compact v2 encoding / gzip.
- **Shell-suite flake (not reproduced):** once, after the Spark-in-Docker checks, the nav pill stayed "loading" for
  5 s in `shell.spec.js`. Not reproduced since in ~180 runs after `test-spark` (see DEVLOG); the live.json read
  timeout and the keep-alive test server remove the two likely causes, and a recurrence now names its cause.
- The paused state can't tell "Spark stopped" from "the whole laptop is off": both mean "the pipeline isn't running"
  to a visitor. An exporter heartbeat would need a supervised exporter (out of scope).
- Docker Compose warns that volume `dockwatch_checkpoints` "already exists but was not created by Docker Compose";
  harmless.
- **Partition evolution** not done (moved to M4 with compaction).
- **Small files:** each streaming query writes ≥ 1 file per minute (silver ≈ 45 KB/file). Compaction arrives in M4.
- `live.json` is ~200 KB uncompressed; fine for now, worth trimming or gzip-serving later (F5).
- **AWS steps are manual** and not done; `checkpoint_root` on AWS will need `s3a://` + hadoop-aws jars (not in the image yet).
- Some stations report a `last_reported` weeks old → `offline`. 35 stations have no `region_id` → placed by coordinates.
- `station_information` rows arrive in a different order on every poll (the content hash sorts them).
- Bay Wheels currently reports 0 scooters at every station; the evolved scooter columns are 0 for new rows, null for old.
