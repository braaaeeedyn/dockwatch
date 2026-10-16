# DockWatch dev log

> **What this file is:** an append-only record of everything done to DockWatch: what was built, decided, tried,
> broken, fixed, **and removed or abandoned**, so later work knows what has already been attempted and why.
> Entries are never edited or deleted after the fact; corrections go in a new entry.
> For how the app works *right now*, see [`CURRENT_STATE.md`](CURRENT_STATE.md).

Entry format: `## YYYY-MM-DD · milestone · short title`, then any of **Did / Decided / Found / Removed / Tried (didn't work) / Next**.

---

## 2026-10-07 · planning · implementation plan
**Did**
- Wrote `docs/IMPLEMENTATION_PLAN.md` from `docs/DOCKWATCH_PLAN.md`: backend milestones M0–M7 (following the main
  plan's phases), frontend milestones F1–F5, repo layout, decisions table, working rules.

**Decided** (full reasons in the plan's §0)
- Two run targets, `local` and `aws`, so everything can be built at $0 before an AWS account exists.
- Compose profiles (`stream` / `batch` / `obs`) because Docker has ~8 GB on this machine.
- Spark jobs run in the `apache/spark` image (Java 17); stateful episodes with `applyInPandasWithState`.
- A static site reading JSON exports, fed by an exporter that consumes the stream; "Replay a day" for when the laptop is off.
- Map = SVG from Census TIGER shapes, split into three region views (SF, East Bay, San José), no tiles or API keys.

**Found** (live feed check)
- `gbfs.baywheels.com/gbfs/gbfs.json` 301-redirects to `gbfs.lyftbikes.com`, which serves GBFS **1.1**; GBFS **2.3** is at
  `gbfs.lyft.com/gbfs/2.3/bay/gbfs.json` (listed in `gbfs_versions`).
- `station_information` has `ttl: 60`, not daily as the main plan assumed.
- 641 stations; 80 empty / 8 full / 4 not renting at check time; `last_reported` up to ~16 days old for some stations.
- `system_regions` lists every region twice; 35 stations have no `region_id`.

## 2026-10-07 · planning · design system reworked
**Did**
- Replaced `docs/DESIGN.md` (it was a copy of TransitPulse's design system: BART line colours, train map, Ask card)
  with the DockWatch design system v1.0.

**Removed**
- From the design: BART line palette, train map/glyph spec, pill-shaped controls, Ask card / answer panel / SQL agent
  components, forecast components, the ink "Findings" band. None of these fit DockWatch.
- The old file was **moved**, not deleted: `docs/reference/DESIGN_SOURCE_transitpulse.md`.

**Decided**
- Neutral "operations console" look, light + dark themes, IBM Plex Sans/Mono.
- Colour only for station state, using the Okabe–Ito colour-blind-safe palette: warm = no bikes (empty/low),
  cool = no docks (high/full), grey = ok, dashed ring = offline. Problem stations are also drawn 1.35× larger.
- Status colours (ok/warn/fail) only on the pipeline page, always with an icon and a word.
- Contrast checked with a script: all text tokens ≥ 4.5:1 on their allowed surfaces; light `low`/`high` markers are
  under 3:1 on white, so every marker gets a 1 px casing.
- Same freeze rule as TransitPulse: frozen from F1 to launch; gaps go to `DESIGN_BACKLOG.md`.

## 2026-10-07 · M0 · scaffold
**Did**
- `git init` (branch `main`) inside `dockwatch/` (the parent `coding/` folder is a separate, empty repo). No commits yet.
- `pyproject.toml` (uv, Python 3.12, groups `dev` + `producer`), `.gitignore`, `.env.example`, `README.md`
  (with Bay Wheels licence attribution and "not affiliated" note), `tasks.py` + forwarding `Makefile`.
- `src/dockwatch/config.py` settings object.
- Terraform in `infra/terraform/` (S3 + lifecycle, Glue DBs, Athena workgroup with scan limit, SNS, $1/$5 budgets,
  IAM roles, GitHub OIDC). `terraform fmt` + `validate` pass via the `hashicorp/terraform:1.9` Docker image.

**Found**
- `terraform` and `aws` CLIs are not installed → Terraform runs through Docker from `tasks.py`.
- `make` is not installed → `tasks.py` (same pattern as TransitPulse).
- Python's default file encoding on this machine is cp1252: scripts that edit docs must open files with `encoding="utf-8"`.

## 2026-10-07 · M0 · local stack
**Tried (didn't work)**
- **MinIO** as local S3 (`minio/minio` and `quay.io/minio/minio`): both images can no longer be pulled
  ("pull access denied" / manifest missing). Also the `minio-init` helper container using `mc` for bucket creation.

**Removed**
- MinIO and `minio-init` services from `docker-compose.yml`; MinIO ports 9000/9001 from config and README.

**Did**
- Switched local S3 to **SeaweedFS** 3.80 (`server -s3`, port 8333, identities in `infra/seaweedfs/s3.json`).
  Bucket creation moved into the producer's `setup` command (`S3Archive.ensure_bucket()`).
- Iceberg REST catalog (`tabulario/iceberg-rest:1.6.0`) pointed at SeaweedFS; smoke-tested create/list/delete namespace.

**Found / fixed**
- SeaweedFS only listened on the container IP, so its healthcheck on `localhost` failed → added `-ip.bind=0.0.0.0`.
- Port 8080 is taken on this machine → Redpanda Console on **8088**. 5432/5433 are taken → ops Postgres on **5434**.
- The Iceberg REST image defaults to an **in-memory** SQLite catalog, which loses every table on restart → catalog DB now
  on a named volume (`CATALOG_URI=jdbc:sqlite:file:/catalog/...`, container runs as root to write it). Verified a
  namespace survives `docker compose restart iceberg-rest`.

## 2026-10-07 · M1 · GBFS producer
**Did**
- `gbfs/` (models, discovery client), `producer/` (messages, archive, kafka, poller, CLI), 20 unit tests on fixtures cut
  from real snapshots (5 stations chosen to include an empty, a full, a stale and a not-renting station).
- Topics created: `gbfs.station_status` (6 partitions), `gbfs.station_information` (compacted), `gbfs.dlq`,
  `gbfs.station_status.replay`.
- First live run: 641 messages per snapshot, 0 rejects, snapshots archived to `s3://dockwatch/raw/gbfs/...`.

**Decided**
- Replay publishes to a separate `.replay` topic so archived (old) events never mix into the live topic.
- `station_information` is polled at its 60 s ttl but only produced when its content changes.

**Found / fixed**
- Bug: `station_information` was re-produced every minute although nothing changed. Cause: Bay Wheels returns the
  stations in a **different order on every poll**, so the content hash changed. Fix: `content_hash()` sorts stations by
  `station_id` first; regression test `test_content_hash_ignores_station_order` added (21 tests now).
- In a 5-station sample, 2 stations in `station_status` had **no row** in `station_information` → M2 joins must be left joins.

**Next**
- Finish the one-hour live run for M1's *Done when*; write `docs/CONCEPTS.md`; then M2 and F1.

## 2026-10-07 · M1 · long live run
**Did**
- Producer ran against the live feed from 15:36 to 19:44 PT (about 4 h 8 min): 247 `station_status` snapshots,
  no errors or warnings in the log. After the hash fix, `station_information` was only produced when it changed.

**Found**
- The run did not crash. Claude Code stopped it because the whole machine was low on memory while the session was idle.
  Docker (Redpanda, SeaweedFS, Iceberg REST, Postgres) shares that memory, so this is a reason to keep using
  only one Compose profile at a time.

## 2026-10-07 · M1 · done
**Did**
- Restarted the producer after the memory stop. Checked M1's *Done when* against the archive and Kafka: full hours have
  59–60 archived snapshots, 641 messages per snapshot, 161,532 messages after ~4 h 15 min, 0 DLQ rows. ✅
- Wrote `docs/CONCEPTS.md` (Kafka section), using real numbers from this run.

## 2026-10-07 · M2 · streaming job into Iceberg
**Did**
- Spark image `infra/spark/Dockerfile`: `apache/spark:3.5.5` (Java 17, Python 3.10) with Iceberg 1.6.1 runtime + AWS
  bundle, Spark Kafka connector and its deps baked in; pandas, pyarrow, pydantic-settings.
- `streaming/episodes.py` (plain-Python state rule + episode state machine), `transforms.py`, `lake.py`,
  `status_stream.py` (four queries: bronze, silver, availability_5m, episodes), `inspect_tables.py`.
- Compose service `status-stream` (profile `spark`), topics `gbfs.alerts` and `dockwatch.station_state`, tasks
  `stream`, `stream-logs`, `stream-stop`, `inspect`.
- First start processed the ~4 h backlog: bronze 169,224 rows (= every Kafka record), silver 168,583, 18,589
  five-minute windows, 653 episodes, 440 alert messages. Latest snapshot: 53 empty / 23 full stations, matching
  53 / 23 open episodes exactly.

**Decided**
- Silver de-dup key is (`station_id`, `snapshot_ts`), not (`station_id`, `last_reported`) as planned. Stale stations
  repeat the same `last_reported` for days: a watermark on it would drop them as late, so they'd never show as offline,
  and without a watermark the de-dup state would grow forever. Event time = `snapshot_ts`.
- Alerts and station state are written to Kafka from `foreachBatch` (at-least-once) with keys that make repeats harmless.
- Partition evolution (days → hours) moved to M4: doing it before compaction exists would only make small files worse.

**Found / fixed**
- Episode state started with `last_ts = 0`, so a first row at time 0 counted as a duplicate (unit test caught it).
  Fixed with `-1`. Real timestamps were never affected.
- Iceberg tables need the Spark user to write checkpoints: the `dockwatch_checkpoints` volume was created by hand and
  `chown`ed to uid 185. (Compose warns that the volume wasn't created by Compose; harmless.)
- My own mistake: a command contained an empty `python - <<EOF` heredoc, which started an interactive Python REPL that
  looped on errors and wrote ~64 MB of output before I stopped it. No project files were affected.

## 2026-10-07 · M2 · JVM crash in the inspection job
**Found**
- `python tasks.py inspect` (a second Spark JVM next to the streaming job) segfaulted in
  `SymbolTable::do_lookup` on the first `COUNT(DISTINCT …)` query.
- First suspected memory (the host had 1.1 GB free). Killed the streaming job and retried: **same crash**, so not memory.

**Tried (didn't work)**
- `-Xshare:off` (class-data sharing off): still crashed.

**Fixed**
- `spark.sql.codegen.wholeStage=false` for the `inspect` and `sql` tasks: no crash. The streaming job keeps codegen on
  and hasn't crashed. If it ever does, apply the same setting there.
- Streaming driver memory lowered from 1500 MB to 1 GB (container limit 2.5 GB → 2 GB) to leave the host more room.

## 2026-10-07 · M2 · kill-and-restart test
**Did**
- `docker kill` on the streaming job (SIGKILL, no clean shutdown), then restarted it from its checkpoints.
- Result after catching up: bronze 172,429 rows = 172,429 distinct (partition, offset); offsets contiguous from 0 in all
  6 partitions and equal to Kafka's high watermarks (0 gaps); silver 0 duplicate (station_id, snapshot_ts). ✅
- Kafka outputs after the kill: some alerts were sent twice (at-least-once, as designed; keyed by `alert_id`).
- Added the gap check to `inspect_tables.py`.

## 2026-10-07 · M2 · schema evolution
**Did**
- The feed always sent `num_scooters_available` / `num_scooters_unavailable`, but `STATUS_SCHEMA` dropped them. Added them
  to the parser and to `lake.EVOLUTIONS`; `migrate()` runs `ALTER TABLE … ADD COLUMN` at job start if missing.
- Restarted the job on its existing checkpoints: no errors; all four queries resumed. Silver: rows before 03:12 UTC
  have null scooter counts, rows after have values (0: Bay Wheels has no scooters right now). No files rewritten.
- New columns are selected **last** in the code because Iceberg appends new columns and Spark writes by position.
- Added the `sql` task for one-off queries.

## 2026-10-07 · M2 · exporter, alert handler, Spark tests
**Did**
- `exporter/`: reads `dockwatch.station_state`, `gbfs.station_information` and `gbfs.alerts` from the start (no
  committed offsets), writes `web/data/live.json` + `alerts.json` every 60 s with atomic renames. First export:
  641 stations, 0 with missing names, data ~90 s old, 51 open alerts. `live.json` ≈ 200 KB.
- `alerts/handler.py`: `format_alert()` + `lambda_handler()` (SNS or print). 3 tests.
- Spark tests (`tests/spark/test_transforms.py`, 4 tests) run inside the Spark image via `python tasks.py test-spark`;
  they include a check that the Spark state rule and the Python `classify()` agree on every case. Skipped on the host.

**Found / fixed**
- Spark tests were silently skipped at first: PySpark lives in `/opt/spark/python`, not in pip; `PYTHONPATH` now includes it.
- Windows has no system time-zone database → added the `tzdata` dependency. `strftime("%-I")` doesn't work on
  Windows → use `%I` and strip the leading zero.
- In the full live join every status station had a `station_information` row (the 2-of-5 gap seen in the M0 sample
  doesn't show up once the compacted topic has accumulated several snapshots).

**Next**
- F1 (tokens + shell) and F2 (live map from `live.json`) to close M2's *Done when*; then M3 (ops DB + Debezium).

## 2026-10-07 · M2 · catalog moved from SQLite to Postgres; Docker engine stopped responding
**Found**
- The streaming container had restarted itself **22 times**. Cause: the Iceberg REST catalog's SQLite file returned
  `SQLITE_BUSY: database is locked` (35 times) when the four streaming queries committed at the same moment; the REST
  call then failed with HTTP 500 → `CommitStateUnknownException` → the job died and Docker restarted it.
- This bug was introduced by my earlier fix (in-memory SQLite → SQLite file). The in-memory catalog had the same
  single-writer limit but failed less visibly; neither is fit for concurrent writers.
- Table metadata had only reached version ~16, so many commits since about 03:09 UTC never landed.

**Removed**
- SQLite catalog: the `CATALOG_URI=jdbc:sqlite:…` setting, the `iceberg-catalog` volume from the compose file, and
  `user: root` on `iceberg-rest`. (The old Docker volume itself still exists, unused; a copy of its database is at
  `data/iceberg_catalog_backup.db`.)

**Did**
- New database `iceberg_catalog` on the existing `ops-db` Postgres (`infra/postgres/init/01-iceberg-catalog.sql` for
  fresh volumes; created by hand with `createdb` on this machine). `iceberg-rest` now uses
  `jdbc:postgresql://ops-db:5432/iceberg_catalog` (the image already contains the Postgres driver).
- Re-registered both namespaces and all four tables in the new catalog with the REST `register` call, pointing at the
  latest metadata file from the SQLite backup. No data files were copied or rewritten. All four returned 200.
- Restarted the streaming job.

**Blocked**
- Right after the restart the **Docker Desktop engine stopped answering** (HTTP 500/502 on `docker ps`). The host had
  ~1.5 GB free and Windows was compressing ~3 GB of memory. So the fix is **not verified yet**: still to check that
  commits succeed, that the job stops restarting, and that the retried batches left no duplicates or gaps
  (`python tasks.py inspect`).
- Restarting Docker Desktop also affects other projects' containers, so it is left to the user.

## 2026-10-08 · M2 · Postgres catalog verified; Spark runs on demand
**Found**
- The Docker engine was responding again; the stack was up and the job was running on the Postgres catalog. No `SQLITE_BUSY` or
  `CommitStateUnknownException` since the move (0 matches in the job log, 0 in the `iceberg-rest` log).
- But `status-stream` had still **restarted twice since 03:26 UTC** (RestartCount=2, OOMKilled=false), for a new
  reason: `HeartbeatReceiver: Removing executor driver with no recent heartbeats: 135969 ms exceeds timeout 120000 ms`
  (03:41) and `Executor: Exit as unable to send heartbeats to driver more than 60 times` (03:49). The driver JVM
  stalled for over 2 minutes, most likely host memory pressure (~2.6 GB free on the host, container at 1.62 / 2 GiB).
  `restart: unless-stopped` then brought it straight back into the same pressure.
- `python tasks.py inspect` only printed numbers; nothing failed when they were wrong.

**Removed**
- `restart: unless-stopped` on `status-stream` (now `restart: "no"`). The job no longer runs 24/7.

**Did**
- `DOCKWATCH_TRIGGER_MODE` (`config.trigger_mode`): `processing_time` (default, one micro-batch every 60 s) or
  `available_now`. New pyspark-free `streaming/run_mode.py` (`trigger_options()`) with two host tests
  (`tests/test_run_mode.py`).
- In `available_now` mode `status_stream.py` uses Spark's `availableNow` trigger: each of the four queries drains
  Kafka up to the offsets present at start (still in batches of at most `maxOffsetsPerTrigger` = 100,000), then
  stops; the job waits for every query (a failed query re-raises, so the process exits non-zero) and stops the
  session. Same image, command and checkpoints as `stream`, so the two modes can be mixed freely.
- New tasks: `catchup` (refuses while `status-stream` is running, then `docker compose run --rm --no-deps -e
  DOCKWATCH_TRIGGER_MODE=available_now status-stream`, blocking, returns the job's exit code) and `verify-lake`
  (`inspect_tables.py --check`: same report, then assertions, exit 1 on any failure). Checks: silver duplicates on
  (`station_id`, `snapshot_ts`) = 0; bronze rows = distinct (partition, offset); all 6 partitions present with 0
  offset gaps; silver's latest snapshot within 5 min of bronze's; every table has an Iceberg snapshot committed in
  the last 60 min (`--max-age-min`), which proves catalog commits succeed.
- Ran `stream-stop` → `catchup` → `verify-lake` (04:12–04:14 UTC):
  - `catchup`: ~28 s wall time including JVM start (backlog ~5 min); all four queries finished, exit 0, no
    `SQLITE`/`CommitStateUnknown` in the output.
  - `verify-lake`: **ok**. bronze 208,966 rows = 208,966 distinct (partition, offset) = the sum of the six Kafka
    high watermarks (33,904 + 30,644 + 34,230 + 32,926 + 36,186 + 41,076), offsets start at 0 in every partition,
    0 gaps; silver 208,966 rows, 0 duplicates; latest snapshot 04:11:04 UTC in both; availability_5m 41,665 rows;
    767 episodes; last commit on each table < 1 min old (42 / 44 / 45 / 41 snapshots).
  - So the retried batches from the SQLite period left **no duplicates and no gaps**; no rebuild from Kafka was needed.
- `test-spark` still 4 pass; host tests 37 pass.

**Next**
- F1 (tokens + shell) and F2 (live map). The old `dockwatch_iceberg-catalog` volume can now be deleted by the user
  (backup stays at `data/iceberg_catalog_backup.db`).

## 2026-10-08 · F1 · design tokens, site shell, Playwright
**Did**
- `web/css/tokens.css` written by hand from the DESIGN.md v1.0 front matter: every colour (interface, state,
  state-text, both heatmap ramps, status, gridline), type scale, radii, spacing, layout, elevation and motion.
  Light is the default; dark applies via `@media (prefers-color-scheme: dark)` unless the visitor picked light
  (`[data-theme="light"]`), or whenever they picked dark (`[data-theme="dark"]`). Plus `base.css` (fonts, reset,
  focus ring, reduced motion), `layout.css` (container, sticky nav, nav overlay, footer) and `components.css`.
- IBM Plex Sans 400/500/600 and Plex Mono 400/500, Latin `woff2`, self-hosted in `web/fonts/` (from
  `@fontsource` 5.3.0, files only) with the SIL OFL 1.1 text in `web/fonts/OFL.txt`; `font-display: swap`, Sans 600
  preloaded. No external requests at all.
- Four pages (`index.html` Live, `insights.html`, `pipeline.html`, `about.html`), each with one ES-module entry in
  `web/js/` and a shared shell (`shell.js` → `theme.js`, `nav.js`, `freshness.js`, `util/time.js`):
  - sticky 56 px nav: wordmark, Live · Insights · Pipeline · About, freshness pill, theme toggle (Lucide v1.52.0
    icons, inline SVG); below 1120 px the links move into a full-screen `<dialog>` (focus trapped, `Esc` closes,
    page scroll locked, focus returns to the menu button);
  - skip link first ("Skip to map" on Live, "Skip to content" elsewhere);
  - freshness pill from `live.json` `generated_at`: Live < 2 min, Delayed 2–5 min, Paused > 5 min (word + dot,
    Pacific time), refetched every 60 s and re-evaluated every 15 s;
  - theme follows the system until the toggle is used; the pick is kept in `localStorage` (wrapped in try/catch)
    and applied by a tiny inline script before first paint;
  - footer: Bay Wheels licence attribution, "Not affiliated with Lyft or Bay Wheels", GitHub link,
    "Data through <date>" (`data_as_of`), "Times are Pacific time.";
  - Insights / Pipeline show a `card-soft` empty state ("Arrives with M4" / "Arrives with M5"); About covers what
    DockWatch is, how it works, and data sources + licences (Bay Wheels, Census TIGER, IBM Plex OFL, Lucide ISC);
  - the Live page has a placeholder card at `#map`; the map itself is F2.
- Playwright: root `package.json` (private, ES module) with `@playwright/test` pinned to **1.64.0** (matches the
  cached Chromium 1248, no browser download) and `@axe-core/playwright` 4.13.0; `package-lock.json`;
  `playwright.config.js` serves `web/` with `python -m http.server 5179`. `node_modules/`, `test-results/`,
  `playwright-report/` are git-ignored.
- `tests/web/fixtures/live.json` / `alerts.json`: a copy of a real export (641 stations, 61 open alerts). Every UI
  test routes `data/*.json` to them and fakes time with `page.clock`, so tests never depend on the live exporter.
- `tests/web/shell.spec.js` (9 tests, all pass): no horizontal scroll on all 4 pages at 320 / 390 / 768 / 1024 /
  1440 / 2560 px; axe 0 violations (WCAG 2.0/2.1/2.2 A + AA tags) on all 4 pages at 320 / 768 / 1440 px in light
  and dark, and in the open nav menu; skip link first; theme toggle remembered across reloads and pages; nav menu
  traps focus and closes with Escape at 390 / 768 / 1024 px; pill turns Live → Delayed → Paused as time passes.
- New `docs/DESIGN_BACKLOG.md` with 14 gaps and judgment calls (e.g. the freshness dot uses status colours on every
  page although §2 keeps status colours off Live/Insights; which timestamp "Data as of" shows; where the theme
  toggle goes on phones). DESIGN.md is unchanged.

**Tried and changed**
- The plan's `python -m http.server 5179` as Playwright's web server refused connections under parallel load
  (`ERR_CONNECTION_REFUSED`: its listen backlog is 5 and Windows refuses instead of queueing; 4 of 9 tests failed with
  4 workers, and 1 worker still failed once under host load). Replaced by `tests/web/serve.py`, the same stdlib
  server with a backlog of 128 and quiet logging, on the same port 5179: 9/9 with 4 workers, repeatedly. The config
  uses 2 workers (memory-tight host); the shell suite takes ~18 s.

**Next**
- F2: Census TIGER map shapes (`python tasks.py geo` → `web/geo/*.json`), the live station map, KPI tiles, list view,
  station details, polling and the stale banner; then the M2 spot-check against the public feed.

## 2026-10-08 · F2 · map-shape build step (started early)
**Did**
- `src/dockwatch/geo/build.py` (pure functions: `projection_for`, `project`, Douglas–Peucker `simplify`, rectangle
  `clip` (Sutherland–Hodgman), `build_view`) and `geo/__main__.py`; task `python tasks.py geo` (uv group `geo` with
  `pyshp`, imported lazily so host pytest doesn't need it).
- Source: US Census cartographic boundary file `cb_2023_us_county_500k` (clipped to shoreline, public domain),
  downloaded once to `data/geo/` (git-ignored, 11.6 MB zip). Six Bay Area counties (06-075, 081, 001, 013, 085, 041).
- Each view has fixed lon/lat bounds in code and its own equirectangular projection (scaled by cos(lat) at the
  view centre) into `0 0 1000 1000`. Output `web/geo/{sf,eastbay,sj}.json`: view, source, projection params,
  viewBox, land paths (clipped to the box + 10, simplified at 0.8 units), 3–4 landmarks. Sizes: sf 1,455 B
  (2 shapes), eastbay 2,120 B (3 shapes), sj 599 B
  (1 shape) — the 1:500k file is already coarse, which suits the "plain" map in DESIGN.md §6.
- **Deviation from the plan's wording:** station positions are **not** written to `web/geo`; the browser projects
  them with the same parameters (`web/js/map/project.js` mirrors `geo.build.project`), so new stations need no
  rebuild. Decision row added to IMPLEMENTATION_PLAN §0.
- `tests/test_geo.py` (5 pass): projection fits the view box, simplify keeps endpoints and reduces points, clip to a
  rectangle, document shape, and every station in `tests/web/fixtures/live.json` projects inside 0..1000 of its view.
  Host tests now 42 pass / 1 skip.
- Note for the map renderer: county shapes share internal borders (e.g. SF / San Mateo); draw the shoreline stroke
  first and the land fills on top so only the coast shows.

**Next**
- F2 map rendering, KPI tiles, legend, regions, list view, details, keyboard, polling/stale banner,
  `tests/web/live-map.spec.js`, then the spot-check.

## 2026-10-08 · F2 · live station map, list view, KPI tiles
**Did**
- Live page (`web/index.html`, `js/live/*.js`, `js/map/map.js`, `css/map.css`, F2 parts of `css/components.css`):
  page title + freshness pill; KPI tiles Empty now · Full now · Open alerts (`alerts.json` `open`) · Bikes available,
  each with an ⓘ definition; stale banner; map card with region segmented control (San Francisco · East Bay ·
  San José, remembered in `localStorage` and mirrored in the URL), `Map | List` and a "Show only problems" chip;
  legend with live counts per state + caption; a polite `aria-live` summary ("San Francisco: 54 empty, 12 full,
  28 offline") that only changes when the counts do.
- Map: SVG land from `web/geo/<view>.json` (2 px shoreline stroke under the land fill, so county borders vanish),
  landmarks with a canvas halo, one `<circle>` per station coloured by `state` from `live.json` (never recomputed),
  1 px casing, 1.35× for every non-ok state, draw order ok → offline → low/high → empty/full (four tier groups),
  offline = dashed `mute` ring. Sizes are screen pixels: a `ResizeObserver` sets `--u` (view-box units per px) and
  `--r-ok` (`clamp(3px, 0.45 % of map width, 5px)`), CSS turns them into radii, strokes and label sizes.
- Map aspect ratio per breakpoint (4:5, 1:1, 4:3, 16:10) with the square view box drawn `meet`; the geo build now
  keeps land 320 units beyond the box (`LAND_MARGIN`) so the extra width/height shows real coastline instead of a
  cut edge. Files are still tiny (sf 2.8 KB, eastbay 3.8 KB, sj 0.6 KB).
- Interaction: hover/click pick the nearest station within 10 px (22 px for touch), so tiny markers are easy to hit;
  no wheel/pinch handlers (the map never captures page scroll). Roving tabindex: one marker in the tab order, arrow
  keys move to the nearest station in that direction, Enter opens details, Escape closes and keeps focus. Hover/focus
  enlarge the marker 1.6×, add a 2 px ink ring and the name label.
- Station details: tooltip pinned beside the marker (≥ 600 px), bottom sheet (`<dialog>`, focus returns to the
  marker) on phones: name, code, state word + swatch, "15 bikes (2 e-bikes) · 0 docks", "Full for 26 min" when
  `state_since` is set, "Last reported 7 min ago".
- List view: `data-table` (Station · Code · State · Bikes · Docks · In state for · Last reported), header buttons
  with `aria-sort`, sticky header and first column, scrolls inside its card; same region/problems filter; a station
  name opens it on the map with details.
- Refresh: the map listens to the shell's single 60 s `live.json` poll (`freshness.js` now also reports failures);
  markers are keyed by station id and updated in place; a changed marker cross-fades its fill (200 ms) and pulses
  once (600 ms), both off under reduced motion. `alerts.json` is re-read with each refresh. Stale banner when
  `generated_at` is over 5 min old ("Live data paused — showing the state at 9:24 PM."; the Replay link is F5).
  Skeletons while loading, error card with Retry, empty card when a region has no problem stations.
- `tests/web/live-map.spec.js`: the 10 required tests plus 3 (empty-problems card, error + Retry, axe 0 violations
  with tooltip / list / phone sheet open in both themes). 13/13 pass, 39/39 with `--repeat-each=3`; the shell suite
  (axe + no horizontal scroll on the new Live page) still 9/9. Every expected number comes from the fixture.
- 18 new entries in `docs/DESIGN_BACKLOG.md` (#15–#32), e.g. what "Show only problems" includes, controls in a
  toolbar row instead of over the map, no sliding segmented thumb, static skeletons.

**Tried and changed**
- Chrome made the `<svg>` itself a tab stop before the roving marker; `tabindex="-1"` on the svg fixed it.
- The map card is a grid; a wide list table widened it to its min-content width (175 px horizontal page scroll at
  320 px). `grid-template-columns: minmax(0, 1fr)` keeps the table scrolling inside its wrapper.
- The name label showed next to an open tooltip that already starts with the name; the label now follows hover and
  focus only.
- A radius check read `r.baseVal` (0, because `r` comes from CSS); the test reads the computed `r` instead.

**Next**
- M3 (ops DB + Debezium CDC) in the next loop; F3 alerts rail later (`.live-layout` is the hook for its column).

## 2026-10-08 · M2 · spot-check: 10 stations vs the public feed
**Did** (the manual *Done when* check for M2/F2, once)
- Stopped state: `status-stream` not running. `python tasks.py catchup` at 05:31:32 UTC processed the backlog in
  33 s (exit 0, no `SQLITE` or `CommitStateUnknown` in the output). The running exporter then wrote `live.json` at
  05:33:01 UTC with `data_as_of` 05:30:02 UTC (10:30 PM Pacific).
- For each station, the raw archive object for the same feed snapshot (`raw/gbfs/station_status/…/1791437402.json.gz`,
  i.e. exactly what the producer fetched from the public feed) was compared with `live.json`. A fresh fetch of the
  public `station_status` feed (last_updated 05:32:02 UTC, 2 min newer) was compared too.
- Stations: first by name per view and state, across all three views and six states.

| View | Code | Station | State in live.json | live.json bikes / e-bikes / docks | Public feed, same snapshot | Public feed 2 min later |
|---|---|---|---|---|---|---|
| sf | SF-H29 | 2nd St at Folsom St | empty | 0 / 0 / 33 | 0 / 0 / 33 ✅ | 0 / 0 / 33 |
| sf | SF-P21 | 20th St at Dolores St | full | 27 / 10 / 0 | 27 / 10 / 0 ✅ | 27 / 10 / 0 |
| sf | SF-N22-1A | 16th St Mission BART South | low | 2 / 2 / 8 | 2 / 2 / 8 ✅ | 2 / 2 / 8 |
| sf | SF-Y30 | Jennings St at Revere Ave | offline | 0 / 0 / 16 | 0 / 0 / 16 ✅ | 0 / 0 / 16 |
| sf | SF-M11 | 10th Ave at Irving St | ok | 5 / 5 / 18 | 5 / 5 / 18 ✅ | 5 / 5 / 18 |
| eastbay | OK-L6-2 | 13th St at Webster St | empty | 0 / 0 / 19 | 0 / 0 / 19 ✅ | 0 / 0 / 19 |
| eastbay | BK-D2 | 10th St at University Ave | high | 18 / 8 / 1 | 18 / 8 / 1 ✅ | 18 / 8 / 1 |
| eastbay | OK-L11 | 10th Ave at E 15th St | ok | 12 / 4 / 3 | 12 / 4 / 3 ✅ | 12 / 4 / 3 |
| sj | SJ-N6 | Auzerais Ave at Los Gatos Creek Trail | full | 23 / 10 / 0 | 23 / 10 / 0 ✅ | 23 / 10 / 0 |
| sj | SJ-I14 | 23rd St at Taylor St | low | 1 / 1 / 18 | 1 / 1 / 18 ✅ | 1 / 1 / 18 |

- Result: **10/10 match** the public feed for the same snapshot, and the states agree with the rule
  (SF-Y30 is offline because the feed says `is_renting: 0` and its last report was 1 h 46 min before the snapshot).
  None of the 10 had changed in the 2 minutes since, so the newer fetch shows no drift for them; with `stream`
  stopped, `live.json` only moves when `catchup` runs, so drift grows with the time since the last catch-up.
- The comparison script lived in the session scratch folder (not committed): it reads `web/data/live.json`,
  `archive.get(archive_key("station_status", ts))` and `GbfsClient.fetch("station_status")`.

## 2026-10-08 · Note · a stray `C:\c` folder was deleted without checking it first
- While taking screenshots during the F1 work (2026-10-08), Node resolved a Git-Bash path `/c/Users/...` as
  `C:\c\Users\...`, so Playwright created `C:\c\...\scratchpad\shots`. The screenshots were moved out and the folder
  was removed with `rm -rf /c/c` (= `C:\c`) **without first checking whether `C:\c` existed before or held anything
  else**. When listed just before deletion it only contained the freshly created path, so it was probably new, but
  that was not verified. The user was told in that iteration's build notes.
- Since then: scripts pass Windows-style paths to Node, nothing outside the project or the session scratch folder
  is deleted, and a stray folder created by a tool is left in place and reported instead.

## 2026-10-08 · F2 fixes · honest freshness, list focus across refresh, 44 px touch targets
**Did**
- **F-1 freshness (exporter):** `generated_at` in `live.json` / `alerts.json` now means "time of the last export
  that had new data". `build.py` got two pure helpers: `carry_generated_at(live, previous)` keeps the previous
  `generated_at` when the previous export has the same non-null `data_as_of`, and `has_new_data(live, previous)` says
  whether to write (no previous export, or `data_as_of` moved forward; an older `data_as_of` while catching up after a
  restart is not written). `__main__.py` reads the previous `live.json` at start (`read_previous`: missing or invalid
  JSON → None), so the rule survives exporter restarts, and skips rewriting both files when there is no new data
  (INFO log "no new data since …, not rewriting"). `alerts.json` gets the live doc's `generated_at`. With Spark
  stopped, the site now goes Delayed → Paused and shows the stale banner; the site's 2 / 5 min thresholds and
  DESIGN wording are unchanged. Four new host tests in `tests/test_exporter.py` (46 pass, 1 skipped).
- **F-2 list focus:** `web/js/live/list.js` no longer replaces `tbody.innerHTML` on every refresh. Rows are kept in
  a `Map` keyed by station id; changed cells are updated in place (`textContent`, the state cell as escaped HTML),
  new rows are created, gone rows removed, and only rows out of place are moved. Focus on a station-name button and
  the wrapper's scroll position are remembered and restored (`focus({ preventScroll: true })`); if the focused
  station left the list, focus goes to the next row, else the previous. Missing bikes/docks now show "—".
  New Playwright test "list keeps focus on the same station across a refresh" (fails against the old `list.js`).
- **Touch targets:** under `@media (pointer: coarse)` the list's sort buttons are 44 px tall, and station-name
  buttons are 44 px tall `inline-flex` boxes whose `::before` covers the whole sticky name cell (the underlined text
  looks the same; 4 px cell padding keeps the focus ring visible). Pointer-fine sizes unchanged. DESIGN.md is
  **v1.0.1**: version bump, one sentence in the §7 Data table bullet, and a changelog entry (accessibility fix, as
  its change policy allows). New `tests/web/touch-targets.spec.js` (390 px, `hasTouch` + `isMobile`): asserts
  `(pointer: coarse)` really matches in Playwright's Chromium (it does), then hit-tests the centre, ±21 px and the
  corners of a 44 × 44 px square around every sort button and the first 10 station buttons. It fails on the old CSS.
- Docs: DESIGN_BACKLOG #2 rewritten (generated_at vs data_as_of), new #33 (pulse scales the marker, §5 says ring),
  #34 (container queries only on the stat tile), #35 (other pressables under 44 px found by a scan: KPI ⓘ buttons
  41 px wide, footer links 18–38 px tall). IMPLEMENTATION_PLAN F2 geo step points to §0 (projected in browser).

**Tried and changed**
- The first touch-test run failed on the Code sort button: centring a column in the wrapper put it under the sticky
  Station column. The test now centres each column in the part of the wrapper the sticky column doesn't cover.
- With 0 px cell padding the 44 px name button's focus ring (2 px + 2 px offset) was partly hidden by the
  neighbouring rows; 4 px padding fixed it (rows are 52 px on touch).

**Results**
- ruff, ruff format, host pytest 46 passed / 1 skipped; Playwright 24/24 (shell 9, live map 14, touch targets 1);
  the two new browser tests 10/10 with `--repeat-each=5`.

## 2026-10-08 · F2 follow-up + F5 · pipeline-off banner, Replay a day, 44 px everywhere

**Did**
- **Paused is a first-class state.** The Live banner keeps DESIGN §6's sentence and adds why and what next: "Live data
  paused — showing the state at 9:24 PM. The DockWatch pipeline isn't running right now, so no new station data is
  coming in; the map updates on its own when it restarts." With data from an earlier Pacific day it says "… at
  9:24 PM on Oct 7", and the pill says "Data as of Oct 7, 9:24:19 PM". `live.js` no longer has its own
  `> DELAYED_MAX_MS` test: the banner uses `freshnessState()` and flips on the pill's tick (`freshness.js` broadcasts
  `dockwatch:freshness` on every render). Insights, Pipeline and About get a visually hidden polite region that
  announces only transitions ("Live data paused — …" / "Live data is back."), never on first load and never on Live
  (the banner's `role="status"` speaks there).
- **Replay a day.** New `src/dockwatch/exporter/replay.py` + `python tasks.py replay-export`: reads one Pacific day
  of `raw/gbfs/station_status` snapshots from SeaweedFS on the host (both UTC `dt=` partitions, filtered by Pacific
  date, the last snapshot per 60 s bucket), computes states with the shared `classify()` / `capacity_of()` and views
  with `view_for()`, and writes `web/data/replay.json` atomically (format v1: station list + frames of
  `[index, state, bikes, docks]`, frame 0 full, then only changed stations). Busiest stations get the smallest
  indices, which keeps the numbers short. No Spark JVM. On the site, the paused banner probes the file once with
  `HEAD` and only then shows "Replay a day"; replay mode (`web/js/live/replay.js`) decodes frames incrementally into
  `live.json`-shaped stations and plays them at 10× through the normal map / legend / list / details / KPI code, with
  a neutral banner "Replay mode — showing Wednesday 7 Oct, 10× speed [Exit]" and a clock outside any live region.
  No pulse or cross-fade in replay, no map-summary announcements per frame, the pill keeps telling the truth about
  live data, Exit restores live data and focus.
- **Touch targets (DESIGN_BACKLOG #35 resolved):** KPI ⓘ buttons `flex-shrink: 0` (were squeezed to 41 px); on
  `pointer: coarse` every footer link, including "Bay Wheels License Agreement" inside its sentence, is a 44 px tall
  `inline-flex` box; icon buttons get a square invisible `::after`, because Chromium hit-tests their rounded corners
  away (the scan found that the 44 × 44 ⓘ, menu and theme buttons missed their corner points). `touch-targets.spec.js`
  gained two tests that hit-test every button, link, summary and segmented label on all four pages at 390 and
  1280 px with honest touch emulation; exempt (and asserted): About's in-sentence links and map markers.
- Tests: `tests/test_replay.py` (6), `tests/web/replay.spec.js` (7), a shell test for the announcements, the stale
  banner test's expected text now includes the reason sentence (same assertions). `prepare()` routes
  `data/replay.json` to 404 by default, or to the new fixture `tests/web/fixtures/replay.json` (built with
  `--step 600` from the real archive: 51 frames, 641 stations, 212,618 bytes).
- Docs: CURRENT_STATE, DESIGN_BACKLOG (#22 updated, #35 resolved, #36–#44 new), IMPLEMENTATION_PLAN (F5 "Replay a
  day" ticked), README (`replay-export`). DESIGN.md unchanged (v1.0.1).

**Decided**
- No exporter heartbeat / `pipeline: stopped` field: with the laptop off nothing writes any file, so it could only
  separate "Spark off" from "everything off", which are the same to a visitor. The banner infers it from
  `generated_at`.
- Replay source = the raw archive read on the host, not Iceberg (a Spark JVM is the memory we protect; 5-minute
  windows are coarser). Pacific day, default = latest complete Pacific day.
- "Replay a day" is a button (it changes the page's mode); no pause or scrubber (Exit is the stop control).

**Tried and changed**
- The plan's size test (641 stations × 1440 frames × 100 changed stations per frame ≤ 1.5 MB) does not fit format v1
  with realistic numbers: 1,742,868 bytes (uniform picks; ~10.5 characters per changed station, the index alone is
  ~2.8 digits). Even with the real change skew it would be ~1.65 MB. Measured: 60 changes/frame → 1.12 MB, 80 →
  1.43 MB, 85 → 1.51 MB. Real days change ~53 stations per minute (max 170 in one frame on Oct 7), so a full real
  day is ~1 MB. Ordering stations busiest-first was added (it cut the real file's average index to ~2.5 digits) but
  can't help a uniform worst case. `test_full_day_fits_the_size_budget` was therefore **not** added rather than
  shipped failing or with smaller-than-real numbers; the plan's numbers need a decision.
- Ruling, same day (size budget): 100 changes/frame measured 1,742,868 B, so the 1.5 MB budget was kept and the
  synthetic rate set to 80 changes/frame, ~1.5× the real mean of 53/min: the total size follows the day's mean change
  rate, not its one-frame peak, and raising the budget would loosen the phone page-weight limit to fit a badly chosen
  test input. The test was added in the next entry.
- The first touch scan failed on every 44 × 44 icon button's corners (rounded corners are not hit-testable); fixed
  with the square `::after` rather than by making the buttons bigger.

**Results**
- Real `python tasks.py replay-export`: 2026-10-07 (Pacific), 401,833 bytes, 641 stations, 492 frames, step 60 s,
  ~5 s; `check_web.py replay --real` ok.
- ruff + ruff format clean; host pytest 52 passed / 1 skipped; Playwright 34/34 (shell 10, live map 14, touch
  targets 3, replay 7).

## 2026-10-08 · F2 follow-up · shell-suite flake, size-budget test

**Did**
- **Shell-suite regression.** In the Checker run after the previous entry, `shell.spec.js` › "nav menu opens below
  1120 px, traps focus and closes with Escape" timed out in `open()`: the nav pill stayed `data-state="loading"` for
  5 s. The suite ran right after the Spark-in-Docker checks (`verify-lake`, `test-spark`) on a memory-tight host.
- **Root-cause analysis (read-only, before any change).** The previous entry's freshness rewiring is ruled out:
  `render()` sets the pill's `data-state` before any of the new code (announcer, `dockwatch:freshness`), a failed
  read gives `freshnessState(null)` = "paused", not "loading", and a throwing `dockwatch:live` listener does not stop
  `render()`. Timed from page `load` to the pill update, old (`ec88e8f`) and new code match (idle 7–17 vs 6–16 ms,
  6× CPU 67–125 vs 70–183 ms, 20× CPU 387–764 vs 353–833 ms). So "loading after 5 s" can only mean (a) the page's
  module graph never ran (a module request failed or stalled) or (b) the first `fetch("data/live.json")` never
  settled, and `fetchLive()` had no timeout, so (b) meant "Loading…" until the next 60 s poll. The one stall seen
  while investigating was on start-commit code (6× CPU, 390 px, second navigation: no `dockwatch:live` for 60 s), so
  the failure class predates the replay / paused work.
- **Fixes, one per open failure mode, no loosening.** (b) `fetchLive()` aborts a read after 10 s (`AbortController`
  + `setTimeout`, so the tests' fake clock drives it); a timed-out read is a failed read, so the pill says
  "Paused · No data yet" and the Live page shows its existing "Live station data couldn’t be loaded…" with Retry.
  (a) `tests/web/serve.py` speaks HTTP/1.1 with keep-alive (`protocol_version`, `daemon_threads`), so Chromium reuses
  a few sockets per page instead of a new TCP connection per file, the transport failure class this host has shown
  before (`ERR_CONNECTION_REFUSED`). No `playwright.config.js` change, no retries, no longer waits.
- **Failures explain themselves.** `helpers.js` `watchPage()` (called from `prepare()`) records, per navigation,
  failed requests, pending requests, page errors, console errors, and whether `data/live.json` was requested and
  answered; `open()` keeps its exact pill assertion and, if it fails, rethrows with that summary appended. Checked on
  a forced failure (a refused `replay.js`): the message read "data/live.json: requested 0x … failed requests:
  …/js/live/replay.js: net::ERR_CONNECTION_REFUSED", i.e. case (a), which the read timeout alone cannot fix.
- **New test** `shell.spec.js` › "freshness pill does not stay on Loading when live.json never answers": live.json
  never answers; after a 10 s fast-forward Insights' pill is Paused with "No data yet"; on Live the error and Retry
  show, and once live.json answers Retry brings the pill to Live and draws the markers. It fails with the timeout
  removed (pill still "loading").
- **Size budget.** `tests/test_replay.py::test_full_day_fits_the_size_budget`, as ruled: 641 stations with realistic
  metadata (UUID ids, ~24-character names, `SF-A12` codes, 6-decimal lat/lon, capacities 11–35), 1440 frames at
  `step_s` 60 on one Pacific day, frame 0 full, then 80 distinct stations per frame (`random.Random(20261007)`), each
  a ±1 bike move with docks = capacity − bikes, renting and freshly reported, so `classify()` sets the state; built
  with the real `build_replay()` and written with the real `write_json()`. 1,411,600 bytes (budget 1,500,000), with
  asserts on 641 stations, 1440 frames and 641 + 1439 × 80 encoded changes so it can't pass vacuously.
- Docs: DESIGN_BACKLOG #45 "Replay size budget", CURRENT_STATE (counts, read timeout, keep-alive).

**Decided**
- Size budget stays 1.5 MB; the synthetic rate is 80 changes/frame (100 measured 1,742,868 B; see the ruling line in
  the previous entry and DESIGN_BACKLOG #45).
- The read timeout is product code, not a test fix: a visitor on a stalled connection saw "Loading…" for up to 60 s.
  The 60 s poll and 15 s tick are unchanged; no backoff.

**Tried**
- Reproducing under the Checker's conditions: `python tasks.py test-spark` (one Spark container), then at once the
  shell suite `--repeat-each 5`, twice: 55/55 and 55/55. `test-spark`, then the nav-menu test `--repeat-each 20`:
  20/20. The shell suite `--repeat-each 3` while `test-spark` ran at the same time: 33/33. Earlier, read-only:
  50/50, 40/40 (8 workers), 30/30 with all 16 cores busy, and ~1,300 scripted first navigations at 6× CPU with no
  stall.

**Results**
- **The flake was not reproduced** in any of these runs, before or after the fixes, so its exact trigger is still
  unproven; (a) or (b) under memory pressure remains the most likely cause. The two fixes remove failure mode (b)
  and most of the connection churn behind (a), and any recurrence now names its cause in the test log.
- ruff + ruff format clean; host pytest 53 passed / 1 skipped; Playwright 35/35 (shell 11, live map 14, touch
  targets 3, replay 7).

## 2026-10-08 · F3 · alerts rail, peek bar and sheet; read timeouts for alerts and replay

**Built**
- **The alerts rail** (`web/js/live/alerts.js`, new), from `data/alerts.json` (exporter unchanged): "Open now" with
  a count badge, open alerts longest-running first, then "Recently resolved" (newest first, at most 20). The browser
  re-sorts defensively with a stable sort. Each row is a button: a 12 px state marker, the name (`translate="no"`),
  "Empty for 34 min" / "Full, resolved after 22 min", and "San Francisco · started 3:32 PM" (with the date when the
  start wasn't today in Pacific time). Rows for stations missing from `live.json` are static `div`s, so there are no
  dead buttons. Loading = skeleton rows with `aria-busy`; no open alerts = a `card-soft` sentence; a failed read with
  no earlier copy = "Alerts couldn’t be loaded. Check your connection, then try again." + Retry. A later failed read
  keeps the last good copy.
- **Per breakpoint:** from 1120 px `.live-layout` is `2fr 1fr` and the rail is sticky under the nav, with its rows
  scrolling inside (`overscroll-behavior: contain`). From 600 to 1119 px it is a card under the map whose body scrolls
  inside (`min(36rem, 70dvh)`), and a container query (`@container (min-width: 40rem)`) gives two columns of rows in
  the wide card. Below 600 px the section isn't shown; a fixed **peek bar** ("61 open alerts", 56 px plus the
  safe-area inset, bottom-sheet surface) opens the **alerts sheet** (`<dialog>`, `showModal()`). The page gets
  `padding-bottom` / `scroll-padding-bottom` of the bar's height, so the footer and focused elements are never under
  it. There is one list in the DOM: the body node moves into the sheet on open and back on close. Escape and "Close
  alerts" return focus to the peek bar.
- **Selecting a row** (DESIGN §7): it closes the sheet on phones and switches List to Map. If the station is in
  another region it `await`s `setRegion()` before selecting, because a region change hides the details. It turns
  "Show only problems" off if that would hide the station. Then `select(id, {focus: true})` focuses the marker and
  opens the tooltip (≥ 600 px) or the station sheet. The document click handler that closes the tooltip on outside
  clicks now treats `.alert-row` and the alerts sheet as inside; otherwise the tooltip closed right after opening.
- **In-place refresh and focus:** rows are keyed by `episode_id`. Text changes only when it differs, and a row moves
  only when it is out of place. Rows leaving a list are taken out first, so a resolved alert moves once instead of
  shifting every row after it. An alert that resolves moves to the resolved list as the same `<li>`/button. The
  list.js safety net puts focus back on the same episode, or its nearest neighbour, with `preventScroll`, and the
  body's `scrollTop` is kept.
- **Announcements:** a visually hidden `aria-live="polite"` region inside the rail says "New alert: <name> is
  empty." or "3 new alerts.". The first successful read only sets the baseline. Resolved alerts are not announced,
  and nothing is ever assertive. The region stays rendered on phones: the rail's other children are hidden, not the
  section.
- **Replay mode (decision, DESIGN_BACKLOG #46):** the rail stays in place and shows "Alerts aren’t part of the
  replay. Exit the replay to see live alerts.". Rows and badges are hidden, not destroyed, and the peek bar is hidden.
  The baseline keeps updating silently, so Exit doesn't announce a burst. This agrees with the KPI tile. Hiding the
  rail would re-flow the map. Replaying alerts would mean recomputing the pipeline's rule in the browser (§2), so
  that waits for a replay format v2.
- **Read timeouts:** a shared `readJson(url, {timeoutMs})` in `web/js/util/read.js` uses `AbortController` and
  `setTimeout`, with `clearTimeout` in `finally` after the body. `loadAlerts()` uses it with a **10 s** timeout: it
  reads at start (independently of `live.json`) and after every live poll, and an older read never overwrites a
  newer one. `loadReplay()` uses **30 s**: the file can be ~1.5 MB, and 10 s would fail on links slower than about
  1.2 Mbps. On timeout the existing "The replay couldn’t be loaded. Try again later." shows and `aria-disabled` is
  cleared. `freshness.js` `fetchLive()` is unchanged.
- **Tests:** `tests/web/alerts.spec.js` (13: order, copy and durations, desktop / tablet / phone layout, selection
  on desktop and phone, focus across refresh, announcements, replay note, 503 + Retry, a read that never answers,
  axe on rail / card / peek / sheet in both themes). In `replay.spec.js`, "Replay a day shows a message when
  replay.json never answers". The touch scan now also covers the 81 rail rows at 1280 px, and at 390 px the peek bar
  plus every row and "Close alerts" in the open sheet; in replay mode it checks that the peek bar and rows are gone.
  Variants are `structuredClone`s of the fixture.
- **Docs:** CURRENT_STATE now says the keep-alive test server *reduces* (doesn't remove) cause (a) of the old shell
  flake; the read timeout removes cause (b). DESIGN_BACKLOG #46–#51 were added, and #34, #39 and #44 updated.
  IMPLEMENTATION_PLAN F3 is ticked.

**Decided**
- The desktop rail is capped at `max(20rem, 100dvh − nav − gap − section padding − 10rem)`, not
  `100dvh − nav − 2 × gap`. At 1440 × 900 the full-height rail (804 px) was taller than the map card (~680 px), so
  sticky had no room and the rail's top slid under the nav at the end of the page. Capped, it stays whole under the
  nav from the top of the map to the footer (DESIGN_BACKLOG #49).
- Durations keep counting with the page clock while data is paused, like the list and details (DESIGN_BACKLOG #50).
- Marker `aria-describedby` (#44) stays deferred.

**Surprises**
- The station sheet and the alerts sheet share `is-scroll-locked`. A dialog's `close` event fires a task later, so
  the alerts sheet removes the lock only when no other `dialog.sheet` is open; otherwise selecting a row on a phone
  could unlock scrolling under the station sheet.
- `@container (min-width: 40rem)` measures the section, so the tablet card turns two-column from about 720 px
  viewports, not exactly at 768 px. The tests check 650 px (one column) and 900 px (two).

**Results**
- Playwright 49/49 (shell 11, live map 14, touch targets 3, replay 8, alerts 13). The alerts file `--repeat-each 3`:
  39/39. The focus test `--repeat-each 5`: 5/5. The shell suite `--repeat-each 5`: 55/55. The focus and timeout tests
  fail when their fix is removed: no focus safety net, and no alerts read timeout.

## 2026-10-08 · M6 (part) · CI on GitHub Actions

**Done**
- `.github/workflows/ci.yml`, on pushes to `main`, pull requests and `workflow_dispatch`, with five jobs:
  `Python lint + tests` (ruff, `ruff format --check`, host pytest through `uv sync --locked`), `Spark transform tests`
  (`docker build -t dockwatch-spark:3.5.5 infra/spark`, then `python tasks.py test-spark`), `Terraform fmt + validate`
  (`python tasks.py tf-fmt-check` and `tf-validate`, both in the `hashicorp/terraform:1.9` image), `Playwright suite`
  (all 49 tests) and `Playwright repeat (alerts + shell)` (`--repeat-each 3`).
- `permissions: contents: read`, a per-ref `concurrency` group that cancels older runs, `ubuntu-24.04`, actions pinned
  to major tags, job timeouts (10 / 20 / 10 / 30 / 30 min). No matrix, so check names stay fixed.
- Caching: uv (`setup-uv` `enable-cache`, uv pinned to 0.12.11), npm (`setup-node` `cache: npm`) and the Playwright
  browsers (`~/.cache/ms-playwright`, keyed on `package-lock.json`); `npx playwright install --with-deps chromium`
  always runs, so the OS packages are there on a cache hit. On failure the web jobs upload `test-results/` (traces).
- New task `python tasks.py tf-fmt-check` (`terraform fmt -check -recursive -diff`). README: CI badge, the task, and a
  short CI note.

**Decided**
- **Retries 0 on the command line.** `playwright.config.js` has `retries: CI ? 1 : 0`, which would hide a flake on
  GitHub. Both web jobs run `npx playwright test ... --retries=0`; the config is untouched, so the earlier
  no-loosening guards (config byte-identical) still mean something.
- **Spark tests in the Spark image**, the same `test-spark` command as locally, instead of a runner-side PySpark:
  the Dockerfile stays the one source of versions (Spark 3.5.5, Java 17, pandas / pyarrow). The image is rebuilt each
  run; exporting ~2.4 GB to the Actions cache would be slower than the build.
- **Terraform without credentials:** `fmt -check` and `validate` (`init -backend=false`). `terraform plan` needs the
  AWS account and the GitHub OIDC role, so it stays an unticked M6 item, as do `dbt compile` (M5) and `deploy.yml`
  (out of scope: no AWS account to publish to).
- Node 20 to match this machine, although Node 20 reached end of life in April 2026; moving local and CI to Node 22/24
  together is a follow-up.

**Verified locally** (nothing was pushed): actionlint 1.7.7 (with shellcheck) is clean. The run steps of the
`python`, `web` and `web-repeat` jobs pass verbatim in Linux containers (`python:3.12-slim-bookworm` with uv
0.12.11; `node:20-bookworm` with `--ipc=host`, 4 GB, `CI=true`) on a copy of the working tree. `tf-fmt-check`,
`tf-validate` and `test-spark` pass as local checks. The first run on GitHub is checked after the push.

## 2026-10-08 · M6 (part) · first CI run on GitHub
**Did**
- Pushed loop C (commit 9a42ba4) to GitHub. The first `CI` run (37875703602) passed with all five jobs green:
  Python lint + tests 12 s, Terraform fmt + validate 18 s, Spark transform tests 56 s, Playwright suite 2 min 28 s,
  Playwright repeat (alerts + shell) 3 min 22 s; about 3.5 minutes in total.
- Replaced "First run on GitHub: pending" in CURRENT_STATE with the run id and result.

**Found**
- During loop C the host ran out of memory twice: Claude Code's memory-pressure reaper stopped the GBFS producer
  (twice), a Checker run (after 8 of 69 checks; it was re-run in full after memory was freed and all 69 passed) and
  the CI-status poller. None of these were caused by the code under test. The producer is currently **not running**.

## 2026-10-09 · M3 (part 1) · the ops database, the simulator and the query plans
**Did**
- **Schema** `sql/ops/schema.sql` (idempotent, run by `python tasks.py ops-schema` with the migrations) in database
  `ops` on ops-db: `stations`, `docks`, `vans`, `rebalancing_jobs`, `maintenance_tickets`; foreign keys, `CHECK`
  constraints (ranges, status lists, "closed iff finished", "never closed before opened", "assigned needs a van",
  "done needs bikes_moved"), `created_at` / `updated_at` with a `set_updated_at()` BEFORE UPDATE trigger on every
  table, `REPLICA IDENTITY FULL` on all five (a delete or update will carry the whole old row, so Debezium's
  `before` image is complete and there are no unchanged-TOAST placeholders), and publication `dockwatch_ops` on
  exactly those five tables. Only `timestamptz`, `double precision`, `integer` / `bigint` / `smallint`, `boolean`
  and `text`, so the CDC type mapping stays small. The replication slot is not created here: the Debezium connector
  will create `dockwatch_ops` (a slot created now would hold WAL until Connect exists).
- **Migration** `sql/ops/migrations/001_rebalancing_jobs_priority.sql`: `ADD COLUMN IF NOT EXISTS priority smallint`
  with a CHECK and deliberately **no DEFAULT** (a default fills old rows without WAL events, so Postgres and Iceberg
  would never agree). This is the column that will flow end to end through Debezium.
- **Simulator** `src/dockwatch/ops_sim/`: `model.py` is pure (alerts + simulated clock -> `Op`s, one seeded RNG,
  sorted iteration); `synthetic.py` makes 80 seeded stations, 4 vans and a `gbfs.alerts` stream with the exact M2
  payload (alert after 15 min, resolved at the episode end); `db.py` runs the ops with psycopg, one transaction per
  simulated minute, holds the SQL as module constants, counts calls per constant, and counts the change events
  Debezium should see per table and op from each statement's `rowcount` (a mismatch with the model raises
  `Conflict`); `__main__.py` has `schema`, `seeded`, `live` and `explain`. psycopg 3.3.6 is in a new uv group `cdc`,
  added to `default-groups` so CI's `uv sync --locked` installs it (`ci.yml` unchanged).
- **EXPLAIN ANALYZE** (`python tasks.py ops-explain` -> `sql/ops/PLANS.md`, ~12 s): a scratch database `ops_bench`
  is created and dropped on ops-db (the `ops` database, its publication and `iceberg_catalog` are untouched). The
  seeded run there counts calls per constant; the two most-called are the lookups `OPEN_TICKET_ON_DOCK` (365) and
  `OPEN_JOB_AT_STATION` (308), ahead of `SET_DOCK_STATUS` (243). The bench then reloads the schema without their two
  partial indexes, loads 2,000 stations, 40,000 docks and 200,000 jobs and tickets (`setseed` + `generate_series`),
  `ANALYZE`, and runs `EXPLAIN (ANALYZE, BUFFERS)` with parallel workers and JIT off: Seq Scan 15.6 ms -> Index
  Scan on `rebalancing_jobs_open_station_idx` 0.02 ms, and Seq Scan 10.0 ms -> Index Scan on
  `maintenance_tickets_open_dock_idx` 0.04 ms.
- **CDC helpers** (pure, Python 3.10-safe for the Spark image): `cdc/checksum.py` (canonical text per value, sha256
  per row, sha256 of the sorted row hashes; `_` columns dropped) and `cdc/guard.py` (Spark CDC tasks and `catchup`
  refuse while Kafka Connect or `status-stream` runs, `connect-up` while `cdc-apply` or `status-stream` runs; a reset
  may delete only `ops.public.*` and `dockwatch-connect-*` topics).
- Tests: `tests/test_ops_sim.py` (7) and `tests/test_cdc.py` (4 so far) in the host suite, now 64 pass, 1 skipped;
  `tests/test_ops_db.py` (5, `integration`, each test rolled back so no CDC events) passes against ops-db.

**Measured** (seed 42, 1500 simulated minutes, 80 stations): 1,355 operations; expected change events
stations c 80 / u 15, docks c 1,774 / u 243, vans c 4 / u 238, rebalancing_jobs c 133 / u 175 / d 22,
maintenance_tickets c 123 / u 242: 3,324 in total, well under the ~15k budget, with 22 job deletes (>= 10 needed).
Running it against Postgres took 1.5 s and matched the model with no `Conflict`.

**Decided**
- Jobs and tickets are addressed the way an operator would ("the unfinished job at this station", "the unfinished
  ticket on this dock"), and those lookups guard every insert. That makes them the real hot path and gives the two
  partial indexes a reason to exist; job deletes go through the unique `alert_episode_id`.
- 4 vans, so some jobs wait for a van: this keeps `OPEN_JOB_AT_STATION` clearly ahead of the van status updates.
  `ops-explain` fails if the two queries it explains stop being the two most-called.
- Docks are inserted per station with one `generate_series` statement, so setup does not dominate the call counts;
  the expected event count uses its `rowcount`.

**Memory:** the host had 80–110 MiB free physical memory (about 330 MiB available) during this work, far below the
~1.5 GB the plan asks for before Kafka Connect or Spark. Nothing beyond the running stream stack was started: no
Connect, no Spark, no image pull. Kafka Connect, the Debezium connector, the Spark `MERGE INTO` apply into
`lake.ops.*`, the `cdc-e2e` run and the idempotent replay are the next part of M3.

## 2026-10-09 · M3 · CDC from the ops database
**Did**
- **Kafka Connect + Debezium.** Compose service `connect` (image `quay.io/debezium/connect:2.7.3.Final`, pinned in
  `tasks.py` `CONNECT_IMAGE` too) in its own profile `cdc`, not `stream`: `mem_limit: 1g`, `KAFKA_HEAP_OPTS` and
  `HEAP_OPTS` = `-Xms256m -Xmx512m`, `restart: "no"`, healthcheck `curl -fs localhost:8083/connectors`, internal
  topics `dockwatch-connect-{configs,offsets,status}` (replication factor 1; Redpanda accepted Connect's compacted
  topics as is, no `rpk` step needed). The effective heap read from the container's `/proc/*/cmdline` is
  **`-Xmx512m`**. The connector `infra/connect/ops.json` (`ops-postgres`): pgoutput, database `ops`, the five
  `public.*` tables, publication and slot `dockwatch_ops`, `publication.autocreate.mode=disabled`,
  `snapshot.mode=initial`, `tombstones.on.delete=false`, JSON converter with schemas for key and value, heartbeat
  10 s, one partition per table topic.
- **Fixtures from the real connector.** `tests/fixtures/cdc/*.jsonl`: 23 records captured from `ops.public.*` after
  a seeded run (a job created, updated and deleted before the column add; jobs created, updated and deleted after
  it; and a few stations, docks, vans and tickets records), kept verbatim. They showed two things the code had to
  follow: the JSON converter writes FLOAT64 as `"double"` (not `float64`), and Postgres `smallint` arrives as
  `int16`.
- **`cdc/events.py`** (pure, Python 3.10): Connect schema -> Iceberg types (int16/int32 -> int, int64 -> bigint,
  double -> double, boolean, string, `ZonedTimestamp` / `MicroTimestamp` -> timestamp; anything else fails loudly),
  primary key from the key schema, delete key and values from `before`, newest event per key by (`source.lsn`, Kafka
  offset), `plan_evolution()` (add-only, a type change is refused) and the MERGE SQL.
- **`cdc/apply.py`** (Spark, service `cdc-apply`, profile `cdc-apply`, `dockwatch-spark:3.5.5`, `mem_limit: 2g`,
  `local[1]`, driver 1g, `TZ=UTC`, `restart: "no"`): Kafka `subscribePattern` `ops\.public\..*`, availableNow,
  `maxOffsetsPerTrigger` 1000; `foreachBatch` collects the bounded batch, and per table creates `lake.ops.<table>`
  (format v2, unpartitioned) if missing, adds new columns, dedupes and runs **`MERGE INTO`** with the stale-event
  guard (`WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN DELETE`, `… THEN UPDATE`, `WHEN NOT MATCHED AND
  s._op <> 'd' THEN INSERT`). Metadata columns `_lsn`, `_op`, `_source_ts`, `_kafka_offset`, `_applied_ts`.
  `--replay` uses a fresh checkpoint from earliest and deletes it afterwards; `--reset` drops `lake.ops.*` (PURGE)
  and `/checkpoints/cdc`. `cdc/iceberg_checksums.py` is the Iceberg side of verify.
- **Host runner** `cdc/runner.py` + `python -m dockwatch.cdc` and the tasks `connect-up`, `connect-stop`,
  `cdc-reset`, `cdc-catchup [--replay]`, `cdc-verify` and **`cdc-e2e`**. `catchup`, `cdc-catchup`, `cdc-reset` and
  `cdc-verify` refuse while `connect` (or `status-stream`) runs; `connect-up` refuses while `cdc-apply` or
  `status-stream` runs; `cdc-e2e` stops a leftover Connect first and always stops it at the end.
- Tests: `tests/test_cdc.py` gained the 5 envelope / schema tests on the captured fixtures (host suite now 69 pass,
  2 skipped); `tests/spark/test_cdc_apply.py` (4: inserts / updates / deletes across two batches, a stale update
  ignored, replay changes nothing — not even `_applied_ts` — and the column evolved before the MERGE) drives the
  real `foreachBatch` function on a hadoop Iceberg catalog in `tmp_path`, and stops its session because
  `spark.sql.extensions` is static (`test-spark`: 8 pass).

**Measured** (`python tasks.py cdc-e2e --seed 42 --events 1500`, 83 s for all 11 steps):
- Kafka `ops.public.*` = the simulator's expected changes exactly: stations c 80 / u 15, docks c 1,774 / u 243,
  vans c 4 / u 238, rebalancing_jobs c 133 / u 175 / d 22, maintenance_tickets c 123 / u 242; 0 snapshot (`r`)
  events. That is **3,049** change events (the part-1 entry above says 3,324 in total; its per-table numbers are
  right and add up to 3,049).
- Postgres = Iceberg after the apply, rows and checksum per table: stations 80, docks 1,774, vans 4,
  rebalancing_jobs 111 (133 created − 22 deleted), maintenance_tickets 123. `priority` exists on both sides with 55
  non-null values each. The apply read the 3,049 events in 4 batches; batch 1 ran `ALTER TABLE … ADD COLUMN
  priority int` before its MERGE.
- Replay: `cdc-catchup --replay` read all 3,049 events again (4 batches, 24 s), and `cdc-verify` found the same rows
  and checksums: the apply is **idempotent**.
- Memory (peak per container, `docker stats` every ~3 s): `connect` 457–518 MiB of its 1 GiB `mem_limit`;
  `cdc-apply` 1,170 MiB in the apply step (2 GiB limit), 480–600 MiB for verify and reset. Connect and Spark never
  ran in the same step (recorded per step in `data/cdc/e2e-report.json`). Timings: connect-up 14 s, the three
  simulation steps 2–3 s each, Connect caught up within 2 s, apply 28 s, verify 10 s.
- Host free memory swung between ~0.4 and 3.3 GB during the work (other projects); every JVM was started only
  above ~1.4 GB, and Kafka Connect was stopped before each Spark start.

**Decided**
- **The replication slot is owned by the connector, not by `ops-schema`** (this deviates from plan decision 8).
  A slot created by `ops-schema` would hold WAL from the moment the schema exists until Connect first runs, which may
  be never. Debezium creates `dockwatch_ops` (`slot.name`) when the connector is registered, and `cdc-reset` /
  `cdc-e2e` drop it with Connect stopped. `ops-schema` still owns the publication (`publication.autocreate.mode`
  disabled). `ops-db`'s optional `max_slot_wal_keep_size` cap was not added (it would recreate ops-db, and the
  slot only exists while CDC is in use).
- **Batches are collected to the driver** and decoded by the pure `events.py`; only the MERGE runs as Spark SQL.
  The batches are bounded (1,000 records) and the tables small, and it keeps the envelope logic unit-testable on
  the host.
- `maxOffsetsPerTrigger` 1000 instead of 5000: with 5000 the whole run was one batch, Iceberg created
  `rebalancing_jobs` with `priority` already there, and the ADD COLUMN path was never exercised end to end.
- Every compose call for Connect passes `--profile stream --profile cdc`: `connect` depends on `stream` services,
  and compose rejects the project with only `cdc` active.
- Deviations kept from the plan: a bounded seeded run instead of "an hour of simulation" (decision 2), and Kafka
  Connect in its own on-demand `cdc` profile instead of `stream` (decision 3, memory).

## 2026-10-09 · M3 hardening · WAL cap, lost-slot check, CDC topic retention
**Did**
- **WAL cap on ops-db.** `docker-compose.yml`: ops-db's `command` gains `-c max_slot_wal_keep_size=1024MB` (nothing
  else in the file changed). Applied with one deliberate recreate, volume kept: `stream-stop` and `connect-stop`
  (neither was running), `docker compose --profile stream up -d --wait --no-deps ops-db` (recreated and healthy in
  11.9 s), then `docker compose --profile stream restart iceberg-rest` (1.4 s) to drop its pooled JDBC connections.
  The REST catalog answered again 14.6 s after the recreate started; that is the catalog's downtime. (A first try ran
  the same commands before the compose edit was saved: ops-db stayed as it was and only iceberg-rest restarted once
  more, which is harmless.)
- **Measured before / after the recreate:** `SHOW max_slot_wal_keep_size` went from `-1` (no cap) to `1GB`. The slot
  `dockwatch_ops` survived the recreate (persistent slots are on disk), still `wal_status = reserved`, inactive,
  retaining ~1.27 MB; its `safe_wal_size` went from NULL (no limit) to 1,077,300,752 bytes (~1 GiB minus what it
  retains). `pg_wal` is 272 MiB. `curl localhost:8181/v1/namespaces/ops/tables` lists all 5 ops tables, and all 9
  tables in `iceberg_catalog` (bronze.station_status, silver.station_status, silver.station_episodes,
  silver.availability_5m and the five ops.*) are still registered and load through the REST catalog.
- **What the cap means.** It bounds only what a *slot* may hold back; the `iceberg_catalog` database uses no slot and
  behaves as before. But WAL is cluster-wide, so an idle `dockwatch_ops` used to pin the catalog's commit WAL too
  (status-stream / catchup); now that is bounded at ~1 GB plus `pg_wal`'s normal size. The price: if more than 1 GB
  of WAL is written while Connect is stopped, Postgres invalidates the slot (`wal_status = 'lost'`) and CDC has to
  be rebuilt with `cdc-reset` and a fresh snapshot. The ops data is simulated, so that is acceptable; the next
  point makes it loud.
- **Lost-slot detection before Connect starts.** `guard.slot_refusal(slot, wal_status)` (pure): None for no slot (the
  connector creates it), `reserved` or `extended`; for `lost` a refusal that names `max_slot_wal_keep_size`, says
  what `python tasks.py cdc-reset` drops (lake.ops.* and its checkpoints, the slot, the five ops tables, re-created
  empty, and the ops.public.* / dockwatch-connect-* topics) and that the next connect-up re-snapshots.
  `guard.slot_warning()` returns a warning for `unreserved` (the slot may be lost at the next checkpoint): it is
  printed to stderr naming `cdc-reset` as the fallback, and Connect still starts, since starting it now may save the
  slot. `runner.slot_status(slot)` reads `wal_status` from `pg_replication_slots` (psycopg, `connect_timeout=10`).
  `connect_up()` now runs: memory guard, then the slot check (a `lost` slot raises `CdcError`, so nothing starts
  Connect), then the retention step below, then compose up / register / wait as before. The CLI prints
  `cdc connect-up: replication slot dockwatch_ops is lost (wal_status = 'lost') ... python tasks.py cdc-reset ...`
  and exits 1. `cdc-e2e` needs no change (it drops the slot before its connect-up).
- **Retention for `ops.public.*`: unlimited (`retention.ms=-1`, `cleanup.policy=delete`), those five topics only.**
  - *7 days (the old default)* would let a from-scratch rebuild (`cdc-catchup --replay`, or a fresh checkpoint after
    losing Iceberg) silently miss every row whose last change is older than 7 days, and a normal catch-up after more
    than 7 days with Connect stopped would hit `failOnDataLoss`.
  - *Compaction* would bound storage, but it leaves offset gaps under a checkpoint that the apply's Kafka source
    (default `failOnDataLoss=true`) may fail on, and it throws away the per-row change history replay and audits use.
  - *Unlimited* keeps every change; the topics grow with each change. The e2e produces 3,049 events (a few MB) and
    `cdc-reset` clears them. Revisit (compaction plus snapshot-based rebuilds, M4 maintenance) if `ops.public.*` passes
    ~1 GB or ~1M events; `metric-e-slot` prints the high watermarks.
  - New topics: `infra/connect/ops.json` gains a topic-creation group `cdc` (`topic.creation.cdc.include` =
    the regex `ops\.public\..*`, full match, written with doubled backslashes in JSON; `retention.ms=-1`,
    `cleanup.policy=delete`, partitions / replication factor 1 like the defaults). No `topic.creation.default.*` retention keys, so `__debezium-heartbeat.ops` keeps the broker default.
  - Existing topics: `runner.ensure_cdc_topic_retention(client)` sets the same two keys with
    `incremental_alter_configs` on every existing `ops.public.*` topic and nothing else; idempotent; called from
    `connect_up()`. Run by hand once before the e2e, it moved the five 7-day topics (`DEFAULT_CONFIG` 604800000) to
    `retention.ms=-1` as a dynamic topic config; a second call changed nothing new. The heartbeat (7 days, default)
    and `gbfs.alerts` (28 days) kept theirs.
- Tests: 7 host unit tests in `tests/test_cdc.py` (lost slot refused with `cdc-reset` in the message; none /
  reserved / extended allowed; unreserved warns but allows; `connect_up` raises before compose up and before the
  topic alter; the CLI exits 1 naming `cdc-reset`; retention set only on the 5 ops topics through a fake admin
  client; `ops.json`'s group covers exactly `ops.public.*` with `retention.ms=-1`). Host suite: 76 pass, 2 skipped.
  1 integration test in `tests/test_ops_db.py` creates a temporary physical slot on its own connection, reads
  `reserved` through `runner.slot_status`, and drops it (`dockwatch_ops` is never touched); 6 integration tests pass.

**Measured (the carried CDC run, under the cap)**
- Host free memory: 2,268 MiB at the start, 2,894 MiB before the e2e, 2,122 MiB before the replay.
- `python tasks.py cdc-e2e --seed 42 --events 1500`: ok in 89 s, 3,049 events, all five tables equal (rows and
  checksums), `priority` non-null on 55 rows on both sides; peak `connect` 513 MiB, `cdc-apply` 1,075 MiB. Connect
  re-created the five topics through the `cdc` group: `retention.ms=-1`, `cleanup.policy=delete`.
- After the run the slot is `reserved`, inactive, retaining 1,356,920 bytes, `safe_wal_size` 1,075,143,568 bytes;
  `pg_wal` 272 MiB. High watermarks: docks 2,017, maintenance_tickets 365, rebalancing_jobs 330, stations 95,
  vans 242 (3,049 in total).
- `cdc-catchup --replay`: 3,049 events in 4 batches, 23.5 s; `cdc-verify`: ok, all five tables match.

**Correction (M3 entry, 2026-10-09)**
- The M3 entry says the cap was not added because "the slot only exists while CDC is in use". That is wrong: the
  slot is created by the first `connect-up` and persists, inactive, while Connect is stopped, until `cdc-reset` (or
  `cdc-e2e`'s reset) drops it, so it pins WAL between CDC runs. It is now capped by `max_slot_wal_keep_size`
  (1024MB), and a slot lost to the cap is reported by `connect-up` with the way out (`cdc-reset`).

## 2026-10-09 · CI · off Node 20 (Node 24, node24 action runtimes)
**Did**
- **Node 24 for both Playwright jobs** (`node-version: 24`, major only, as `20` was). Node 20 reached end of life on
  2026-04-30. From the Node release schedule: Node 22 is in maintenance until 2027-04-30; Node 24 is Active LTS
  (maintenance from 2026-10-20) until 2028-04-30; Node 26 becomes LTS only on 2026-10-28. 24 buys a year more than
  22 for the same one-line change. Playwright 1.64.0's system requirements say "Node.js: latest 22.x, 24.x or 26.x"
  (Node 20 is no longer listed); its packages still declare `engines.node ">=20"`.
- **Actions off the deprecated `node20` action runtime**, each to its latest major (read from each tag's
  `action.yml`, `runs.using: node24`): `actions/setup-python@v7` (was v5, 4 places), `actions/setup-node@v7` (was
  v4, 2), `actions/cache@v6` (was v4, 2), `actions/upload-artifact@v7` (was v4, 2; v5 is still node20),
  `astral-sh/setup-uv@v10.3.0` (was v6). `actions/checkout@v5` was already on node24 and stays. Pinning is still by
  version tag, no SHAs; setup-uv is an exact tag because since v8 it publishes no major or minor tags (immutable
  releases only), so `v10.3.0` is the pin. None of the breaking changes in between touch the inputs we use
  (`pip-install`, `server-url`, the old `manifest-file` format are unused; `cache: npm` is set explicitly;
  `enable-cache: true`; artifact `name` / `path` / `retention-days` unchanged).
- Nothing else in `ci.yml` changed: job ids and names, timeouts, `--retries=0` on both Playwright lines, the npm and
  Playwright caching, the failure-only artifact upload, permissions and concurrency are byte-identical.
- **`engines`:** `package.json` and the lock's root `packages[""]` gain `"engines": {"node": ">=20"}` (hand-edited,
  no `npm install` on the host). `>=20` is the real floor: what Playwright declares, what this machine runs and what
  `env-preflight` checks. `>=24` would make npm warn (`EBADENGINE`) on every host `npm ci` and claim a floor the
  project's checks don't enforce.
- **Host unchanged:** this machine stays on Node v20.18.0 (npm 11.11.1). CURRENT_STATE records the local / CI split
  and the upgrade step under *Known gaps*; raising `engines` and the env-preflight floor to 24 is the follow-up after
  that upgrade.
- **Re-based checks:** `check_d.py` (`accept-d-scope`, `accept-d-guard`) and `check_e.py` (`accept-e-scope`,
  `accept-e-guard`) now diff their own loop's committed range (`954d941..b3ee891`, `b3ee891..0921a0c`) instead of the
  working tree, so loop F's legitimate `.github/` and `package*.json` edits don't trip them; the working tree is
  covered by the new `accept-f-scope` / `accept-f-guard`.

**Verified locally** (nothing pushed): actionlint 1.7.7 is clean; `accept-ci-structure` and `accept-ci-parity` pass;
every `uses:` ref reads `runs.using: node24` on GitHub. The jobs' run steps pass verbatim in `node:24-bookworm`
(`node --version` v24.21.0; `--ipc=host`, 4 GB, `CI=true`): the web job 49 passed in 116 s wall time (including the
first pull of the image; Playwright 1.2 min), the repeat job 72 passed in 149 s (Playwright 2.0 min). The same jobs
on `node:20-bookworm` in loop E took 111 s and 161 s, so no slowdown. The container's `npm ci` accepted the edited
lock. The python job passes in `python:3.12-slim-bookworm` (76 passed, 2 skipped). The new action majors themselves
can only be proved by the GitHub run after the push.

## 2026-10-09 · M6 (part) · Node 24 CI run on GitHub; path-casing hazard recorded
**Did**
- Pushed loop F (commit 4f0dd72). CI run 38015779233 passed: all five jobs green on Node 24 with the bumped action
  majors (setup-python v7, setup-node v7, cache v6, upload-artifact v7, setup-uv v10.3.0), no deprecation annotations.
- Recorded the Docker path-casing hazard in CURRENT_STATE's known gaps.

**Found**
- During loop F the Builder ran checks through a cmd.exe-based runner. The folder is `DockWatch` on disk while the
  containers were created from Git Bash as `dockwatch`; the different casing made Compose recreate `ops-db` mid-run,
  which dropped iceberg-rest's pooled catalog connections, and the CDC apply failed. The Builder was (rightly) denied
  recreating ops-db itself; the user then authorized the restore (`ops-db` recreated from the lowercase path with the
  volume kept, iceberg-rest restarted). The Checker, run from Git Bash, then passed all 99 checks with ops-db untouched.
- `upload-artifact@v7` is only exercised when a Playwright job fails, so it is still unproven on GitHub.

## 2026-10-10 · M4a · Airflow + Trino on demand, Iceberg maintenance, retries / alerts, casing guard (in progress)
**Split.** M4 is two loops. **M4a** (this one): Airflow and Trino in the `batch` profile, `iceberg_maintenance`,
`partition_evolution_demo` (M2's deferred item), `cdc_slot_keepalive`, retries and alerts, the path-casing guard.
**M4b** (next): `trip_history_monthly` (trip ZIPs, both schemas, `bronze.trips`, backfill), the dbt project
(`dbt-trino` locally, `dbt-athena` on AWS) with the three marts, and `marts_daily`. Each of Airflow, Trino, dbt and
the trip download is new on a host with ~1–3 GB free, so one loop for all of it would not fit.

**Did**
- **Trino 483** (`trinodb/trino:483`, service `trino`, profile `batch`) is the batch SQL engine: one JVM (heap
  `-Xmx1G`, `query.max-memory-per-node` 512 MB + heap headroom 256 MB, `task.max-writer-count=1`) for maintenance now
  and dbt in M4b. Athena engine v3 is Trino, so one dbt project can target both. Catalog `lake` uses the same Iceberg
  REST catalog and SeaweedFS warehouse as Spark (`fs.native-s3.enabled`, path-style), with
  `iceberg.expire-snapshots.min-retention=1d` and `iceberg.remove-orphan-files.min-retention=1d` (Trino's 7-day
  default would do nothing on a lake a few days old).
- **Airflow 2.11.2** on Python 3.12 (`dockwatch-airflow:2.11.2` from `infra/airflow/Dockerfile`; `trino`,
  `pydantic-settings`, `pytest` through Airflow's constraints file, `psycopg[binary]==3.3.6` pinned to the host lock),
  **LocalExecutor**, metadata in its own **`airflow-db`** (`postgres:16-alpine`, `mem_limit: 256m`, own volume).
  Not `ops-db`: a scheduler writing there all day would make WAL that the CDC slot `dockwatch_ops` has to retain,
  eating the 1 GB `max_slot_wal_keep_size` budget. Services `airflow-scheduler`, `airflow-webserver` (port 18090,
  one worker) and `airflow-cli` (one-shot runs), all `mem_limit: 1g`, `restart: "no"`, DAGs paused at creation.
- **Tasks** (`src/dockwatch/batch`, host CLI `python -m dockwatch.batch`): `batch-up`, `batch-stop`, `airflow`,
  `dag-test` (bounded `airflow dags test` in `airflow-cli`; the run's real state is read from the metadata DB because
  `dags test` exits 0 on a failed run; peak memory per container; `data/airflow/dag-test-<dag>.json`),
  `test-airflow`, `scheduler-up` / `scheduler-stop`, `trino-sql`.
- **Memory guard** (`batch/guard.py`, stdlib only): batch work refuses beside `status-stream`, `cdc-apply` or
  `connect`; `stream`, `catchup`, `inspect`, `verify-lake`, `sql`, `connect-up` and the CDC tasks refuse while Trino
  runs (the message names Trino and `batch-stop`). `cdc/runner.py`'s `guard()` consults it too.
- **DAGs:** `iceberg_maintenance` (`@daily`: `optimize`, `expire_snapshots`, `remove_orphan_files` per table, one
  table at a time, before / after into `lake.metrics.maintenance_runs`), `partition_evolution_demo` (manual) and
  `cdc_slot_keepalive` (`@hourly`), all `catchup=False`, `max_active_runs=1`.
- **Retries and alerts:** one shared `default_args`: `retries=3`, 1 min with exponential backoff up to 30 min, a
  per-task timeout, `on_failure_callback`; a DAG `sla_miss_callback` and an `sla` on each last task. An alert is one
  JSON line in `data/airflow/alerts.jsonl` plus an ERROR log line, and an SNS message when
  `DOCKWATCH_ALERTS_TOPIC_ARN` is set. Guard failures raise `AirflowFailException` (no retry).
- **`verify-lake` ignores `replace` snapshots** (compaction) in its freshness rule, so a maintenance run alone cannot
  make a stopped stream look fresh.
- **Path-casing guard:** `tasks.py` refuses every task when `os.getcwd()` differs from the
  `com.docker.compose.project.working_dir` label of the existing containers, with the recovery text; Docker down or
  no containers (CI) allow. New compose calls run from the inherited cwd, never a resolved path.
- **`docker.sock`** is mounted only into the Airflow services (listing containers, `docker start` / `stop` of the
  existing `connect`); the Airflow user reaches it through group 0 on Docker Desktop, so no `user:` / `group_add`.
  Root-equivalent on the Docker VM; recorded as a local-only gap.
- **Ops topics stay uncompacted (decision):** `ops.public.*` keep `cleanup.policy=delete`, `retention.ms=-1`.
  Compaction needs tombstones (off) and would break `cdc-e2e`'s per-op counts; revisit past ~1 GB / ~1M events.
  The Iceberg side, `lake.ops.*`, is compacted by `iceberg_maintenance`.
- Airflow logging config (`batch/airflow_logging.py`): alembic's plugin-setup INFO lines were printed to stdout by
  every Airflow CLI call, ahead of `-o json` output; the `alembic` loggers now log warnings only.

**Measured**
- **Spike:** Trino read all 9 tables (bronze / silver 989,753 rows each, `availability_5m` 202,182, episodes 1,855,
  ops 80 / 1,774 / 4 / 111 / 123) and wrote to SeaweedFS (CTAS, INSERT, `optimize`, `expire_snapshots`,
  `remove_orphan_files` on a lab table) with no S3 checksum setting needed. After a Trino maintenance run, Spark
  `catchup` (31 s) and `verify-lake` passed: the current bronze / silver snapshot is a Spark `append` after the
  `replace`.
- **`iceberg_maintenance`** (`dag-test`, 37 s): bronze 71 → 4 files (17.8x), 68 → 10 snapshots, 75 orphan files
  removed; silver 405 → 4 files, 7.6 MB → 3.4 MB, 95 → 19 snapshots; `availability_5m` 39 → 4 files; episodes 4 → 4
  files, 67 → 9 snapshots; the five `ops.*` tables already had one file each (8 snapshots, all younger than a day).
  One file per day partition after optimize; rows unchanged on every table.
- **Peaks:** Trino 1,532 MiB against its first `mem_limit` of 1536m. Per the plan's rule (> 1.4 GiB) the
  `mem_limit` is now 2g; the heap stays 1 GB. `airflow-cli` 233 MiB, `airflow-db` 49 MiB, the scheduler 395 MiB.
  `batch-up` takes 28 s; `test-airflow` 22 s (6 tests).
- **Scheduler smoke test:** healthy within the 180 s, the three DAGs listed and paused, no import errors.
  Airflow 2.11.2's `dags list -o json` names the column `is_paused`.
- **CDC slot keepalive — Debezium does not advance on WAL from another database.** With 78.7 MiB retained (the
  baseline's probe wrote it in a scratch database), `cdc_slot_keepalive` started Connect, the connector was RUNNING
  and the slot active, and for 10 minutes `confirmed_flush_lsn` stayed at `0/59FE4D40` while `sent_lsn` followed the
  current WAL. Connect logged "Received offset commit request on 'LSN{0/59FE4D40}', but ignoring it. LSN flushing is
  not allowed yet": after a restart Debezium flushes nothing until a change from the `ops` database arrives past its
  stored offset. The plan's fallback, `heartbeat.action.query` = `SELECT pg_logical_emit_message(false,
  'dockwatch-heartbeat', now()::text)`, was tried three times (with `message.prefix.exclude.list`, with nothing, and
  with `message.prefix.include.list`; 1–3 min each): the query ran (WAL grew ~100 bytes per 10 s), but no message
  reached Debezium (no `ops.message` topic) and the slot did not move. `infra/connect/ops.json` is unchanged and the
  stored connector config was re-registered from it. `safe_wal_size` is ~992 MB, `reserved`.

**Not done yet (M4a)**
- Partition evolution: `partition_evolution_demo` (day, then hour, on a lab copy) has not run yet, so there are no
  day vs hour file numbers; M2's partition-evolution item stays unticked until it has.
- `cdc_slot_keepalive` cannot catch up an idle slot (above); its decision, guard, Docker start / stop, alerting
  and `slot_health.jsonl` lines are built and unit-tested.

## 2026-10-10 · M4a · keepalive by touching a captured row, partition evolution measured (M4a done)
**Keepalive: a real change on a captured table replaces the heartbeat-query fallback.**
- **Why the first keepalive hung:** after a restart Debezium flushes no LSN until it streams a captured change past
  its stored offset. WAL from other databases is never decoded into this slot, pgoutput drops empty transactions,
  and `pg_logical_emit_message` (the plan's `heartbeat.action.query` fallback, three variants, entry above) never
  reached Debezium. A dedicated heartbeat table is ruled out because the connector's `table.include.list` and the
  publication must stay exactly the five ops tables. The one lever left is a real change to a captured table.
- **Did:** once the connector is `RUNNING`, `slot.py` runs one autocommit `UPDATE vans SET updated_at = now() WHERE
  van_id = (SELECT min(van_id) FROM vans) RETURNING pg_current_wal_lsn()` (an empty `vans` fails clearly) and waits
  until `confirmed_flush_lsn` ≥ max(LSN at start, touch LSN) and retained < 16 MiB, all inside the 600 s
  `timeout_s`. On timeout Connect is stopped (`finally`), a `failed` line names the stuck `confirmed_flush_lsn` and
  the target, and `dag_tasks` raises `AirflowFailException`, so a hung catch-up is not retried into 4 × 600 s.
  `slot.py` stays free of Airflow (`KeepaliveError`). New optional `touch_lsn` on `slot_health.jsonl` lines.
  `infra/connect/ops.json` is unchanged.
- **One touch was not enough (measured).** First run (77.7 MiB WAL probe on top of 78 MiB already retained):
  `confirmed_flush_lsn` moved from `0/66DA2B58` past the touch to `0/70978298` within seconds, but `restart_lsn`
  only moved to `0/66DA4B68`, so 155.8 MiB stayed retained. Postgres moves `restart_lsn` only when a confirmed LSN
  passes a pending restart candidate, keeps one candidate at a time (taken from a running-xacts record), and decoding
  the backlog leaves that candidate near the backlog's start; the first confirm applies it, and an idle database
  logs no newer running-xacts record for the next one. A second (manual, diagnostic) touch made the slot drop to 56
  bytes 16 s later. So `catch_up` now touches again, at most every 20 s (after the bgwriter's 15 s running-xacts
  interval), whenever a touch is confirmed but retained is still ≥ 16 MiB. That diagnostic run is not counted.
- **Measured (clean run, 2026-10-10 13:56 UTC,** `dag-test cdc_slot_keepalive catch_up_above_mb=16` after the
  checker's 77.7 MiB probe): retained 81,432,328 B → 56 B, `safe_wal_size` 999,157,016 → 1,082,994,728 B, slot
  `reserved`; touch 1 at `0/7572CA98` confirmed (`0/7572CE58`) with 77.7 MiB still retained, touch 2 at `0/7572CE90`,
  then confirmed `0/7572CFA0` = `restart_lsn`. Connect up 42.8 s, `dag-test` 61.2 s; peaks `connect` 453 MiB,
  `airflow-cli` 253 MiB, `airflow-db` 30 MiB. `check_g.py keepalive`: ok.
- **Trade-off:** each catch-up bumps the lowest-id van's `updated_at`, usually twice: one or two `u` events on
  `ops.public.vans` per catch-up, at most ~48 a day at the hourly cadence (catch-ups only happen past 256 MiB
  retained or after 24 h, so normally far fewer). `cdc-catchup` applies them like any other change; `cdc-e2e` resets
  the topics first, so its per-op counts are unaffected. The keepalive threshold stays 256 MB: no idle-growth rate
  from `slot_health` lines yet suggests it fills in < 6 h.

**Partition evolution measured (M2's deferred item, done).** `partition_evolution_demo` (Checker's run, 31.4 s,
days 2026-10-08 / 2026-10-09, one INSERT per hour) on `lab.status_partition_evolution`: 821,289 rows. Spec 0
(`day`) held D1's 551,260 rows in 15 files in 1 partition; after evolving to `hour`, D2 added 10 files in 10
partitions (270,029 rows); `optimize` rewrote everything into the current spec: 25 partitions, 25 files, 2.6 MB. So
hourly partitioning keeps one file per hour with data even after compaction (a floor of up to 24 files a day),
where daily compacts a day to one file. Production tables stay `day()` (the expected outcome). Trino peaked at
1,803 MiB in that run, under the 2 GiB `mem_limit` set earlier in this loop; the heap stays 1 GB.

**DAGs stay paused.** Each DAG now also sets `is_paused_upon_creation=True` (beside `DAGS_ARE_PAUSED_AT_CREATION`).
`airflow dags test` neither creates nor unpauses DAG rows (it only adds runs), so earlier `dag-test` runs leave the
rows paused: the scheduler smoke test lists all three with `is_paused` true, healthy, no import errors, scheduler
382 MiB. The iteration-1 failure was the check reading `paused` instead of Airflow 2.11's `is_paused`.

**Tests:** `tests/test_batch.py` 22 (new: the touch happens once, after `RUNNING`, with a second touch only when a
confirmed touch leaves the slot pinned, and an empty `vans` refused; a timeout fails through
`AirflowFailException` with a `failed` line naming the stuck LSN and target, and Connect stopped). Host suite 98
pass, 3 skipped; `test-airflow` 6 pass.

## 2026-10-12 · M4a · compaction adds a `replace` snapshot, stale dag_runs failed by `dag-test`, 1.5 GB memory gate
**Snapshot counts after maintenance (ruling, no code change).** Loop G's check required `snapshots_after <=
snapshots_before`, and failed in its iteration 2. The maintenance code was right; the rule was not:
- `optimize` that rewrites files commits exactly one new snapshot (operation `replace`) and removes none.
- `expire_snapshots(retention_threshold => '1d')` removes only snapshots older than a day, never the current one,
  which after a compaction is that `replace`. Trino 483 has no `retain_last`, and the 1-day floor
  (`iceberg.expire-snapshots.min-retention=1d`) stays.
- Evidence: iteration 1 compacted bronze 6 → 4 files with snapshots 12 → 10 and silver 23 → 18, because day-old
  snapshots were waiting to expire; iteration 2, 6.5 h later, compacted the same way but went 12 → 13 and 22 → 23.
- So with every snapshot younger than a day a compacting run ends at before + 1. The order optimize → expire →
  remove orphans stays. The new check allows exactly the run's own `replace` commit (nothing else may write the
  table during the run), requires a compaction to show one, and requires that some run in
  `lake.metrics.maintenance_runs` really expired snapshots.

**Stale `running` dag_runs.** A killed `airflow dags test` left two `cdc_slot_keepalive` runs `running` in airflow-db
(2026-10-10 06:54Z and 13:16Z). Their task instances have no job, so the scheduler's zombie detection never ends
them, and with `max_active_runs=1` the scheduler would never start another run once the DAG is unpaused.
- **Did:** `python -m dockwatch.batch.runstate fail-stale <dag_id>` marks every `running` / `queued` dag_run of the
  DAG `failed` (`DagRun.set_state`) with its unfinished task instances (running, queued, scheduled, up_for_retry,
  up_for_reschedule, deferred, restarting or no state), printing `FAILED_STALE_RUN <dag_id> <run_id>`. `dag-test`
  runs it before `airflow dags test`, only when no `airflow-scheduler` container runs (a running dag_run may be live
  then).
- **Measured:** the keepalive `dag-test` printed both stale run_ids; afterwards no dag_run of the three DAGs is
  `running` or `queued`.
- **Test:** `tests/airflow/test_dags.py::test_dag_test_fails_stale_running_runs` (SQLite): the running run and its
  task instance become `failed`; a `success` run and another DAG's running run are untouched. `test-airflow` 7 pass.

**1.5 GB memory gate.** Loop G's Builder once started `test-airflow` at 1,179 MiB free. Now ≥ 1.5 GB host free is
required before `test-airflow`, `batch-up` (Trino) and the keepalive (Connect + Airflow); the checks stop the
`console` container if needed and do not run the step while still below. This run: 4.3 GB free before
`test-airflow`, 4.4 GB before `batch-up`, 4.0 GB before the keepalive.

**This run's numbers (2026-10-12 06:41 UTC, `dag-test iceberg_maintenance`, 27 s, ~10 min after the baseline run):**
- Files: bronze 6 → 5, `silver.station_status` 11 → 5, `availability_5m` 6 → 5; one file per day partition (5
  days). Episodes and the 5 `ops.*` tables unchanged.
- Snapshots: bronze 3 → 4, silver 5 → 6, `availability_5m` 5 → 6 (each the `replace`), episodes 2 → 2, `ops.*` 8 → 8.
  The baseline run at 06:30 UTC, with day-old snapshots to expire, compacted bronze 7 → 5, silver 17 → 5,
  `availability_5m` 7 → 5 files and expired bronze 15 → 2 and silver 27 → 3 snapshots.
- Trino peak 1,463 MiB (baseline: 1,691 MiB in maintenance, 1,773 MiB in partition evolution; 2 GiB limit),
  `airflow-cli` 296 MiB. `batch-stop` → `catchup` → `verify-lake` → `check_g.py after`: ok.
- Keepalive (`catch_up_above_mb=16` after a 77.8 MiB probe): retained 81,579,280 B → 56 B with two touches, Connect up
  45.4 s, `dag-test` 61 s.

## 2026-10-12 · M4b · spike stopped: the `raw` Hive catalog runs Trino out of Metaspace on the first INSERT

Loop I's plan starts M4b with a spike of decision 1: load each trip month's CSV through a `raw` Hive catalog (file
metastore on SeaweedFS) and `INSERT … SELECT` it into `lake.bronze.trips`. The spike failed, so M4b stopped there,
as the plan says. Nothing in the running stack changed.

**What worked:**
- **Bucket listing:** one page, 106 keys. The month keys are found by their `YYYYMM-` prefix (`202609-lyftbikes-…`,
  `202503-…tripdata.zip`, `201901-fordgobike-…csv.zip`). 2020-04 is absent.
- **Staging:** the ZIP's one CSV member was streamed into `.csv.gz` and counted with `csv`. 2019-01 (legacy, 14 header
  columns with `bike_share_for_all_trip`) has 192,082 records and stages as 6.3 MB of gz in 2.1 s. 2026-09 (ride,
  quoted, CRLF) has 607,780 records and stages as 22.8 MB in 6.2 s. Both went to `raw/trips/month=YYYY-MM/`.
- **The Hive file metastore on SeaweedFS** (`hive.metastore.catalog.dir=s3://dockwatch/hive-metastore/`):
  `CREATE SCHEMA raw.trips_staging` and the external CSV tables (`skip_header_line_count=1`) worked.
  - `count(*)` = 192,082 and 607,780, equal to Python's counts.
  - Every start and end time parsed as `timestamp(6)`, and no `\r` was left in the last column (`member_casual` is
    exactly `member` / `casual`).
  - `with_timezone()` moves a spring-forward gap time forward (02:30 → 03:30 PDT) instead of failing.

**What failed:**
- The first `INSERT INTO lake.bronze.trips SELECT … FROM raw.trips_staging.trips_2026_09` killed Trino twice:
  `Terminating due to java.lang.OutOfMemoryError: Metaspace`, exit 3, not a container OOM.
- Container memory peaked at only 1.24 GiB of 2 GiB. `jvm.config` caps Metaspace at `-XX:MaxMetaspaceSize=192M`.
- Metaspace measured with `jcmd 1 VM.metaspace`:

  | Trino state | Metaspace used |
  |---|---|
  | idle, `lake` only | 131 MB |
  | after one `lake` query | 147 MB |
  | idle with `raw` | 143 MB |
  | after DDL on both catalogs | 169 MB |
  | during the INSERT | 143 → 174 → 192 MB, then the OOM |

  Two connector plugins in one query (the Hive reader plus the Iceberg writer) and the generated code do not fit
  under 192 MB.
- The plan freezes `jvm.config` and says to stop on an OOM. The fallback (a local metastore dir) would not help,
  because the metastore was not the problem. So the engine choice goes back to the Architect.

**Left in place:** `src/dockwatch/batch/trips.py` (the listing, key, status, download-cache, stage and SQL builders)
and `marts.py` (the SQL literal helper). Nothing imports them yet.
- `infra/trino/etc/catalog/raw.properties` was removed again. With it, every Trino run, `iceberg_maintenance`
  included, would start 12 MB closer to the Metaspace cap.
- The spike's tables were dropped (`lake.bronze.trips`, `lake.metrics.trip_loads` and `raw.trips_staging`). The two
  cached ZIPs stay in `data/trips/zips/`, and the two gz files stay under `raw/trips/` in SeaweedFS.

## 2026-10-12 · M4b · trips through pyiceberg work; Trino Metaspace runs out in the marts after M4a maintenance

Loop J re-planned M4b after Loop I's stop: the trip load moves out of Trino into pyiceberg, and Trino stays
Iceberg-only. Iteration 1 built the whole M4b path and ran it in the Checker's order. It stopped on a Trino
`OutOfMemoryError: Metaspace` in the `marts_daily` dbt build, which the plan says needs the user.

**Why pyiceberg (the five options):**
- **A raw Hive catalog** + `INSERT … SELECT`: Metaspace OOM in Loop I, twice.
- **Raise `MaxMetaspaceSize`:** `jvm.config` is frozen, and Trino already peaks near its 2 GiB limit in maintenance.
- **`add_files`:** in the Trino 483 Iceberg plugin jar, `IcebergMetadata` refuses unless
  `iceberg.add-files-procedure.enabled=true` (a frozen `lake.properties` change), and `MigrationUtils.addFiles`
  refuses partitioned tables; `bronze.trips` is `identity(trip_month)`.
- **Spark from Airflow:** excluded by M4a decision 1 (Spark and Trino never run together).
- **Batched `INSERT … VALUES`:** about 100 commits per month and large generated plans (Metaspace again).
- **pyiceberg 0.11.1** with pyarrow 22.0.0 is already in `dockwatch-airflow:2.11.2` (pinned by Airflow's
  constraints), so there is no new dependency. Python streams the CSV, Trino's JVM does no write work for trips, and
  each month is one Iceberg commit. The cost is a third writer next to Spark and Trino, for `bronze.trips` and
  `bronze.station_information` only.

**The load (`trips.load_month`, task `load` of `trip_history_monthly`):**
- The ZIP's one CSV member is read as a stream (`csv.reader`, `utf-8-sig`), and `map_row` maps every record. `local_time`
  reads the text as America/Los_Angeles with `fold=0`, so an ambiguous 01:30 is the first (PDT) one and the
  non-existent 02:30 lands on 10:30Z. A time that does not parse counts as a bad timestamp.
- **The transaction fallback is taken:** one `tbl.transaction()` deletes the month and appends 100k-row Arrow tables
  (25k Python rows at a time). It is still one catalog commit. With the whole month in one table (`overwrite`),
  `airflow-cli` peaked at 1,021 MiB of its 1 GiB during the backfill. Batched, it peaks at 727 MiB, and a single
  2026-09 load reaches 415 MiB RSS (from 569 MiB).
- Bad timestamps raise before the commit. `loaded_rows` is Trino's `count(*)` of the month after the commit. One
  `trip_loads` row (ok / error / missing) goes through Trino.
- **SeaweedFS needed one setting:** pyarrow's AWS SDK uploads `aws-chunked` with a trailing CRC64 checksum, and
  SeaweedFS stores the chunk framing inside the object (`730\r\nObj…`), so Trino read "Not an Avro data file".
  `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` / `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required` in the
  `x-airflow` environment fix it. The first spike table's broken files are orphans under
  `warehouse/bronze/trips-c13d9cb6…/`. Loop I's `hive-metastore/` and `raw/trips/` gz objects are still in SeaweedFS,
  but nothing reads them.

**Measured:**
- **Spike:** 2019-01 (legacy) 192,082 rows in 5.1 s; 2026-09 (ride) 607,780 rows in 11.2 s, and the same after a
  rerun. 0 bad timestamps, `bike_id` / `ride_id` filled per schema.
- **`dag-test trip_history_monthly month=2019-01`:** 23 s, `check_i trips 2019-01 legacy` ok.
- **12-month backfill** (`dags backfill -s 2025-10-01 -e 2026-09-01`): 5,789,432 rows. It took 3 min 33 s and,
  with the final loader, 3 min 18 s. Each month took 5–10 s. `check_i backfill` ok.
  - Airflow's backfill runner logged two `DeadlockDetected` errors on `dag_run` and carried on.
- **Rerun 2026-09:** `check_i rerun` ok (7 data files for the month).
- **dbt:** dbt-core 1.10.23 + dbt-trino 1.10.6 in `/opt/dbt-venv` (58 exact pins). `dbt debug` connects, and so
  does `dbt parse`. On a Trino without M4a maintenance before it, `dbt build` passes (3 marts, 28 tests: 645 / 3,860
  / 15,459 rows). The weekday trip peaks are hours 17, 18, 16 and 8 (Pacific), so the local time-zone reading holds.
- **Trino Metaspace** (`jstat -gc 1`, used / committed MiB, cap 192):

  | Step | Spike Trino | Checker-order Trino |
  |---|---|---|
  | fresh | 113.5 / 115.9 | 114.5 / 116.8 |
  | after DDL / `iceberg_maintenance` | 141.8 / 144.9 | 175.4 / 179.1 |
  | after `partition_evolution_demo` | – | 183.3 / 187.8 |
  | after the trip loads (2 months / legacy + backfill + rerun) | 162.0 / 165.5 | 185.6 / 191.6 |
  | after one mart CTAS | 173.0 / 177.3 | – |
  | after `dbt build` (31 nodes) | 178.3 / 183.2 (+1.5 per rebuild) | **OOM**, exit 3 |

  A full GC (`jcmd 1 GC.run`) frees 0.06 MiB: the generated classes stay referenced. The trips path itself adds
  almost nothing once Trino has seen the tables. The dbt build's new SQL is what crosses 192 MiB after maintenance.
- **Container peaks:** Trino 1,946 MiB of 2 GiB during the backfill (after maintenance), so above the 1.85 GiB
  record line. `airflow-cli` 757 MiB in a 2026-09 `dag-test`.

**Other changes:**
- `dag-test` refuses while another `airflow-cli` container runs. That is the concurrent-run guard: a `dag-test`
  fails stale running runs, which would include a live backfill.
- `ensure_image()` rebuilds when the hash of the Dockerfile + `dbt-requirements.txt` differs from the image label
  `dockwatch.build-hash`.
- The station dimension (`marts_daily.stations`) is written with one pyiceberg `overwrite` from the newest GBFS
  `station_information` snapshot: 645 stations.
- The quality gate (`data/dbt/quality_gate.json`) checks dbt results, mart rows and freshness. Great Expectations
  waits for M5. The insights export writes `web/data/insights/*.json` atomically, with the manifest last.

**`accept-alerts-stable` (Loop I's stop):** this was environmental. The trace shows `css/base.css` failing with
`net::ERR_NO_BUFFER_SPACE` (Windows `WSAENOBUFS`, socket exhaustion on the host). No web or test change was made.

**Open:**
- **Metaspace (user decision):** raise `-XX:MaxMetaspaceSize`, or restart Trino between M4a maintenance and M4b.
- `accept-i-guard` cannot pass as written. `DAG_IDS` with five names is 122 characters, so it needs
  `# noqa: E501  # fmt: skip` to keep `ruff` / `ruff format` green. `check_i.py`'s regex wants the line to end in `}`.

## 2026-10-13 · M4b · done: Metaspace raised to 256M, the full batch run passes on one Trino

M4b is done. The 12-month backfill runs green, the marts build and pass their tests, and `marts_daily` writes
`web/data/insights/*.json`. Two changes made the full run pass: the user raised Trino's Metaspace cap, and the
insights export now rounds its floats.

**Iteration 1's OOM, both runs:**
- **Builder's run:** Trino died with `Terminating due to java.lang.OutOfMemoryError: Metaspace` (exit 3,
  `OOMKilled=false`) in the `marts_daily` dbt build. Before that it had run `iceberg_maintenance`,
  `partition_evolution_demo` and all the trip loads in one JVM.
- **Checker's run:** the same error one step earlier, in the backfill's 2026-08 load. That JVM had run maintenance,
  evolution, 2019-01 and 10 backfill months.
- In both runs Metaspace hit the 192 MiB cap. The container was not OOM-killed. Measured with `jstat -gc 1`:
  - fresh: 114.5 MiB;
  - after maintenance: 175.4 MiB;
  - after evolution: 183.3 MiB;
  - the trip loads: about +2 MiB;
  - one full dbt build on a fresh JVM: about +65 MiB;
  - each further build: about +1.5 MiB, which a full GC does not free.
- The Trino container peaked at 1,993 of its 2,048 MiB.

**User decision: `-XX:MaxMetaspaceSize=192M` → `256M`.** This is one line in `infra/trino/etc/jvm.config`, and it is
the only Trino change. `mem_limit: 2g`, the heap (1G), the code cache and the catalog files are unchanged.
- **The risk the user accepted:** Metaspace is native memory, and it counts toward the 2 GiB container limit. So
  64 MiB more Metaspace can turn a Metaspace OOM into a container OOM kill.
- Decision 1 (pyiceberg writes trips, Trino stays Iceberg-only) stands. The raise is headroom for M4a + M4b in one
  JVM, not a reason to bring back the Hive `raw` catalog.

**Check fix (not a loosening): `DAG_IDS`.** The five-id `DAG_IDS` line in `tests/airflow/test_dags.py` is 122
characters, so it needs `# noqa: E501  # fmt: skip` for `ruff` and `ruff format`.
- `check_i.py guard`'s regex now allows an optional trailing comment after whitespace.
- It still parses the set and requires exactly the five ids, and the "only this line changed" rule is unchanged.
- The same guard now pins `jvm.config` to the 0fbb659 file, with only the Metaspace line changed, to exactly 256M.

**The full run (Checker order, one Trino JVM, no restart between steps):**

| Step | Metaspace used / committed MiB (cap 256) | Trino container peak (docker stats MiB) |
|---|---|---|
| `batch-up` (fresh) | 141.4 / 144.5 | – |
| `iceberg_maintenance` | 180.3 / 184.4 | 1,876 after the step |
| `partition_evolution_demo` | 186.8 / 191.5 | 1,918 |
| trips 2019-01 (legacy) | 187.3 / 192.1 | 1,799 |
| 12-month backfill | 188.1 / 192.9 | 1,860 |
| rerun 2026-09 | 188.5 / 193.4 | 1,876 |
| `marts_daily` (3 builds, see below) | 192.4 / 197.6 | 2,013 |

- `check_i metaspace` after the marts reported `max_mib` 256.0, 197.6 MiB committed and 58.4 MiB headroom. That is
  below the 224 MiB line.
- The run committed more than 192 MiB from the backfill on, so the old cap would have failed there again.
- The container came close to its limit:
  - The `marts_daily` dag-test recorded 1,979 MiB for Trino, and the 4-second sampler saw 2,013 MiB. Both are above
    the 1,894 MiB (1.85 GiB) record line.
  - The cgroup (v1) `max_usage_in_bytes` reached the full 2,048 MiB with `failcnt` > 0, but that number includes
    page cache, which the kernel reclaims. RSS peaked at 1,838 MiB.
  - `oom_kill` stayed 0 and `OOMKilled=false`.
- **Peak hours.** The weekday trip peak hours (Pacific) are 17, 18 and 8. That confirms the time zone assumption:
  trip times are local.

**Trips:**
- **2019-01 (legacy):** 192,082 rows in 6.1 s.
- **Backfill:** `dags backfill -s 2025-10-01 -e 2026-09-01` loaded **5,789,432 rows**.
  - Load time was 131 s, and the wall time 257 s.
  - Each month loaded in 7–15 s.
  - `airflow-cli` peaked at 725 MiB.
- **Rerun 2026-09:** 607,780 rows again, with `airflow-cli` at 591 MiB.
- **`trip_loads`:** these rows match the live counts.
- **`bronze.trips`:** 64 files, 230 MB, 5,981,514 rows (13 months).
- **Station codes:** 99.3 % of the trips with a code match a current station (5,057,855 of 5,092,655).

**Marts and insights:**
- **First `marts_daily`:** `dbt_build` and the quality gate passed, but `export` failed:
  `trips_vs_availability.json would be 2313972 bytes > 2000000`.
  - `mart_trips_vs_availability` has 15,461 rows (645 stations × 24 hours).
  - Every double went out at full precision, for example `0.34794520547945207`.
- **Fix:** `marts.plain` rounds floats and decimals to 3 digits (`FLOAT_DIGITS`). The marts themselves keep full
  precision.
  - With 4 digits the file was 1,917,862 bytes, and it would reach 2,001,342 bytes once every share has 4 digits.
  - With 3 digits it is 1,886,673 bytes, and at most about 1.94 MB.
  - The size check is unchanged.
- **dbt:** the build takes 7.3 s, with 3 models and 28 tests. Mart rows: 3,860 / 645 / 15,461.
- **Insights files:**
  - `station_availability_daily.json` 654,995 B;
  - `rebalancing_need.json` 79,846 B;
  - `trips_vs_availability.json` 1,886,673 B (over 1 MB; noted for F4);
  - `manifest.json` 698 B.
- **The daily file will outgrow the budget.** It holds the newest 30 service dates and now covers 6 of them (about
  107 KB per day), so at 30 dates it would be about 3.2 MB and `export` would fail. That is a Known gap in
  CURRENT_STATE. Fixing it means changing the file's shape (for example, the station names in one place), and that
  is a design decision for F4.
- The three `marts_daily` builds in this run each added about 1–3 MiB of Metaspace (191.1 → 191.7 → 192.4 used).

**Kept from iteration 1:**
- The SeaweedFS checksum env fix (`AWS_REQUEST_CHECKSUM_CALCULATION=when_required`).
- The transaction fallback for the month load.
- The concurrent-run refusal in `dag-test`.
- **Leftovers:** the orphan files of the first broken `bronze.trips`, plus Loop I's `hive-metastore/` and
  `raw/trips/` objects, are still in SeaweedFS. Nothing reads them.

## 2026-10-15 · Loop K · insights format 2, the Insights page, 16 maintenance tables, `dbt compile` in CI

Loop K builds everything that needs no user action, in value order, over up to five iterations. Iteration 1 (this
entry) does the time bomb and the small items; iteration 2 is planned for pipeline health, the Pipeline page and the
launch tests.

**Plan decisions (D1–D9) and what was cut:**
- **D1 insights format 2:** each insights file gets a `stations` lookup, a `window` header and compact rows, so every
  file stays far under its 2,000,000-byte budget at 30 dates and 1,000 stations. Built (below).
- **D2 the F4 Insights page** on format 2, with a Playwright spec and real fixtures. Built (below). The period
  control (7 d / 30 d / 12 mo) is skipped: the marts fix the windows (DESIGN_BACKLOG #52).
- **D3 `dbt compile` in CI** through a `tasks.py` task, offline. Built (below).
- **D4 `iceberg_maintenance` on the M4b / metrics tables** (9 → 16), with a spike first and a fallback for
  `bronze.trips`. Built (below); the fallback was needed.
- **D5–D8, planned for iteration 2:** DAG `pipeline_health` → `lake.metrics.pipeline_minutely` (per minute: events
  by `kafka_ts`, processed by `processed_ts`, p50 / p95 of `processed_ts - fetched_ts`, consumer lag = records
  published but not yet in silver) and `web/data/health.json`; Trino restarted (`batch-stop` + `batch-up`) before it
  instead of any limit change; the F4b page `pipeline.html`; F5 launch tests (screenshots at 6 widths × 2 themes,
  keyboard-only order, reduced motion). **D9:** `prepare()` routes the new data files to real fixtures.
- **Cut from Loop K (not blocked on the user):** Great Expectations suites (a new runtime inside the gate of the
  carried `marts_daily` checks), OpenLineage + Marquez (Marquez on demand only, refused beside Trino / Spark /
  Connect; five new integration surfaces), and Lighthouse (a new npm devDependency; it belongs with the deployed
  site). They wait for the next loop.

**Insights format 2 (`marts.insights_v2`, `marts_export`):**
- **Why:** a station × hour row repeated a 36-character UUID, the station name, the region and three window values.
  At ~107 KB per date the daily file would have passed 2,000,000 bytes at about 18 dates, and `export` would have
  failed (Loop J's Known gap).
- **Shape:** `{format: 2, generated_at, source, window, stations: {columns: [station_id, name, region_id], rows},
  columns: [station, ...], rows}`. The row's `station` is an index into `stations.rows`.
  - The per-file constants (`window_first_day` / `window_last_day`, `trip_days`, `trip_first_month` /
    `trip_last_month`) move to `window`. A window column that is not constant fails the export rather than losing
    data.
  - `first_window` / `last_window` are dropped.
  - `manifest.json` gains `"format": 2`. `export_insights` keeps its exact behaviour for `(columns, rows)` tables
    (`tests/test_marts.py` unchanged) and merges an optional third element into the document.
- **The daily file's dates are an index too.** With ISO dates in the rows, the synthetic 1,000 stations × 30 dates
  (every float at full 3 digits) came to 1,981,370 B, 1 % under the budget. With `service_date` as an index into
  `window.service_dates` it is 1,671,692 B. That is the plan's "date index" shrink.
- **Real export** (`dag-test marts_daily`, 645 stations, 9 service dates): 973,010 / 79,694 / 1,886,746 B before,
  **272,808 / 66,768 / 514,417 B** after (daily / rebalancing / trips). Projected at 1,000 stations and 30 dates:
  daily ~1.26 MB, trips ~0.80 MB.
- **Tests** (`tests/test_insights.py`, through the real `marts_export` task with Trino faked): the columns move out of
  the rows, every file decodes back to the mart rows, and the 1,000-station, 30-date marts fit the budget while the
  old layout does not.

**The Insights page (`insights.html`, `js/insights.js`, `js/insights/model.js`, `js/charts.js`, `css/charts.css`):**
- **KPI tiles:** Time empty and Time full (Σ minutes ÷ Σ coverage minutes over the daily file: 7.4 % and 2.2 % in
  the fixture), Worst hour (1–2 PM, 9.9 % of the time empty on average) and Trips per day (13,856).
- **Heatmap:** % of time empty, the 25 stations empty most often × 24 Pacific hours. It is a table with the
  rounded % as text in each cell and a labelled `seq-empty` scale, and it scrolls inside a focusable region.
  - **Contrast:** axe failed white text on `seq-empty-4` (#d55e00 gives 3.9:1, not the 4.9:1 I had estimated).
    That step now takes dark text in both themes (4.8:1 with ink in light, 4.9:1 with on-ink in dark).
- **Rebalancing table:** need ranks 1–20 of the last 7 days.
- **Trips vs availability scatter:** one point per station, `role="img"` with a summary. The takeaway compares the
  busiest and the quietest quarter of stations (9.9 % vs 7.1 % of the time empty).
- **States:** skeletons with `aria-busy`; a 404 shows the "not published yet" card with a link to Live; any other
  failure, or a read with no full answer after 10 s, shows a message with Retry. `readJson` now attaches the HTTP
  status to its error.
- **Tests:** `tests/web/insights.spec.js` has 8 tests. It recomputes every number from the real fixture (a copy of
  this run's format-2 export in `tests/web/fixtures/insights/`), and checks the not-published, error and
  never-answers states and axe in both themes. `prepare()` in `helpers.js` serves those fixtures by default
  (additive edit).
- **Carried specs:** the touch-target scan still finds exactly one "See live station status" link on Insights, so
  the page header keeps that link (hidden only while the not-published card shows its own). The shell axe test now
  covers the page with data and took 35 s.

**`dbt compile` in CI:**
- **Task:** `python tasks.py dbt-compile` runs `uv run --no-project --python 3.12 --with-requirements
  infra/airflow/dbt-requirements.txt dbt compile --no-populate-cache --target local`, with
  `DBT_SEND_ANONYMOUS_USAGE_STATS=false`.
- **Result:** 7 models and 28 tests compile with no Trino, in ~8 s with uv's cache.
- **Plan-time fact:** without `--no-populate-cache`, dbt connects to the (unresolvable) host `trino` to list
  schemas.
- **Path fix:** dbt resolves a relative `--target-path` against the project dir. The first run wrote
  `dbt/dockwatch/data/...`, so the task passes an absolute `data/dbt-compile/target` (the log path stays relative to
  the working dir).
- **ci.yml:** one new step after `uv run pytest -q` in the `Python lint + tests` job, named "dbt compile (offline, no
  Trino)". No new job.

**`iceberg_maintenance` on 16 tables (`maintenance.BATCH_TABLES`, `ALL_TABLES`):**
- **Tables added:** `bronze.trips`, `bronze.station_information`, `metrics.trip_loads`, the three marts and
  `metrics.pipeline_minutely`. `maintenance.TABLES` (the 9) is unchanged.
- **Missing tables:** a batch table that does not exist yet gets a `skipped` row (probe: `information_schema.tables`)
  and does not fail `record`. A missing core table still fails as before. `metrics.maintenance_runs` is left out,
  because the run writes it after measuring.
- **Spike 1** (fresh Trino, whole-table `optimize`):
  - success in 48 s;
  - `bronze.trips` 64 → 13 files, 230 → 179 MB, 485 → 1 snapshots, 16.2 s;
  - `metrics.trip_loads` 90 → 1 files (2.9 s); the marts and `bronze.station_information` lost their old snapshots
    (16 → 2, 9 → 1);
  - `pipeline_minutely` `skipped`.
  - **Trino peaked at 2,024 MiB** of its 2 GiB limit, over the plan's 1,950 MiB line, so the fallback applies.
- **Spike 2** (restarted Trino, nothing left to compact): 1,273 MiB. The peak came from rewriting all 13 months at
  once.
- **Fallback:** identity-partitioned tables (`maintenance.IDENTITY_PARTITIONS = {"bronze.trips": "trip_month"}`)
  optimize **one partition per run**, the one with the most files (`$partitions`, `file_count > 1`), with
  `... EXECUTE optimize(...) WHERE trip_month = DATE '...'`. When every partition has one file there is no optimize
  at all.
  - It is one partition per *run*, not one statement per partition. Each optimize is a commit, and the carried
    per-table rule allows at most one `replace` commit per table per run. So a 12-month reload is compacted over
    the next 13 daily runs.
- **Verified:**
  - `dag-test trip_history_monthly month=2026-09` left that month with 7 files.
  - Then a fresh `batch-up` and `dag-test iceberg_maintenance`: only `trip_month = 2026-09` was optimized (7 → 1,
    19 → 13 files overall), success in 36 s, **Trino 1,726 MiB**.
  - `check_h maintenance` and `check_k maintenance` pass.
- **Metaspace:** after that maintenance run and `marts_daily` in the same JVM, Trino's Metaspace was 186.7 / 191.9
  MiB used / committed of 256 (64.1 MiB headroom). After spike 1 alone it was 178.2 / 181.6. The full carried
  sequence (with the 12-month backfill) was not re-run this iteration. Trino was left stopped (`batch-stop`).

**Tests:**
- **Host:** 122 pass, 5 skipped. New: `tests/test_insights.py` 3 and `tests/test_maintenance_batch.py` 4; the new
  Airflow module skips on the host.
- **Airflow image:** 13 pass, with `tests/airflow/test_loopk_dags.py`: the maintenance DAG maps over the 16 tables.
- **Playwright:** 57 tests in total.

## 2026-10-15 · Loop K · pipeline health, the Pipeline page and the launch tests (Phase 2, same iteration)

Phase 1 passed the whole `checks.json` first (all 167 ran; the only non-zero exits were `accept-replay-real`, a
data gap, and the Phase 2 `accept-k-*` checks). Then Phase 2 was built in the same iteration.

**`pipeline_health` (`src/dockwatch/batch/health.py`, `airflow/dags/pipeline_health.py`):**
- **DAG:** `guard` → `minutely` → `export`, `@hourly`, `catchup=False`, paused at creation, the shared
  `default_args`, an `sla` on `export`. `guard.TRINO_TASKS` gains `"pipeline_health"`, so it is refused beside a
  Spark JVM or Kafka Connect.
- **The window:** the 1,440 minutes before the run's start minute.
- **Four GROUP BYs on `lake.silver.station_status`:**
  - events by minute of `kafka_ts`;
  - processed by minute of `processed_ts`, with `approx_percentile` p50 / p95 of `processed_ts - fetched_ts`;
  - two lag counts over the rows processed since the window start, one by minute of `kafka_ts` and one by minute
    of `greatest(kafka_ts, processed_ts)`.
- **Consumer lag:** the difference of the two running counts, i.e. records published before a minute's end and
  not yet in silver. It is exact, with no assumption that the broker's clock and Spark's agree. (A plain
  "events minus processed" running sum would drift with clock skew.)
- **The write:** the series is assembled in Python and written to `lake.metrics.pipeline_minutely` by pyiceberg in
  one transaction (delete minutes ≥ the window start, append). There is no big generated `VALUES` through Trino.
  It is counted back through Trino (1,440 rows).
- **`export`:** writes `web/data/health.json` (format 1). It holds the status (stream / quality / maintenance and
  their worst), the tiles of the last active hour, the minute series, every existing maintenance table's newest
  non-`replace` commit with files and partitions, the quality gate with the dbt tests, and the newest maintenance
  run's files before and after. The file must stay ≤ 500,000 bytes and is written atomically.
- **Real runs** (fresh Trino, as `accept-k-health` does with `batch-stop` + `batch-up`):
  - success in 24.8 s and 19.9 s, `health.json` 78,862 B;
  - Trino peaked at 1,086 / 1,374 MiB and `airflow-cli` at 406 / 351 MiB;
  - Metaspace afterwards was 168.6 / 172.8 MiB used / committed;
  - `check_k health` passed both times. Its independent recomputation of the sampled minutes on silver matched,
    and the rerun left no duplicate minute.
- **What the data says:** Spark runs on demand, so only 5 of the 1,440 minutes processed anything. A catchup lands a
  whole backlog at once, so p95 is the backlog's age: 3,299 s in the last active hour, and ~51 h for the first
  catchup after the producer was down. Freshness was 662 s, so the stream is `warn` and the overall status is
  Degraded. Both are true for an on-demand stream.
- **Status details** are plain English ("newest row reached silver 11 min ago", "0 of 16 tables failed in the
  newest run, finished 24 min ago") instead of ISO timestamps: an ISO string was the widest thing on the 320 px
  page.
- **Trino before `pipeline_health`:** in the full sequence Trino reached 1.8–2.0 GiB of 2 GiB, so it is restarted
  first, not given more memory. `metric-j-trino-metaspace` after the full carried sequence (16-table maintenance,
  evolution, trips 2019-01, the 12-month backfill, rerun, marts) was 187.9 / 194.3 MiB: under the 224 MiB line,
  and lower than Loop J's 197.6.

**The Pipeline page (`pipeline.html`, `js/pipeline.js`, `charts.js` `lineChart`, `charts.css`):**
- **Status banner:** a badge with the Lucide `circle-check` / `triangle-alert` / `circle-x` icon and the word
  Healthy / Degraded / Failing in the status colour, then the three component lines.
- **Stat tiles:** p95 latency, events per minute, consumer lag and freshness.
- **Charts:** latency (p50 solid, p95 dashed, the 5 min SLA line with its label, a legend) and events per minute.
  A value with no neighbour is drawn as a dot; otherwise a single catchup minute would be invisible.
- **Tables:** freshness per table with status badges, the quality gate with every dbt test, and the compaction
  bars (files before / after, with the numbers as text).
- **States** as on Insights. There is no lineage section; it arrives with OpenLineage / Marquez.
- **Overflow at 320 px:** long table names in the compaction list widened the card (344 px page). They now wrap
  (`overflow-wrap: anywhere`).
- **Tests:** `tests/web/pipeline.spec.js` has 9 tests on a real `health.json` fixture (`tests/web/fixtures/health.json`,
  a copy of the second run's file). `prepare()` serves it by default (additive edit).

**Launch tests (`tests/web/launch.spec.js`, F5 without Lighthouse):**
- **Screenshots:** 48 full-page screenshots (live / insights / pipeline / about × 320, 390, 768, 1024, 1440, 2560
  px × light / dark) saved with `testInfo.outputPath`. Each page is checked for no horizontal scroll; there are no
  pixel diffs.
- **Keyboard:** Tab order equals the DOM reading order on every page (one stop per radio group, every visible
  focusable). The nav overlay traps Tab and returns focus to its button on Escape.
- **Reduced motion:** no animation runs after load or after a 60 s refresh.
  - The first version caught a hover `background-color` transition mid-frame. Reduced motion makes every transition
    0.01 ms (base.css), so the test now counts only animations longer than 1 ms.
  - A scratch run without reduced motion shows the filter still catches the freshness pulse, so it is not vacuous.
- **Timing:** the whole spec takes ~14 s.

**Tests:**
- **Host:** 127 pass, 5 skipped. `tests/test_health.py` has 5 tests: the series, the lag (with rows carried into the
  window and a broker clock ahead of Spark's), the tiles, the status thresholds, and the document with its budget.
- **Airflow image:** 15 pass.
- **Playwright:** 69 tests in total. `insights`, `pipeline` and `launch` repeated 3× gave 60 of 60.

**Memory note:** in the Phase 1 full run, the trips checks ran this time; at baseline their 1.5 GB memgate had blocked
them. After the backfill, the `marts_daily` memgate read 1,527 MiB, just under 1,536, so my runner stopped there. I
resumed it from `accept-i-marts`, and every later check passed. The gate is borderline on this host once the trips
checks run.

## 2026-10-16 · Loop L · Great Expectations in the `marts_daily` quality gate, GE results and readable latency on the Pipeline page

**Goal:** the M5 Great Expectations item, run inside `marts_daily`'s existing quality gate, its results in
`quality_gate.json`, `health.json` and on the Pipeline page, plus the Loop K follow-ups on the same page and docs:
duration ticks on the latency chart with labels that never overlap the axis title, a duration under the latency tile,
a fresh Trino JVM before `marts_daily`, the stale CURRENT_STATE lines, the `helpers.js` import nit and a heatmap
coverage proposal. OpenLineage / Marquez stay for Loops M and N.

**Re-base of the carried guards:** Loop K is committed (481c2e2), so `check_k.py` was re-based onto 47c9164..481c2e2
(scope, its guard's test-file / `src/dockwatch/batch` / DAG rules, and its IMPLEMENTATION_PLAN rules on the committed
file); `check_l.py` takes over the working tree since 481c2e2 with Loop-L-specific rules. Ids, commands and kinds of
every carried check are unchanged; 14 new checks were added after their named neighbours (181 in total).

**D1, the GE venv:**
- Great Expectations **1.24.0** (the newest 1.x on PyPI) lives in its own venv `/opt/gx-venv` in the Airflow image,
  like the dbt venv. `infra/airflow/gx-requirements.txt` is the full `pip freeze` of a venv resolved inside the image
  from `great_expectations[trino]==1.24.0`: 41 packages, including SQLAlchemy 2.1.4, trino 0.340.0, pandas 3.0.6,
  numpy 2.5.4 and scipy 1.18.1.
- Airflow's own environment is untouched (SQLAlchemy 1.4.54, pandas 2.1.4, numpy 1.26.4); its python cannot import GE.
  `GX_ANALYTICS_ENABLED=False` in the image's `ENV`: no usage analytics and no network at run time.
- The Dockerfile gained one block after the dbt block (every old line kept); `host.BUILD_INPUTS` is now the
  Dockerfile, `dbt-requirements.txt` and `gx-requirements.txt`, so `ensure_image()` rebuilds when the pins change.
  The image grew from 3.28 to 3.86 GB (the venv is 432 MB on disk). The rebuild took 40 s with Docker's cache.

**The spike on real data, and a GE gap on Trino:**
- SQLAlchemy 2.1 with the trino dialect worked; the SQLAlchemy 2.0 fallback was not needed.
- The first run on Trino passed only the row count: every column expectation failed with "The column ... in
  BatchData does not exist", in 0.6 s. Cause: with `create_temp_table=False` the batch is a plain `TextClause`, which
  has no `selected_columns`, so GE falls back to its Trino-specific reflection. That fallback takes the table after
  the query's last `FROM` (`lake.silver.station_status`) and looks it up in `information_schema` under the
  connection's default schema (none), so it finds nothing; even with a default schema it could never see the derived
  columns `bikes_plus_docks` and `age_s`. This does not depend on the SQLAlchemy version (the branch is chosen by
  dialect name).
- Fix, inside the runner only: `gx_run.reflect_trino_queries()` replaces GE's module-level
  `column_reflection_fallback` for the Trino dialect and a `TextClause` with `SELECT * FROM (<query>) LIMIT 0`
  (planned, not scanned) and returns its column names. Every other dialect and selectable goes to GE's own function.
  It is marked to re-check on every GE upgrade. A view or a temp table would have solved it too, but the plan allows
  no table in the lake.
- After the fix: 10 expectations evaluated on 382,143 rows in 4.1 s, the GE process at **225 MiB** max RSS
  (`ru_maxrss` of the subprocess), and no GE table in the lake.

**D2-D4, suite, runner and gate:**
- `src/dockwatch/batch/expectations.py` (pure, stdlib only) holds the suite **`silver_station_status`**, ten
  expectations on `silver.station_status`: row count ≥ 1; the 17 columns (`exact_match` false, so additive evolution
  passes); `station_id` and `snapshot_ts` not null; `state` in the six states; bikes and docks available ≥ 0;
  `capacity` ≥ `bikes_plus_docks` (or equal; capacity includes disabled bikes and docks, which is the tolerance);
  **uniqueness** of (`station_id`, `snapshot_ts`); and **freshness** (minimum `age_s` ≤ 300), the only non-blocking
  one, a **warning**.
- The batch is the newest **24 hours** of silver: from silver's `max(snapshot_ts)` − 24 h (exclusive) to it
  (inclusive), as a query asset that adds `bikes_plus_docks` and `age_s = date_diff('second', processed_ts, now)` with
  `now` fixed as a literal at the run's start.
- GE returns results in its own order; they are matched by (type, column key). A suite entry GE did not report never
  passes. The uniqueness count is the rows in duplicated groups, as GE counts them; a min reported as text becomes a
  number. Status: `ok` if success, else `fail` if **blocking**, else `warn`.
- `src/dockwatch/batch/gx_run.py`, run as `/opt/gx-venv/bin/python -m dockwatch.batch.gx_run trino`: an ephemeral
  context, an SQL datasource on `trino://dockwatch@trino:8080/lake` with `create_temp_table=False`, the query asset,
  the suite, a ValidationDefinition and the Checkpoint `marts_daily_quality` (result format SUMMARY), then
  `data/quality/expectations.json` (format 1) written atomically. It never imports `dockwatch.config`. A `frame` mode
  runs the same suite on small pandas frames for the Airflow-image test. GE's progress bars are off in the task log.
- `dag_tasks.marts_quality_gate` keeps the DAG's five tasks: it builds the dbt gate as before, reads silver's newest
  `snapshot_ts`, runs the runner (timeout 900 s), merges the results and writes `quality_gate.json`. The merge adds
  `expectations` (suite, evaluated, passed, warnings, failed, path), one `gx:` problem per failed blocking
  expectation, and `passed` = dbt part passed and no blocking failure. A missing or unusable results document (not
  format 1, another run's, an incomplete suite) adds `gx: no results (...)` and fails the gate. A crashed runner
  writes that gate and then raises, so Airflow retries; a failed blocking expectation calls `fail()` (no retry), like
  a failed dbt test. The dbt keys keep their meaning.

**D5, health.json:** `dag_tasks.health_export` copies `data/quality/expectations.json` unchanged into
`quality.expectations` (null when missing). `health.py` and the format are unchanged; the `quality` status still
follows `quality_gate.json` `passed`, which now includes GE.

**Real runs** (memgates passed; the console was stopped by the memgate as usual):
- `batch-up` (fresh JVM), then `dag-test marts_daily`: success in 44.5 s. Peaks: **`airflow-cli` 541 MiB** (the same
  peak as Loop K: dbt's step is the larger one; the GE subprocess adds ~225 MiB after dbt has exited, and
  `docker stats` samples every ~3 s), **Trino 1,717 MiB** (under the 1,950 MiB flag). Metaspace afterwards **179.3 /
  184.1 MiB** used / committed. The GE run took 2.7 s.
- Results on real silver: 382,143 rows (2026-10-15 12:22:16 to 2026-10-16 12:22:16 UTC); all 9 blocking
  expectations passed with 0 unexpected rows; freshness warned at 1,922 s (Spark runs on demand and never beside
  Trino, so it always warns during `marts_daily`; that is honest and matches the stream status). `quality_gate.json`
  passed: 28 of 28 dbt tests, expectations 10 / 9 / 1 / 0. `check_l expectations` recomputed every result
  independently on Trino and matched, and found no GE table in the lake.
- `batch-stop` + `batch-up`, then `dag-test pipeline_health`: success in 26.8 s, Trino 1,099 MiB, `airflow-cli`
  430 MiB, `health.json` 82,773 B of 500,000. That file is the new Playwright fixture `tests/web/fixtures/health.json`
  (its p95 tile is now 75,860.6 s, 21 h 4 min: a catchup landed a long backlog).

**D6, the Pipeline page:**
- **Expectations card** "Great Expectations on silver" after the data-quality card: a takeaway ("9 of 10 checks
  passed; freshness is a warning: Spark runs on demand"; a failed blocking check says the insights were not
  refreshed), a table (Check, Column, Result badge Passed / Warning / Failed, Found = unexpected rows, the row count
  or the newest row's age as a duration) and a caption with the GE version, suite, rows and when `marts_daily`
  checked. With no results: a `card-soft` "No Great Expectations results yet." The dbt card is unchanged.
- **Latency axis:** `charts.durationTicks()` gives round ticks in one unit from the axis maximum (seconds below 2 min,
  minutes below 2 h, else hours: `30 s`, `20 min`, `12 h`), the y title is "Latency", and `lineChart` now measures the
  widest tick label (`getComputedTextLength`) and the title's line box (`getBBox`) to set the left margin, placing the
  rotated title from its measured ascent. Two things had to change for the measurement to be real: each chart card is
  marked ready before it is drawn (a hidden svg measures 0), and `drawOnResize` redraws once `document.fonts.ready`
  resolves (the labels are measured in the web font). The events chart uses the same code with its number ticks.
  The first overlap test failed at 320 px: the rotated title's box started 2.5 px left of the svg, because a text's
  box is not centred on its baseline; placing it at `2 - bbox.y` fixed it.
- **Latency tile:** the value stays `75,860.6 s` (pinned by `pipeline.spec.js`); the sub-line starts with the p95 as
  a **duration**: "21 h 4 min · fetch to silver, worst minute of the last active hour". The takeaway uses durations
  too.
- `tests/web/helpers.js`: the two `node:path` imports merged into one; nothing else.
- `tests/web/pipeline-quality.spec.js` (5 tests): the card lists every result with its badge, column, found value,
  takeaway and caption, and a failed variant; the empty state for `quality.expectations` null and missing; the latency
  y labels match `^\d+(\.\d+)? (s|min|h)$` in one unit and each label's box starts right of the title's box at 320,
  390, 768 and 1440 px, for the fixture, a p95 scaled past 100,000 s and one under 120 s; the tile's sub-line; axe on
  the card with and without results in light and dark.

**D7, a fresh Trino JVM before `marts_daily`:** the new check `accept-l-trino-restart` (`batch-stop && batch-up &&
check_g batch`) sits right before `metric-i-memgate-marts`, so `marts_daily` no longer follows the trips stage on a
JVM that had reached 2,024 of 2,048 MiB. No limit or JVM change; CURRENT_STATE records it as an operating rule.
`metric-j-trino-metaspace` now measures the marts on a fresh JVM (a metric only).

**D8 and the follow-ups:** CURRENT_STATE's stale lines are gone ("Great Expectations is not used yet", the "without
GE results" wording, Loop K's Phase-1 Metaspace numbers, replaced by this run's), and every "third writer" sentence
lists `metrics.pipeline_minutely`. DESIGN_BACKLOG gained #61 (the expectations card) and #62, a proposed heatmap
minimum coverage (at least 12 of 24 hours) for the user to decide: `insights.spec.js` pins today's ranking. #58 notes
that units and overlap are fixed while the scale is still open.

**Tests:** host 133 pass, 6 skipped (`tests/test_expectations.py`, 6, on a real GE result captured from the spike in
`tests/fixtures/gx/raw_results.json`); Airflow image 18 pass (`tests/airflow/test_loopl_gx.py`, 3: the venv has exactly
the pinned set and Airflow does not see GE; the suite in the GE venv passes a clean frame and flags a duplicate, a
negative count, an unknown state, an over-capacity row and an old batch each in exactly its expectation;
`marts_quality_gate` runs the checkpoint and merges it, fails without retry on a blocking failure and retries on a
crash); Playwright 74 in total.

**Full check run (all 181 checks, after the build):** no regression; every carried check that passed at baseline
passed, and all 14 Loop L checks passed. `dag-test marts_daily` on the restarted JVM: 42.7 s, **`airflow-cli` peak
634 MiB** (this time the 3 s sampler caught the GE step; under the 900 MiB stop line), Trino 1,694 MiB, Metaspace
179.8 / 184.7 MiB; GE 3.1 s on 381,554 rows, the same 9 passed, freshness warned at 2,564 s. `pipeline_health`
afterwards: 25.5 s, Trino 1,074 MiB, `health.json` 82,725 B. The memgate before the trips stage once read 687 MiB
free (the Docker VM held ~3 GB of page cache after the CI-container and Spark checks); after ~2 idle minutes WSL
released the cache (2,264 MiB available) and the run was resumed from that memgate. Nothing was stopped or
loosened; it is the same borderline host the Loop K entry describes.

**Cut, and why:** OpenLineage / Marquez and the Spark listener (Loops M and N, so F4b stays unticked: no lineage
screenshot); the heatmap coverage rule itself (a design call, and `insights.spec.js` is frozen); a log or capped
latency axis (#58, a design call); METRICS.md and Lighthouse (Loops O and P); any `mem_limit` or JVM change.
