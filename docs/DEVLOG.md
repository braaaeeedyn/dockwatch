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
