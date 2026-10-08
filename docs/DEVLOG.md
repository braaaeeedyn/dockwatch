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
