# DockWatch: current state

> **What this file is:** a description of DockWatch *exactly as it is right now* and what is being worked on.
> It is rewritten whenever the code changes. If something is removed from the code, it is removed from here too.
> For the full history, including what was tried and removed, see [`DEVLOG.md`](DEVLOG.md).
> For the plan, see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).

**Last updated:** 2026-10-07 (evening) · **Milestones:** M0 ✅ (except manual AWS steps) · M1 ✅ · M2 ✅ except
partition evolution (moved to M4) and the live map (F2)

---

## Working on now

| What | Why | Status |
|---|---|---|
| **Verify the catalog fix** (Iceberg REST catalog now on Postgres instead of SQLite) | The SQLite catalog failed concurrent commits (`SQLITE_BUSY`) and made the streaming job restart 22 times. | Fix applied and tables re-registered, **not yet verified**: the Docker Desktop engine stopped responding (low memory) right after the restart. Needs a Docker Desktop restart, then `python tasks.py inspect`. |

Next after that: **F1** (design tokens + site shell) and **F2** (live station map from `live.json`), then **M3**.

### Running right now on this machine
| Process | How it was started | Stop with |
|---|---|---|
| GBFS producer | `uv run python -m dockwatch.producer run` (log: `data/producer-run.log`) | Ctrl+C / kill the process |
| Exporter | `uv run python -m dockwatch.exporter` (log: `data/exporter.log`) | Ctrl+C / kill the process |
| Spark streaming job | `python tasks.py stream` (container `status-stream`, restarts automatically) | `python tasks.py stream-stop` |
| Stream stack | `python tasks.py up stream` | `python tasks.py down` |

⚠ As of the last check, **Docker Desktop's engine is not responding**, so the containers' real state is unknown. The
producer and exporter (host processes) keep running but can't reach Kafka until Docker is back.

Memory: the Spark job uses about 1.5–1.9 GB; the rest of the stack under 1 GB. The machine had ~1–4 GB free while
these ran; a background process was once stopped by Claude Code for low memory (see DEVLOG).

---

## How data flows today

```
Bay Wheels GBFS 2.3 ──(producer, every 60 s)──► Kafka gbfs.station_status (6 partitions, key station_id)
        │                                     ├─► Kafka gbfs.station_information (compacted, only on change)
        │                                     └─► Kafka gbfs.dlq (invalid rows)
        └─► raw archive  s3://dockwatch/raw/gbfs/<feed>/dt=…/HH/<last_updated>.json.gz

gbfs.station_status ──(Spark status_stream, 60 s micro-batches)──►
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
| `DESIGN.md` | DockWatch design system **v1.0** (frozen when F1 starts). |
| `CONCEPTS.md` | Kafka, stream processing and Iceberg explained through this codebase. |
| `reference/DESIGN_SOURCE_transitpulse.md` | The TransitPulse design file that was here before; reference only. |
| `CURRENT_STATE.md` / `DEVLOG.md` | This file / append-only history. |

### Python package `src/dockwatch/`
Host code runs on Python 3.12 (uv). Code under `streaming/` also runs in the Spark image (Python 3.10), so it avoids 3.11+ features.

| Module | What it does | Why it's built this way |
|---|---|---|
| `config.py` | One `Settings` object from env / `.env` (prefix `DOCKWATCH_`): target, GBFS, Kafka topics, S3, Iceberg, checkpoints, trigger, watermark, state thresholds, exporter. | Switching to AWS is a config change. |
| `gbfs/models.py`, `gbfs/client.py` | Typed GBFS records (unknown fields kept); discovery → GBFS 2.3; re-discover on 404; custom User-Agent. | Feeds move and grow. |
| `producer/` | `messages.py` (explode per station, DLQ rejects, order-insensitive content hash), `archive.py` (gzip JSON to S3 or a folder), `kafka.py` (topic specs, idempotent publisher), `poller.py` (ttl-paced poll loop), `__main__.py` (`run`, `once`, `setup`, `replay`). | Keyed by station for per-station ordering; idempotent to avoid retry duplicates. |
| `streaming/episodes.py` | **Plain Python:** `Thresholds`, `classify()` (station state rule), `EpisodeState` + `advance()` (episode/alert state machine). | Unit-tested without Spark; the Spark rule is tested to give the same answers. |
| `streaming/transforms.py` | Spark: `parse_kafka()`, `with_state()`, `to_silver()`, `availability_windows()`. | Pure DataFrame functions, tested on tiny inputs. |
| `streaming/lake.py` | Spark/Iceberg session config (`local` = REST catalog + SeaweedFS, `aws` = Glue), table DDL, `migrate()` for schema evolution (`EVOLUTIONS` list). | Tables evolve in place; fresh and old installs share one history. |
| `streaming/status_stream.py` | The streaming job (four queries, see flow above). | One app keeps memory low; separate checkpoints recover independently. |
| `streaming/inspect_tables.py`, `streaming/sql.py` | Health/consistency report (counts, freshness, duplicates, Kafka-offset gaps, states, episodes, file sizes); one-off SQL. | Proof for the restart test and day-to-day checks. |
| `exporter/build.py`, `exporter/__main__.py` | Builds `live.json` (641 stations with name, code, lat/lon, region, map view, bikes/e-bikes/docks, state, state since, last reported; counts per view) and `alerts.json` (open alerts longest first, last 20 resolved). Atomic writes. | The static site only reads these files. Stations without a `region_id` are placed in a view by coordinates. |
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
| `status-stream` | spark | 4040 (Spark UI) | The M2 streaming job; image `dockwatch-spark:3.5.5` (`infra/spark/Dockerfile`: Spark 3.5.5, Java 17, Iceberg 1.6.1 + AWS bundle, Kafka connector, pandas, pyarrow, pytest); `src/` mounted read-only; checkpoints in volume `dockwatch_checkpoints`; 2 GB memory limit. |

### Infrastructure (`infra/terraform/`), validated, **not applied**
S3 bucket (private, encrypted, lifecycle), Glue databases `dockwatch_{bronze,silver,ops,marts,metrics}`, Athena workgroup
(1 GB scan limit), SNS topic, $1 / $5 budgets, IAM roles (spark-writer, athena-reader, alert-lambda, github-ci via OIDC).
Remote state backend commented out until the AWS account exists.

### Tooling and tests
- `tasks.py` (`Makefile` forwards): `lint`, `fmt`, `test`, `test-integration`, `test-spark`, `up [profile]`, `down`, `ps`,
  `setup`, `produce`, `produce-once`, `replay`, `stream`, `stream-logs`, `stream-stop`, `inspect`, `sql "<query>"`,
  `export`, `tf-fmt`, `tf-validate`.
- **Host tests:** 35 pass (`python tasks.py test`): GBFS client, messages/archive, poller, episodes/state rule, exporter,
  alerts. **Spark tests:** 4 pass in the Spark image (`python tasks.py test-spark`); skipped on the host.
- `inspect` and `sql` run Spark with whole-stage codegen **off** (a JVM crash otherwise; see DEVLOG).
- Lint: ruff, line length 120.

---

## Known gaps and facts to remember
- **Nothing is committed to git yet.**
- An unused Docker volume `dockwatch_iceberg-catalog` (old SQLite catalog) still exists; backup copy at
  `data/iceberg_catalog_backup.db`. Safe to delete once the Postgres catalog is verified.
- **No website yet** (F1/F2), so M2's "live map" check is still open.
- **Partition evolution** not done (moved to M4 with compaction).
- **Small files:** each streaming query writes ≥ 1 file per minute (silver ≈ 45 KB/file). Compaction arrives in M4.
- `live.json` is ~200 KB uncompressed; fine for now, worth trimming or gzip-serving in F2.
- **AWS steps are manual** and not done; `checkpoint_root` on AWS will need `s3a://` + hadoop-aws jars (not in the image yet).
- Some stations report a `last_reported` weeks old → `offline`. 35 stations have no `region_id` → placed by coordinates.
- `station_information` rows arrive in a different order on every poll (the content hash sorts them).
- Bay Wheels currently reports 0 scooters at every station; the evolved scooter columns are 0 for new rows, null for old.
