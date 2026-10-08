# DockWatch: a real-time bike-share lakehouse (project plan)

> **Type:** brand-new data engineering project (new repo, e.g. `github.com/braaaeeedyn/dockwatch`)
> **Data:** Bay Wheels, the Bay Area's public bike-share system (operated by Lyft). DockWatch is an independent
> project on its public data, not affiliated with Lyft or Bay Wheels.
> **One line:** stream live Bay Wheels station status through Kafka, process it in real time into Apache Iceberg
> tables on S3, join it with years of trip history and a CDC-replicated operational database, and publish
> trustworthy, tested, lineage-tracked marts and alerts.
> **Effort:** ~8–10 weeks part-time, after TransitPulse. **Cost target:** ~$0 (local Docker + AWS free plan credits,
> with budget alerts on day one).

It complements TransitPulse instead of repeating it: same region and domain (Bay Area mobility), but **streaming,
lakehouse, AWS and Airflow** where TransitPulse is **batch, warehouse, GCP and Dagster**.

---

## 0. Purpose and usage

**The problem it addresses.** A dock-based bike-share system fails its riders in two ways: an **empty** station (no
bike to take) and a **full** station (nowhere to return one). Operators fight this by *rebalancing*, driving vans to
move bikes, and that only works if they know, minutes ahead, which stations are empty or about to fill up. The public
feed only gives a snapshot of right now; it doesn't keep history, flag problems, or say how often a station fails.

**What DockWatch is for:** turning that snapshot feed into reliable, continuously updated data that answers:
- *Right now:* which stations are empty or full, and for how long?
- *Operations:* where is rebalancing needed most, and did yesterday's rebalancing fix it?
- *Planning:* which stations fail riders most often, at what times, and does it track trip demand?

**Who would use it, and how:**
| User | How they use it |
|---|---|
| Operations / rebalancing team | Watch the live map and empty/full alerts to dispatch vans; review the daily rebalancing-need mart. |
| Planners / analysts | Query the Iceberg tables (Athena / SQL) for station reliability over months, and compare availability with trip history. |
| Riders (public demo) | See live availability and which stations are usually empty at a given time. |
| Engineers maintaining it | Use the health page (latency, lag, freshness, data-quality results) and the lineage graph to trust and debug the pipeline. |

**What it is for you:** the portfolio project that proves streaming and lakehouse data engineering, with a realistic
operational use case behind every design choice.

## 1. What it does

### What a visitor sees
| Feature | Description |
|---|---|
| **Live system map** | Every Bay Wheels station coloured by bikes / docks available, refreshed from the stream (not from the public API directly). |
| **Empty / full alerts** | A station that stays empty or full for more than N minutes raises an alert (a live feed on the page, plus an SNS email for the demo). |
| **Pipeline health page** | End-to-end latency (event time → queryable in Iceberg), events per minute, consumer lag, data-quality check results and data freshness, all from the pipeline's own metrics. |
| **Analytics marts** | Daily station availability, rebalancing need (hours empty / full), and trips vs. availability, queryable with Athena. |
| **Lineage graph** | A screenshot / link of the OpenLineage (Marquez) graph from raw topic to mart. |

### What runs behind the scenes
1. A small **producer** polls the GBFS feeds (`station_status` ≈ every 60 s, `station_information` daily) and writes
   each snapshot to **Kafka** (Redpanda in Docker). The poll interval respects each feed's `ttl`.
2. An operational **Postgres** database (stations, docks, maintenance tickets, rebalancing jobs) is driven by a
   simulator, and **Debezium** streams its changes (CDC) into Kafka.
3. **Spark Structured Streaming** reads both topics, de-duplicates, handles late and out-of-order snapshots with
   watermarks, computes windowed availability and empty/full episodes, and writes **Iceberg** tables on **S3**.
4. **Airflow** runs the batch side: compaction and snapshot expiry for Iceberg, the monthly trip-history backfill,
   dbt models, Great Expectations checks, and the daily marts.
5. **Athena** (or Trino in Docker) queries the Iceberg tables; a small static site shows the map and health page.
6. **Terraform** defines the AWS side; **GitHub Actions** lints, tests and deploys.

---

## 2. Gaps this project closes (beyond TransitPulse)

| Gap | How it is covered |
|---|---|
| **AWS** | S3, IAM (least-privilege roles), Glue Data Catalog, Athena, Lambda + SNS for alerts, all in Terraform |
| **Airflow** | DAGs for compaction, backfills, dbt, data-quality gates, with retries, SLAs and backfill by date |
| **Kafka + stream processing** | Topics, partitions, consumer groups, offsets; Structured Streaming with event-time windows, watermarks, late data, checkpoints, exactly-once sinks |
| **Open table format** | Apache Iceberg: partition evolution, schema evolution, `MERGE INTO`, time travel, compaction, snapshot expiry |
| **Change data capture** | Debezium on Postgres logical replication; upserts and deletes applied to Iceberg with `MERGE` |
| **Data quality + observability** | Great Expectations checkpoints inside Airflow, freshness SLAs, OpenLineage → Marquez lineage |
| **Operational DB design** | Normalised Postgres schema, indexes, `EXPLAIN ANALYZE` on the simulator's hot queries |
| **Streaming + batch together** | A lambda-style split: streaming for minutes-fresh state, batch for history and heavy rebuilds |

**Still not covered on purpose:** governance/PII (public data only), Kubernetes, managed Kafka (MSK / Confluent Cloud).
Snowflake and Databricks are optional add-ons (section 7).

---

## 3. Stack

### ✅ Used
| Area | Tool | Notes |
|---|---|---|
| Message bus | **Redpanda** (Kafka API) in Docker | Single binary, low memory, Kafka-compatible clients. Swap to Apache Kafka later if you want the name. |
| CDC | **Debezium** (Kafka Connect) + **Postgres 16** | Logical replication (`wal_level=logical`), one connector per table group. |
| Stream processing | **Spark 3.5 Structured Streaming** (PySpark) | Local / Docker. Iceberg Spark runtime + AWS bundle jars. |
| Table format | **Apache Iceberg** | Catalog: **AWS Glue Data Catalog** (simplest with Athena). |
| Storage | **Amazon S3** | One bucket, prefixes `raw/`, `warehouse/`, `checkpoints/`. Lifecycle rules on raw. |
| Query | **Amazon Athena** (engine v3, reads Iceberg) | Pay per TB scanned; partitioning keeps scans tiny. **Trino** in Docker as the free fallback. |
| Orchestration | **Apache Airflow 2.x** (Docker Compose) | Runs locally; a VM is optional. |
| Transform | **dbt Core** + `dbt-athena` adapter | Same habits as TransitPulse, on a different engine. |
| Data quality | **Great Expectations** | Checkpoints as Airflow tasks; a failure blocks the downstream mart. |
| Lineage | **OpenLineage** (Airflow + Spark integrations) → **Marquez** (Docker) | |
| Alerts | **AWS Lambda** + **SNS** | Triggered by the alerts topic via a tiny consumer, or by Athena query results. |
| IaC / CI | **Terraform**, **GitHub Actions** (OIDC to AWS, no stored keys) | |
| Tests | **pytest** (transform unit tests with a local SparkSession), **ruff** | |

### ❌ Not used, and why
| Not used | Why |
|---|---|
| Managed Kafka (MSK, Confluent Cloud) | Cost; the skills are the same against Redpanda. |
| Flink | One stream engine is enough; Spark Structured Streaming reuses the Spark skills from TransitPulse. Flink is the stretch goal if a target company lists it. |
| Kubernetes | Docker Compose covers it at this size. |
| Snowflake / Databricks in the core path | Not needed for the data volume (see section 7); added as optional phases. |
| Dagster | Already learned in SeismicSoCal/TransitPulse; this project is where Airflow is learned. |

---

## 4. Data sources (public)

> Check each source's licence/terms before starting and note attribution in the README.

| Source | Use |
|---|---|
| **Bay Wheels GBFS feeds** (auto-discovery `gbfs.json`, linked from Lyft's Bay Wheels "System Data" page and the MobilityData GBFS systems catalog) | `station_status` (live counts) and `station_information` (locations, capacity). |
| **Bay Wheels monthly trip history** (CSV/ZIP files on the System Data page) | Several years of trips for the batch backfill and "trips vs. availability". |
| **Simulated operations DB** (your own) | Maintenance tickets, rebalancing jobs, dock outages, generated by a simulator that reacts to the live feed (e.g. opens a rebalancing job when a station is empty for 20 min). This is the CDC source. |

Data volume: station status is small per snapshot (hundreds of stations) but continuous, about **0.5–1 M rows/day**
once exploded per station. Trip history is tens of millions of rows. Both are enough to exercise partitioning,
compaction and backfills without needing a cluster.

---

## 5. Build plan

### Phase 0: setup (week 0)
- [ ] Repo `dockwatch`, Python 3.12, `uv`, ruff, pytest, `docker-compose.yml` for Redpanda, Postgres, Debezium (Kafka Connect), Airflow, Marquez, Trino.
- [ ] AWS account: **budget alerts at $1 and $5**, MFA on root, an admin IAM user for you only.
- [ ] Terraform: S3 bucket (+ lifecycle), Glue database, IAM roles (Spark writer, Athena reader, Lambda), SNS topic. State in S3 with a lock.

### Phase 1: ingest (week 1) · Kafka
- [ ] GBFS producer: auto-discover feed URLs, poll respecting `ttl`, key messages by `station_id`, include `last_reported` (event time) and fetch time.
- [ ] Topic design: `gbfs.station_status` (6 partitions, keyed by station), `gbfs.station_information` (compacted topic).
- [ ] Idempotent producer settings; a dead-letter topic for malformed payloads; a raw S3 archive of every snapshot (gzip JSON) so anything can be replayed.
- [ ] Be able to explain: partitions and ordering, consumer groups, offsets and rebalancing, log compaction, at-least-once vs. exactly-once.

### Phase 2: stream processing into Iceberg (weeks 2–3)
- [ ] Structured Streaming job: parse, de-duplicate on (`station_id`, `last_reported`), **watermark** on event time, write `bronze.station_status` (append) and `silver.station_status` (cleaned).
- [ ] Windowed aggregates: 5-minute availability per station; **empty/full episodes** with stateful processing (start, end, duration).
- [ ] Iceberg: partition by `days(event_ts)`, sort by `station_id`; show **partition evolution** (start daily, move to hourly) and **schema evolution** (add a field when GBFS adds one).
- [ ] Checkpoints in S3; kill and restart the job mid-stream to prove no duplicates and no gaps.
- [ ] Alerts topic: episodes longer than N minutes → Lambda → SNS email.

### Phase 3: CDC from the operational database (week 4)
- [ ] Postgres schema: `stations`, `docks`, `maintenance_tickets`, `rebalancing_jobs` (normalised, foreign keys, indexes); the simulator writes to it.
- [ ] Debezium connector → `ops.*` topics; Spark applies inserts, updates and **deletes** to Iceberg with `MERGE INTO`.
- [ ] `EXPLAIN ANALYZE` the simulator's two hottest queries before and after adding an index; record the plans in the README.
- [ ] Handle a schema change in Postgres (add a column) end to end.

### Phase 4: batch, history and orchestration (weeks 5–6) · Airflow
- [ ] DAG `trip_history_monthly`: download a month of trips, validate, load to `bronze.trips`, **backfillable** by date (`airflow dags backfill`).
- [ ] DAG `iceberg_maintenance` (daily): `rewrite_data_files` (compaction), `expire_snapshots`, `remove_orphan_files`; log file counts and sizes before/after.
- [ ] DAG `marts_daily`: dbt (`dbt-athena`) builds `mart_station_availability_daily`, `mart_rebalancing_need`, `mart_trips_vs_availability`; Great Expectations checkpoint gates it.
- [ ] SLAs and alerting on DAG failure; retries with exponential backoff.

### Phase 5: quality, lineage, observability (week 7)
- [ ] Great Expectations suites: schema, ranges (`num_bikes_available ≥ 0`, `≤ capacity`), uniqueness, freshness (silver table updated < 5 min ago).
- [ ] OpenLineage from Airflow and Spark → Marquez; screenshot the graph from Kafka topic to mart.
- [ ] Pipeline metrics table: events/min, end-to-end latency (event → committed to Iceberg), consumer lag, files per partition.

### Phase 6: ship and measure (week 8)
- [ ] Static site (map + health page) reading small JSON exports produced by Athena queries.
- [ ] GitHub Actions: ruff, pytest (Spark transforms on tiny fixtures), dbt compile, `terraform plan`; deploy on `main`.
- [ ] Results for the README:
  - p50 / p95 end-to-end latency
  - sustained throughput (replay a day of archived snapshots at 10× and 50× speed)
  - small-files problem: query time and file count before vs. after compaction
  - cost: a full trip-history backfill vs. an incremental month (Athena bytes scanned)
  - recovery: restart after a crash, with no duplicates (count check)

### Optional phases (choose at most one; see section 7)
- [ ] **7a. Snowflake reads the same Iceberg tables** (30-day trial).
- [ ] **7b. Databricks Free Edition** runs the historical backfill and a Delta Lake comparison.

---

## 6. Skills you will be able to claim

**Streaming:** Kafka (topics, partitions, consumer groups, compaction), Spark Structured Streaming (event time,
watermarks, stateful processing, exactly-once sinks), CDC with Debezium.
**Lakehouse:** Apache Iceberg (partition/schema evolution, MERGE, time travel, compaction), Glue catalog, Athena / Trino.
**Orchestration and quality:** Airflow (DAGs, backfills, SLAs), dbt on Athena, Great Expectations, OpenLineage.
**Cloud and ops:** AWS (S3, IAM, Glue, Athena, Lambda, SNS), Terraform, GitHub Actions with OIDC, Docker Compose.
**Databases:** normalised Postgres design, indexing, query plans.

Example résumé bullet (fill in real numbers):
> Built DockWatch, a real-time bike-share lakehouse: Kafka + Spark Structured Streaming ingest live GBFS station status
> and Debezium CDC from Postgres into Apache Iceberg on S3 (p95 **N s** event-to-query latency, **N** events/min,
> exactly-once on restart); Airflow orchestrates compaction, backfills, dbt and Great Expectations gates, with
> OpenLineage lineage, all provisioned with Terraform.

---

## 7. Snowflake and Databricks: where they would fit

Neither is needed for this data volume, which is the honest reason they're not in the core path. They are worth
adding only to show you can use them, and only where they'd plausibly be used in a real company:

| Option | What you'd do | Why a company would do this |
|---|---|---|
| **Snowflake on the Iceberg tables** | Register the Glue catalog in Snowflake (catalog integration) and query the same Iceberg tables; build one dbt mart in Snowflake and compare it with Athena. | Analysts and BI tools live in Snowflake, while engineering keeps data in an open format on S3. Iceberg lets both read the same files with no copy. |
| **Databricks Free Edition for the backfill** | Run the multi-year trip-history backfill and the heavy aggregations as a Databricks job, write Delta Lake, and compare Delta with Iceberg (MERGE, time travel, compaction). | Large batch rebuilds and ML feature pipelines are where a Spark platform pays off; the streaming path stays as is. |

Check the current limits before starting: Snowflake's trial is time- and credit-limited, and Databricks Free Edition
has restricted compute and may not reach an external S3 bucket (upload the history files to it if so).

---

## 8. Cost guardrails
- AWS budget alerts at $1 and $5; delete idle resources; S3 lifecycle rule moves `raw/` to cheaper storage after 30 days.
- Athena: always filter on the partition column; set a per-query bytes-scanned limit on the workgroup.
- Everything heavy (Kafka, Spark, Airflow, Postgres, Marquez) runs locally in Docker, not on paid AWS compute.
- Compaction matters for cost: thousands of tiny files make every Athena query slower and pricier.

## 9. Adding it to the portfolio
New page `/projects/dockwatch/` (`dockwatch(ctx)` in `pages.py`, a sky/teal hero). Sections: overview with
latency/throughput stats; architecture (flow + an Iceberg diagram); streaming correctness (watermarks, restart test);
lakehouse maintenance (before/after compaction chart); data quality and lineage (Marquez screenshot); costs. Add proofs
to the **Data Engineer** role card (Kafka, Iceberg, Airflow, AWS) and a `feats` card on the home page.
