# DockWatch concepts

Short explanations of the streaming and lakehouse ideas DockWatch uses, each tied to the exact place in this repo
where it shows up. Written as each feature is built (M1: Kafka, M2: stream processing and Iceberg). Later milestones add sections.

---

## Kafka (M1)

DockWatch uses **Redpanda**, which speaks the Kafka protocol, so everything below is standard Kafka.

### Topics and partitions
A **topic** is a named, append-only log. It is split into **partitions**, and each partition is an ordered log of
its own stored on a broker. Partitions are how Kafka scales: different consumers can read different partitions at once.

- `gbfs.station_status` has **6 partitions** (`producer/kafka.py`, `topic_specs`). About 641 messages arrive per
  minute, which one partition could handle; 6 gives room for 6 parallel Spark tasks in M2 and for faster replays in M6.
- In the first ~4 hours, the 6 partitions held 312 / 282 / 315 / 303 / 333 / 378 messages after the first few
  minutes: roughly even, because there are 641 different keys.

### Keys and ordering
Kafka only guarantees order **within one partition**. A message's **key** decides its partition
(`hash(key) % partitions`), so every message with the same key lands in the same partition, in the order it was sent.

- Every `station_status` message is keyed by **`station_id`** (`producer/messages.py`). So all snapshots of one station
  are in one partition, in time order. That is exactly what M2 needs: empty/full episodes are computed per station,
  and they need that station's events in order.
- There is no order *across* stations, and DockWatch doesn't need any.
- Changing the partition count later would move keys to different partitions and break this ordering for a while,
  so the count is chosen up front.

### Consumer groups and rebalancing
Consumers that share a **group id** split a topic's partitions between them: each partition is read by exactly one
member of the group. If a member joins, leaves or crashes, the group **rebalances** and the partitions are handed out
again. Different groups read the same topic independently, each with its own position.

- In M2 the Spark streaming job and the exporter will read `gbfs.station_status` as **separate** consumers, so each
  sees every message. Spark Structured Streaming doesn't use a consumer group to track its position; it stores
  offsets in its own checkpoint (see below), which is what makes its restarts exact.

### Offsets
An **offset** is a message's position in its partition (0, 1, 2, …). A consumer's progress is "the next offset to read"
per partition. Where that number is saved, and *when*, decides what happens after a crash:
- saved **before** processing → a crash loses messages (at-most-once);
- saved **after** processing → a crash re-processes some messages (at-least-once).

The partition "high watermarks" above are the latest offsets; after about 4 h 15 min, `gbfs.station_status` held
161,532 messages in total (≈ 252 snapshots × 641 stations).

### Log compaction
A normal topic deletes old messages by **time or size** (`gbfs.station_status`: 7 days). A **compacted** topic
instead keeps **at least the latest message for each key** and eventually removes older ones with the same key.

- `gbfs.station_information` is compacted (`cleanup.policy=compact`): it acts like a table of "current metadata per
  station" that never expires. A new consumer reading it from the start gets every station's latest name, location
  and capacity, without needing the 7-day history.
- Because the feed reshuffles its rows on every poll, the producer hashes the stations **sorted by id** and only
  re-sends when something actually changed (`content_hash`). Otherwise the compacted topic would get 641 identical
  messages a minute.

### Delivery guarantees
- **At-most-once:** may lose messages, never duplicates.
- **At-least-once:** never loses messages, may duplicate them (retries after a lost acknowledgement).
- **Exactly-once:** each message affects the result once. In practice this is *at-least-once delivery plus
  de-duplication or transactions* somewhere.

What DockWatch does at each hop:
1. **Producer → Kafka:** the idempotent producer (`enable.idempotence=true`, `acks=all`) gives each producer a
   sequence number per partition, so the broker drops duplicates caused by retries. `acks=all` means a write only
   counts once it is stored. Result: no loss and no retry duplicates *within one producer session*.
2. **Across producer restarts:** a restarted producer could fetch and send the same feed snapshot again. That's why
   every message carries `last_reported` and `snapshot_id`: M2 de-duplicates on (`station_id`, `last_reported`).
3. **Kafka → Iceberg (M2):** Spark checkpoints its Kafka offsets and Iceberg commits atomically, so after a crash the
   job redoes the unfinished batch and the commit either happens once or not at all. M2's kill-and-restart test proves this.

### Dead-letter topic
Rows that fail validation (for example a negative bike count or a missing `station_id`) go to `gbfs.dlq` with the
error and the original payload, and the rest of the snapshot still flows. In the first ~4 hours of live data there were
**0** dead-lettered rows; the path is covered by unit tests.

---

## Stream processing (M2)

The streaming job is `streaming/status_stream.py`: one Spark app running four **queries**, each reading
`gbfs.station_status` and writing one output, each with its own checkpoint.

| Query | Output | Kind |
|---|---|---|
| `bronze_status` | `lake.bronze.station_status` | stateless append |
| `silver_status` | `lake.silver.station_status` | de-duplication (stateful) |
| `availability_5m` | `lake.silver.availability_5m` | windowed aggregation (stateful) |
| `episodes` | `lake.silver.station_episodes` + `gbfs.alerts` + `dockwatch.station_state` | arbitrary state per key |

### Micro-batches and triggers
Structured Streaming runs a query as a series of small batch jobs. With `trigger(processingTime="60 seconds")`,
every minute it reads the Kafka offsets that arrived since the last batch, processes them, and commits. 60 s matches
the feed's `ttl`; on its first start the job worked through the ~4-hour backlog in large batches (`maxOffsetsPerTrigger`).

### Event time vs. processing time
**Processing time** is when Spark sees a row; **event time** is when the thing happened. DockWatch's event time is
`snapshot_ts`, the feed's `last_updated` ("the station looked like this at 15:32:04"). Windows and episodes use it, so
replaying old data or catching up after downtime gives the same answer as processing it live.

Why not `last_reported`? Some stations haven't reported for weeks but still appear in every snapshot. Their state
right now is "offline", which can only be decided at snapshot time.

### Watermarks and late data
A **watermark** says how late data may arrive: `withWatermark("snapshot_ts", "10 minutes")` means "once I've seen
event time T, I won't accept rows older than T − 10 min". It lets Spark **drop old state** (otherwise de-dup keys and
open windows would pile up forever) and decide when a window is final. Rows older than the watermark are dropped as late.

- `silver_status` uses `dropDuplicatesWithinWatermark(["station_id", "snapshot_ts"])`: a snapshot re-sent by a
  restarted producer (minutes later at most) is dropped; its key is forgotten once the watermark passes.
- `availability_5m` writes in **append** mode: a 5-minute window is written once, after the watermark passes its end.
  That's why `availability_5m` lags silver by about 10–15 minutes.

### Stateful processing per key
Empty/full **episodes** need memory across batches: "this station has been empty since 15:32". `applyInPandasWithState`
groups each batch by `station_id`, hands each station's new rows plus its saved state (`kind`, `start_ts`, `last_ts`,
`alerted`) to a Python function, and saves the updated state. The logic (`streaming/episodes.py`) is plain Python:

- a row at or before `last_ts` is ignored → duplicates and late rows are harmless;
- entering `empty`/`full` opens an episode; any other state (including `offline`) closes it;
- after 15 minutes an alert is `raised`, once; closing an alerted episode sends `resolved`.

State is stored in RocksDB inside the checkpoint, so it survives restarts.

### Checkpoints and exactly-once
Each query's checkpoint holds (1) the Kafka offsets of every batch, written **before** the batch runs, (2) a commit
marker written **after** the sink succeeds, and (3) the state store. After a crash, Spark re-runs the last batch with the
**same offsets**. Whether that creates duplicates depends on the sink:

- **Iceberg sink (bronze, silver, availability):** Iceberg records which streaming batch id each commit came from, and
  skips a batch it already committed. Re-running is safe → exactly-once.
  *Proved:* after `docker kill` (no clean shutdown) and a restart, bronze held every offset 0…N exactly once in all six
  partitions (`python tasks.py inspect`).
- **`foreachBatch` (episodes):** you get at-least-once and must make the writes idempotent yourself.
  `MERGE INTO … WHEN MATCHED AND s.updated_ts >= t.updated_ts` makes the episode upsert safe to repeat; Kafka writes
  can repeat, so alerts are keyed by `alert_id` and consumers keep one per key (14 repeats were seen after the kill test).

---

## Apache Iceberg (M2)

Iceberg is a **table format**: Parquet data files plus metadata files that say which data files make up each version
(**snapshot**) of the table. A **catalog** (here the Iceberg REST catalog; Glue on AWS) stores the pointer to the current
metadata file. Committing = atomically swapping that pointer, which is why readers never see half a batch.

- **Snapshots and time travel:** each streaming batch is a commit; `SELECT … FROM lake.silver.station_status.snapshots`
  lists them (641 rows added per minute in steady state). Older versions can be queried with `VERSION AS OF`.
- **Hidden partitioning:** tables are `PARTITIONED BY (days(snapshot_ts))`. Queries filter on `snapshot_ts` and
  Iceberg prunes files itself; nobody has to know or filter on a separate `date` column.
- **Schema evolution:** `ALTER TABLE … ADD COLUMN num_scooters_available int` only changes metadata. Old files
  don't have the column and read it as null; new files have it. Columns are tracked by id, not name or position,
  so renames and reorders are safe too. (Spark writes by position, so new columns are added last in the code.)
- **`MERGE INTO`:** row-level upsert in one atomic commit, used for episodes (insert new, update open, close finished).
- **Small files:** every minute each query writes at least one small file per partition (~45 KB for silver). That's
  fine for writing but slow and costly to read; M4 compacts them with `rewrite_data_files` and expires old snapshots.

---

## Change data capture (M3)

**Change data capture (CDC)** turns every committed insert, update and delete in a database into an event, in commit
order, without the application doing anything extra. DockWatch reads the Postgres `ops` database this way (Debezium
on Kafka Connect) and applies the events to Iceberg tables `lake.ops.*` (Spark `MERGE INTO`).

### Logical replication, pgoutput and the publication
Postgres writes every change to its write-ahead log (WAL) first. With `wal_level=logical` the WAL also holds enough to
rebuild row changes, and **logical replication** decodes it into a stream of row events. **pgoutput** is the decoder
built into Postgres (no extension to install); Debezium asks for it with `plugin.name=pgoutput`. A **publication**
says which tables the stream includes: `dockwatch_ops` lists exactly the five ops tables, so other tables (and the
`iceberg_catalog` database) never leave the server. `ops-schema` owns the publication, and the connector is told not
to create its own (`publication.autocreate.mode=disabled`).

### The replication slot and WAL retention
A **replication slot** is the server's bookmark for one consumer: it remembers the last WAL position (LSN) the
consumer confirmed, and Postgres keeps every WAL file after that point until it is confirmed. That is what makes CDC
lossless across a Connect restart — and what makes a forgotten slot dangerous: while nobody reads it, WAL piles up on
disk. In DockWatch the connector creates slot `dockwatch_ops` when it is registered, and `cdc-reset` drops it. No
slot exists before Connect does, so nothing holds WAL while CDC is unused.

### REPLICA IDENTITY
By default an UPDATE or DELETE in the WAL carries only the primary key of the old row. `REPLICA IDENTITY FULL` makes
Postgres log the whole old row, so Debezium's `before` image is complete: a delete event still says which station
and status the job had, and there are no "unchanged TOAST value" placeholders. It costs more WAL per change, which is
fine at this volume.

### The snapshot
A new connector cannot get old changes from the WAL (they are gone), so `snapshot.mode=initial` first reads every
existing row and emits it as op `r` (read), then switches to the stream at the exact LSN where the snapshot was taken.
The seeded run registers the connector on empty tables, so it sees 0 `r` events and every row arrives as `c`.

### The Debezium envelope and the LSN
Each Kafka message (topic `ops.public.<table>`, key = the primary key) holds an envelope:
`op` (`c` create, `u` update, `d` delete, `r` snapshot read), `before`, `after`, and `source` (database, table,
transaction id and **`lsn`**, the WAL position of the change). With the JSON converter's `schemas.enable=true` every
message also carries its schema: column names and Connect types (`int64`, `double`, `io.debezium.time.ZonedTimestamp`
…). The apply takes the table from the topic, the key columns from the key schema and the column types from the value
schema; a delete takes its key from `before`.

**Why LSN order is the right order per row:** a writer holds the row lock until it commits, so two transactions that
change the same row are serialised — the second one's change is written to the WAL after the first committed. So
for any single row, ordering events by LSN gives commit order. (Across *different* rows LSN order and commit order can
differ, but the apply never needs that.) Each table is one Kafka partition, so Kafka offset order agrees too; the
offset only breaks ties.

### MERGE INTO with deletes, and idempotent replay
Each micro-batch keeps the newest event per key by (`lsn`, offset) and runs one `MERGE INTO` per table:
```sql
WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN DELETE
WHEN MATCHED AND s._lsn > t._lsn               THEN UPDATE SET ...
WHEN NOT MATCHED AND s._op <> 'd'              THEN INSERT ...
```
Every Iceberg row stores the `_lsn` of the change that produced it. An event whose LSN is not newer than the row's
changes nothing, so applying the same events twice — a Spark retry, or `cdc-catchup --replay` from the earliest
offset with a fresh checkpoint — is **idempotent**: the table converges to the same rows and the same checksum. One
subtlety: hard deletes leave no row to compare against, so a replay briefly re-inserts a deleted row from its old
insert and the later delete event removes it again; by the end of the replay the state is identical.

### Schema evolution end to end
Migration 001 adds `rebalancing_jobs.priority` in Postgres. Debezium notices the new column on the next change and
the message schema grows a field. Before the MERGE, the apply compares the incoming columns with the Iceberg table
and runs `ALTER TABLE … ADD COLUMN priority int` (a metadata-only change in Iceberg). It is add-only: a changed
type, or a Connect type it doesn't know, stops the apply with an error instead of guessing. The migration has no
`DEFAULT` on purpose: a default would fill old rows without any WAL event, and Iceberg would never learn those values.

### Proving it: counts and checksums
`cdc-verify` computes one checksum per table on both sides with the same function (`cdc/checksum.py`): canonical
text per value, sha256 per row, sha256 of the sorted row hashes, over Postgres's columns. Equal row counts and equal
checksums for all five tables mean Iceberg holds exactly what Postgres holds.

### Capping the slot's WAL, `wal_status`, and how long the change topics keep events
The slot paragraph above says no slot exists before Connect does; that is only true before the first `connect-up`.
After it, slot `dockwatch_ops` stays (inactive) between CDC runs until `cdc-reset` drops it, and it keeps holding WAL
the whole time. So ops-db runs with **`max_slot_wal_keep_size=1024MB`**: a slot may hold back at most 1 GB of WAL.
`pg_replication_slots.wal_status` says where a slot stands:
- **`reserved`**: what it needs is within `max_wal_size`; the normal state.
- **`extended`**: it holds more than that, but still within the cap.
- **`unreserved`**: past the cap; the WAL it needs will be removed at the next checkpoint unless the consumer catches
  up first.
- **`lost`**: that WAL is gone. The slot can never resume, because the changes in between no longer exist anywhere.

`safe_wal_size` is how much more WAL can be written before the slot becomes unreserved (NULL without a cap). The cap
protects the disk, and the catalog database's WAL along with it; the cost is that a long gap with Connect stopped can
lose the slot. `connect-up` checks `wal_status` first: on `lost` it refuses and points to `python tasks.py cdc-reset`
(rebuild from a fresh snapshot); on `unreserved` it warns and starts Connect, which may still save the slot.

The Kafka side has the same question: how long `ops.public.*` keeps events. A rebuild of Iceberg from Kafka
(`cdc-catchup --replay`) needs *every* change since the snapshot, so time-based retention (7 days by default) would
quietly drop old rows. **Log compaction** keeps the newest event per key, which is enough to rebuild state, but it
leaves gaps in the offsets, which Spark's Kafka source treats as data loss by default, and it loses the history of
each row. DockWatch sets **`retention.ms=-1`** (keep forever, `cleanup.policy=delete`) on the five change topics only,
and accepts that they grow; `cdc-reset` clears them, and compaction plus snapshot-based rebuilds is the plan once they
get large.

## Table maintenance and orchestration (M4a)
**Small files.** A streaming job commits to Iceberg every minute, and every commit writes at least one new data file
per table and partition, plus a new snapshot and manifest. After a few days `silver.station_status` had 405 files of
~20 KB each for under a million rows. Every query has to open, and plan around, every one of those files, so reads get
slower as the table ages even though the data is small.

**Compaction** rewrites many small files into a few large ones without changing a single row. In Trino it is
`ALTER TABLE lake.silver.station_status EXECUTE optimize(file_size_threshold => '100MB')`: every file under the
threshold is read and rewritten, and the change is committed as one new snapshot whose `operation` is `replace`
(the rows are the same, only the files changed). DockWatch's first run took silver from 405 files to 4, one per day
partition. Because a `replace` snapshot adds no data, `verify-lake` ignores it when it asks "did anything commit in
the last hour?"; otherwise a maintenance run would make a stopped stream look healthy.

**`expire_snapshots`.** Every commit keeps the previous snapshots, so you can time-travel to them; that is also why
the old small files stay on disk after compaction: an old snapshot still points at them. `EXECUTE
expire_snapshots(retention_threshold => '1d')` forgets snapshots older than a day (never the current one) and deletes
the files only they used. After it you can no longer read the table "as of" two days ago. DockWatch refuses less
than a day, in code and in the Trino catalog: Spark is never writing while Trino runs, but a day of history is cheap
insurance. So when every snapshot is younger than a day, a compacting run ends with one *more* snapshot than
it started with: the `replace` that `optimize` just committed is the current one, and nothing is old enough to expire.

**Orphan files** are files in the table's folder that no snapshot references, typically left by a write that
failed before it committed. Nothing reads them and nothing would ever delete them, so `EXECUTE
remove_orphan_files(retention_threshold => '1d')` lists the folder, compares it with the metadata and deletes what is
unreferenced and older than a day (younger files might belong to a commit still in flight). The first run removed 75
orphans from bronze.

**Partition spec and partition evolution.** An Iceberg table's partition spec says how rows are grouped into folders
and files, for example `day(snapshot_ts)`: one partition per UTC day, so a query for one day skips every other day's
files. Unlike Hive-style tables, the spec can change without rewriting data (*partition evolution*): `ALTER TABLE …
SET PROPERTIES partitioning = ARRAY['hour(snapshot_ts)']` adds a new spec with a new id; old files keep spec 0, new
files are written with spec 1, and queries plan across both. The trade-off is file count: by hour, a stream writes
up to 24 times as many partitions a day, and compaction can never merge files across partitions.
`partition_evolution_demo` measures that on a lab copy (`lab.status_partition_evolution`) before production tables
change.

**DAG.** Airflow describes a workflow as a DAG, a directed acyclic graph of tasks (`guard → maintain → record`):
each task runs only after the ones it depends on, and no path loops back. A DAG file is Python that builds the
graph; Airflow's scheduler parses it and decides when each task runs. DockWatch keeps the DAG files thin and puts the
logic in `src/dockwatch/batch/`, where host tests cover it without Airflow.

**Logical date.** Each run of a scheduled DAG covers a data interval, and its *logical date* (`execution_date` in
older Airflow) is the start of that interval, not the moment it runs: the `@daily` run that starts just after
midnight on Oct 10 has logical date Oct 9. That makes a run repeatable: rerun the Oct 9 run next week and it still
means "Oct 9". `python tasks.py dag-test` passes an explicit logical date so it can find its run afterwards.

**Catchup.** If a DAG's `start_date` is in the past, or the scheduler was off for a while, Airflow by default creates
a run for every missed interval (*catchup*). That is what a backfill wants (M4b's `trip_history_monthly` will load
history that way), but not maintenance: compacting yesterday's state 30 times is pointless. DockWatch's DAGs set
`catchup=False` and `max_active_runs=1`, so a scheduler that was off just runs the latest interval once.

**Retries with exponential backoff.** Most batch failures are transient (Trino restarting, a network blip), so each
task gets `retries=3`. With exponential backoff the wait doubles each time, starting at 1 minute and capped at 30
(plus some jitter so many tasks don't retry in lockstep): a short outage is retried quickly, a long one is not
hammered. Failures that a retry cannot fix, such as a Spark job running beside Trino or a lost replication slot,
raise `AirflowFailException` and fail at once. Whatever finally fails calls `on_failure_callback`, which writes one
alert line to `data/airflow/alerts.jsonl`.

**Keeping an idle replication slot moving.** A logical replication slot pins every byte of WAL written after the
last position its consumer *confirmed*, in any database of the cluster. Two details decide how the slot is freed.
First, Debezium only confirms a position after it has streamed a change from a captured table past its stored
offset; WAL from other databases is filtered out before it reaches Debezium, and a transaction with no captured
change arrives as nothing at all. Second, Postgres frees WAL by moving the slot's `restart_lsn`, and it moves it only
when a confirmed position passes a "restart candidate" taken from a running-transactions record in the WAL; it holds
one candidate at a time. So `cdc_slot_keepalive` makes the change itself: once the connector runs it updates one van's
`updated_at` and waits for the confirm; if the first confirm only consumed an old candidate and the slot still pins
the backlog, it touches the row once more. The price is visible in the data: each catch-up adds one or two `u`
events on `ops.public.vans` and a newer `updated_at` on one van.

## dbt, marts and backfills (M4b)

**dbt.** dbt (data build tool) turns SQL `SELECT` statements into tables. Each model is one `.sql` file, and dbt works
out the order from the `ref()` and `source()` calls inside them. `dbt build` creates every model and runs every test,
in dependency order. DockWatch runs dbt-core with the `dbt-trino` adapter, so the SQL runs on Trino against the
Iceberg catalog. On AWS the same project would use `dbt-athena`.

**Source.** A *source* is a table that dbt reads but does not build, such as `silver.availability_5m` (written by
Spark) or `bronze.trips` (written by the trip loader). Declaring sources in YAML lets models say
`source('silver', 'availability_5m')` instead of hard-coding names, and lets tests run on the raw inputs too.

**Ephemeral model.** A staging model cleans and renames one source (`stg_trips` adds the Pacific start hour, for
example). DockWatch makes them *ephemeral*: dbt inlines them as a CTE into each model that uses them, and nothing is
created in Trino. That keeps the warehouse to the tables people read.

**Mart.** A mart is a table shaped for one question, at one stated grain (one row per station and day, per station,
or per station and hour). The three DockWatch marts answer "how often is each station empty or full", "where is
rebalancing most needed" and "do busy trip hours match the hours a station is empty". The website's insights files
are exported from them.

**dbt test.** A dbt test is a query that returns the rows that break a rule; zero rows means it passed. Generic tests
(`not_null`, `unique`, `accepted_values`, `relationships`) are declared in YAML, and singular tests are hand-written
SQL (for example, that no share is outside 0 to 1). `marts_daily` reads dbt's `run_results.json`, and its quality gate
refuses to export if any test failed.

**Backfill and data interval.** Every scheduled run covers a *data interval*. For the `@monthly`
`trip_history_monthly`, the run whose interval starts on 2026-09-01 loads September 2026. A *backfill* creates runs
for past intervals on purpose: `airflow dags backfill -s 2025-10-01 -e 2026-09-01` makes 12 monthly runs and runs them
one at a time. The DAG stays paused, so the scheduler never starts the ~90 months since 2019 on its own.

**Idempotent reload.** An idempotent step gives the same result however many times it runs. Each trip month is loaded
by deleting that month and appending it again in *one* Iceberg commit. So a rerun replaces the month: readers see the
old rows or the new rows, never both and never none.

**External table.** An external table is a table definition over files that the engine does not own, such as CSV in
object storage read through a Hive catalog. M4b first tried that for the raw trip CSVs, but Trino ran out of
Metaspace. Now Python reads the CSV and pyiceberg writes Iceberg directly, so there is no external table.

**Sensor.** A sensor is an Airflow task that waits for a condition instead of doing work. `published` checks the
Bay Wheels listing until the month's ZIP exists. Lyft publishes around the 5th, while the scheduled run for a month
fires on the 1st of the next one. In `reschedule` mode the sensor frees its worker slot between checks.
