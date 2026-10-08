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
