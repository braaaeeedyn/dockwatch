# DockWatch: measurements

Every number below was measured locally on this machine (a 16 GB Windows 11 laptop, Docker Desktop on WSL2 with a
7.6 GiB VM, Chrome closed) on 2026-10-20 UTC, during Loop O. Each row names the command that produced it. Rerun that
command against the same window and it prints the same number (the lab tables are never compacted, so their windows
stay reproducible). Production snapshots older than a day are expired by `iceberg_maintenance`, so the commit-based
numbers of the 1× hour are also saved in `data/metrics/window-*.json`. The one number that needs AWS (Athena) is
marked "needs AWS".

## Summary

| Measurement | Value | Command | Run |
|---|---|---|---|
| Latency p50, fetch to silver (1× stream) | 59.3 s | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Latency p95, fetch to silver (1× stream) | 60.9 s | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Latency p50, fetch to queryable (1× stream) | 64.1 s | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Latency p95, fetch to queryable (1× stream) | 68.7 s | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Micro-batch duration p95 (1× stream) | 9.6 s | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Events per minute (1× stream) | 647 events/min | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Fetch to Kafka p50 (clock check) | 0.3 s | `python tasks.py measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z` | `stream` 02:09:58Z – 03:18:34Z |
| Throughput (10× replay) | 6,294 events/min | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:25:00Z 2026-10-20T03:54:00Z` | `stream-lab` 03:25:18Z – 03:53:23Z |
| Micro-batch duration max (10× replay) | 9.7 s | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:25:00Z 2026-10-20T03:54:00Z` | `stream-lab` 03:25:18Z – 03:53:23Z |
| Latency p95, fetch to silver (10× replay) | 58.7 s | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:25:00Z 2026-10-20T03:54:00Z` | `stream-lab` 03:25:18Z – 03:53:23Z |
| Sustained (10× replay) | yes | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:25:00Z 2026-10-20T03:54:00Z` | `stream-lab` 03:25:18Z – 03:53:23Z |
| Throughput (50× replay) | 30,757 events/min | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:55:00Z 2026-10-20T04:15:00Z` | `stream-lab` 03:55:09Z – 04:14:52Z |
| Micro-batch duration max (50× replay) | 11.2 s | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:55:00Z 2026-10-20T04:15:00Z` | `stream-lab` 03:55:09Z – 04:14:52Z |
| Latency p95, fetch to silver (50× replay) | 56.9 s | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:55:00Z 2026-10-20T04:15:00Z` | `stream-lab` 03:55:09Z – 04:14:52Z |
| Sustained (50× replay) | yes | `python tasks.py measure window lake.lab_silver.station_status 2026-10-20T03:55:00Z 2026-10-20T04:15:00Z` | `stream-lab` 03:55:09Z – 04:14:52Z |
| Silver files before compaction | 419 files | `python tasks.py measure files lake.silver.station_status` | `manual__2026-10-20T03:22:09+00:00` |
| Silver files after compaction | 11 files | `python tasks.py measure files lake.silver.station_status` | `manual__2026-10-20T03:22:09+00:00` |
| Silver query time before compaction | 287 ms | `python tasks.py measure files lake.silver.station_status` | `manual__2026-10-20T03:22:09+00:00` |
| Silver query time after compaction | 163 ms | `python tasks.py measure files lake.silver.station_status` | `manual__2026-10-20T03:22:09+00:00` |
| Bronze files before compaction | 79 files | `python tasks.py dag-test iceberg_maintenance` | `manual__2026-10-20T03:22:09+00:00` |
| Bronze files after compaction | 11 files | `python tasks.py dag-test iceberg_maintenance` | `manual__2026-10-20T03:22:09+00:00` |
| Crash-restart duplicates | 0 | `python tasks.py crash-restart` | `2026-10-20T03:19:02Z` |
| Crash-restart missing offsets | 0 | `python tasks.py crash-restart` | `2026-10-20T03:19:02Z` |
| Athena bytes scanned, full backfill vs one month | needs AWS | — | — |

## How it was measured

### Definitions (`python tasks.py measure window <table> <from> <to>`, `src/dockwatch/measure.py`)
- **Window:** the rows of `<table>` with `processed_ts` in [from, to). Spark sets `processed_ts`
  (`current_timestamp()`) once per micro-batch, so one distinct `processed_ts` is one **micro-batch**.
- **Fetch to silver** = `processed_ts - fetched_ts` for every row. `fetched_ts` is when the producer fetched the
  snapshot (in a replay, when `replay` published it). This is the Pipeline page's latency.
- **Fetch to queryable** = the **committed** snapshot's `committed_at - fetched_ts`, for every row: from then on the
  row can be queried. The window's micro-batches, in time order, are paired with the table's `append` snapshots that
  have `added-records`, committed in [from, to + 15 min]. Each batch takes the first unused snapshot committed after
  its `processed_ts` whose `added-records` equals the batch's rows. A no-data batch (it only moves the watermark)
  commits an `append` without `added-records` and never pairs.
- **Micro-batch duration** = `committed_at - processed_ts`, one value per paired batch.
- **Percentiles** are **nearest-rank** over the (value, count) groups of `GROUP BY processed_ts, fetched_ts`: the
  value at rank ⌈q·N⌉ in ascending order, exact. Max is the largest value.
- **Events per minute / throughput** = rows × 60 / (max `kafka_ts` − min `kafka_ts`) over the window's rows.
- **Fetch to Kafka p50** = `approx_percentile(kafka_ts - fetched_ts, 0.5)`: the clock check. `fetched_ts` comes
  from the Windows clock and the other columns from the Docker VM's, so a skew between them shifts every latency.
  0.3 s means the skew is small (it includes the publish itself).
- **Sustained** = every micro-batch paired with its snapshot and finished within the **60 s trigger**. A
  processingTime trigger starts the next batch late only when a batch overruns, so this is the condition for keeping
  up.
- Seconds are rounded to 0.1 s, events per minute and ms to integers, files and counts are exact. The full record
  (rows, batches, p50 / p95 / max of each latency) is in `data/metrics/window-<table>-<UTC>.json`.

### Latency: an hour of the continuous stream (1×)
1. `python tasks.py catchup` (02:09:13Z – 02:09:42Z), so the stream started current.
2. `python tasks.py stream` at 02:09:58Z; the job ran for 68.6 min, watched from foreground calls.
3. `python tasks.py stream-stop` at 03:18:34Z.
4. On a fresh Trino JVM (`python tasks.py batch-up`) after `python tasks.py dag-test pipeline_health`:
   `measure window lake.silver.station_status 2026-10-20T02:15:00Z 2026-10-20T03:18:00Z`. The window starts 5 min
   after `stream` (the JVM, the DDL and the first batches) and spans 63 min.

The hour: 60 micro-batches, 40,114 rows, 0 unpaired, batch p50 / p95 / max 6.2 / 9.6 / 9.6 s; fetch to silver max
61.5 s; fetch to queryable max 69.7 s. `pipeline_health` (`manual__2026-10-20T03:24:02+00:00`) on the same data:
p95 tile 61.2 s, 636.2 events/min, consumer lag 0.

**Why fetch to silver is ~60 s:** the producer polls every 60 s and the trigger fires every 60 s. In this hour each
snapshot reached Kafka just after a trigger had fired, so it waited almost a whole trigger interval for the next
micro-batch (p50 59.3 s, max 61.5 s). The two clocks drift only slowly, so a run with another phase would wait
anywhere from 0 to 60 s; with snapshots arriving all the time, as in the replays, the median wait is half a trigger
(p50 30.8 s and 29.8 s below). A shorter trigger would lower it; the 60 s trigger is a product setting, measured
here as it is.

### Throughput: the archived day 2026-10-16 replayed into lab tables
The archive has no complete day. **2026-10-16** is its longest gap-free day: 782 snapshots of 646 stations over
13.1 hours, one every 60.5 s, no gap over 5 min. `replay` re-publishes archived snapshots into
`gbfs.station_status.replay`, paced by the feed's schedule (snapshot k goes out at start +
(`last_updated`ₖ − `last_updated`₀) / speed). `--skip` and `--limit` take a slice of the day in time order, so a run
fits in foreground calls under 10 min.

The streaming job runs in **lab mode**: `python tasks.py stream-lab` starts the same job, image and 60 s trigger in
one `compose run` container (`dockwatch-stream-lab`). It writes `lake.lab_bronze.station_status` and
`lake.lab_silver.*`, the topics `gbfs.alerts.lab` and `dockwatch.station_state.lab`, and its own checkpoints
`/checkpoints/lab/<UTC>`. It reads the replay topic from its end (`latest`). It refuses production names, and
`stream-lab` returns only after every query has written its Kafka source's starting offsets. No production table,
checkpoint or topic is touched.

- **10×:** `python tasks.py stream-lab` (03:25:18Z), then
  `python tasks.py replay --date 2026-10-16 --speed 10 --skip 0 --limit 80`, the same with `--skip 80` and
  `--skip 160` (480 s each; 240 snapshots, 155,040 records), a 150 s settle, `python tasks.py stream-lab-stop`
  (03:53:23Z). Window 03:25:00Z – 03:54:00Z: 26 micro-batches, 155,040 rows in lab silver and in lab bronze, 240
  distinct snapshots, achieved speed **9.81×** (feed time / Kafka time; the 13–25 s hand-offs between the chunks are
  inside the Kafka span). Batch p50 6.4 s, max 9.7 s; fetch to silver p50 30.8 s; fetch to queryable p95 64.0 s.
- **50×:** `python tasks.py stream-lab` (03:55:09Z), then
  `python tasks.py replay --date 2026-10-16 --speed 50 --skip 0 --limit 391` and `--skip 391 --limit 391` (474 and
  472 s; all 782 snapshots, 505,172 records), a 150 s settle, `python tasks.py stream-lab-stop` (04:14:52Z). Window
  03:55:00Z – 04:15:00Z: 17 micro-batches of ~30,000 rows, 505,172 rows in lab silver and in lab bronze, 782 distinct
  snapshots, achieved speed **48.0×** (a 39 s hand-off between the two chunks). Batch p50 7.1 s, max 11.2 s; fetch to
  silver p50 29.8 s; fetch to queryable p95 64.1 s.
- Both runs are **sustained**: every batch paired with its commit and finished in under 12 s of the 60 s trigger.
  The whole day at 10× (79 min of a second long-running Spark JVM) was not run; 26 one-minute batches at a steady
  rate measure the same thing.
- A first 50× start at 03:53:52Z was stopped before any replay, because its window would have touched the 10× one;
  it left an empty `/checkpoints/lab/20261020T035352Z`.

### Small files: before and after compaction
Each side ran on a fresh Trino JVM, with one discarded warm-up run.
1. `python tasks.py batch-up`, then `python tasks.py measure files lake.silver.station_status`: files, bytes and
   partitions from `$files` / `$partitions`, then
   `SELECT station_id, count(*) AS n, max(snapshot_ts) AS newest FROM lake.silver.station_status GROUP BY station_id`
   once as a warm-up and 5 times more (`--runs`), reporting the median of Trino's own `elapsedTimeMillis`.
2. `python tasks.py dag-test iceberg_maintenance` (run `manual__2026-10-20T03:22:09+00:00`, 50 s), then
   `python tasks.py batch-stop`.
3. `python tasks.py batch-up` and the same `measure files` again.

| | Files | Bytes | Partitions | Warm-up | Runs (ms) | Median |
|---|---|---|---|---|---|---|
| Before (03:21:43Z) | 419 | 17,723,236 | 11 | 1,393 ms | 518, 357, 281, 276, 287 | 287 ms |
| After (03:23:38Z) | 11 | 14,349,364 | 11 | 664 ms | 202, 195, 145, 163, 146 | 163 ms |

The same query ran 1.8× faster on 11 files than on 419. The bronze file counts are the same run's
`lake.metrics.maintenance_runs` rows: `bronze.station_status` 79 → 11 files (3,915,131 rows both sides); silver 419 →
11 there too. The run also compacted `silver.availability_5m` 27 → 11.

### Crash-restart: SIGKILL mid-batch, then a zero-diff count
`python tasks.py crash-restart` (03:18:55Z – 03:20:18Z, 83 s):
1. It starts `stream` and polls the production container's `/checkpoints/<query>/offsets` against `commits` every
   0.5 s, until `silver_status` has a batch that this JVM planned and has not committed.
2. It sends **SIGKILL** at once (`docker kill`, exit 137, no shutdown hook). Here that was 03:19:02Z, 0.35 s after
   the queries started: silver batch 301 had its offsets logged and nothing committed. Bronze 205, availability 302
   and episodes 204 were in flight as well.
3. It reads `gbfs.station_status`'s end offsets, runs `python tasks.py catchup` and then `verify-lake`, and writes
   `data/metrics/crash-restart-20261020T031902Z.json`.

Result: catch-up and `verify-lake` exited 0. Bronze held 3,915,131 rows and 3,915,131 distinct (partition, offset)
pairs, with 0 silver duplicates and 0 offset gaps. In all 6 partitions bronze's newest offset is the end offset at the
kill minus 1, so every record published before the kill is in the lake exactly once.
- duplicates = rows − distinct offsets + silver duplicates = **0**;
- missing = gaps + offsets published before the kill that bronze lacks = **0**.

### Athena
Bytes scanned for a full backfill vs one incremental month: **needs AWS** (the AWS account, M6). There is no local
stand-in for Athena's scan accounting.

### Memory during the campaign (docker stats every 5 s; limits 2 GiB for Spark and Trino)
The stream JVM peaked at 2,022 MiB, of which ~1,855 MiB was RSS and the rest page cache (cgroup `memory.stat`). The
lab JVM peaked at 2,033 MiB at 10× and 2,005 MiB at 50×. The catch-ups peaked at 1,455, 1,695 (after the kill) and
1,261 MiB, `verify-lake` at 1,459 MiB. Trino peaked at 1,970 MiB in `iceberg_maintenance`, 1,380 MiB in
`pipeline_health` plus the window query, and 997 MiB for the lab windows. None was OOMKilled. Host free memory stayed
between 2,643 and 6,294 MiB.
