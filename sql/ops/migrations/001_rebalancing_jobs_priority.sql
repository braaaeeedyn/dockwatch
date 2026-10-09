-- M3 schema change that flows end to end through Debezium into Iceberg (lake.ops.rebalancing_jobs).
-- No DEFAULT on purpose: a default would fill existing rows without any WAL event, so Postgres and Iceberg would
-- disagree. Old rows stay NULL; the simulator sets priority on every job it inserts or updates from now on.
ALTER TABLE rebalancing_jobs ADD COLUMN IF NOT EXISTS priority smallint CHECK (priority BETWEEN 1 AND 3);
