-- The Iceberg REST catalog keeps its table pointers in its own database on the same Postgres instance.
-- (SQLite could not handle concurrent commits from the streaming queries: SQLITE_BUSY. See DEVLOG 2026-10-07.)
-- Runs only when the Postgres volume is first created; on an existing volume run: createdb -U ops iceberg_catalog
CREATE DATABASE iceberg_catalog OWNER ops;
