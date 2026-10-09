-- DockWatch operational database (M3), database `ops` on compose service ops-db.
-- Idempotent: `python tasks.py ops-schema` runs this file (then sql/ops/migrations/*.sql) on every call.
-- All times are timestamptz; lat/lon are double precision (no numeric / date, so CDC types stay simple).
-- Every table has created_at / updated_at; set_updated_at() keeps updated_at honest on UPDATE.
-- REPLICA IDENTITY FULL: deletes and updates carry the whole old row in the WAL (Debezium's `before`).

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END
$$;

CREATE TABLE IF NOT EXISTS stations (
    station_id  text PRIMARY KEY,
    name        text NOT NULL,
    short_name  text,
    lat         double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon         double precision NOT NULL CHECK (lon BETWEEN -180 AND 180),
    capacity    integer NOT NULL CHECK (capacity >= 0),
    region_id   text,
    is_active   boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS docks (
    dock_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id  text NOT NULL REFERENCES stations (station_id),
    dock_number integer NOT NULL CHECK (dock_number > 0),
    status      text NOT NULL DEFAULT 'ok' CHECK (status IN ('ok', 'out_of_service')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (station_id, dock_number)
);

CREATE TABLE IF NOT EXISTS vans (
    van_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    plate       text NOT NULL UNIQUE,
    capacity    integer NOT NULL CHECK (capacity BETWEEN 1 AND 40),
    status      text NOT NULL DEFAULT 'idle' CHECK (status IN ('idle', 'busy', 'maintenance')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rebalancing_jobs (
    job_id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id       text NOT NULL REFERENCES stations (station_id),
    van_id           bigint REFERENCES vans (van_id),
    status           text NOT NULL CHECK (status IN ('open', 'assigned', 'done', 'cancelled')),
    alert_episode_id text UNIQUE,  -- one job per empty episode (streaming/episodes.py episode_id)
    opened_at        timestamptz NOT NULL,
    closed_at        timestamptz,
    bikes_moved      integer CHECK (bikes_moved >= 0),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT rebalancing_jobs_close_after_open CHECK (closed_at IS NULL OR closed_at >= opened_at),
    CONSTRAINT rebalancing_jobs_closed_iff_finished CHECK ((status IN ('done', 'cancelled')) = (closed_at IS NOT NULL)),
    CONSTRAINT rebalancing_jobs_assigned_has_van CHECK (status <> 'assigned' OR van_id IS NOT NULL),
    CONSTRAINT rebalancing_jobs_done_has_bikes CHECK (status <> 'done' OR bikes_moved IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS maintenance_tickets (
    ticket_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dock_id     bigint NOT NULL REFERENCES docks (dock_id),
    issue       text NOT NULL CHECK (issue IN ('jammed', 'broken_lock', 'no_power', 'damaged', 'other')),
    status      text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'closed')),
    opened_at   timestamptz NOT NULL,
    closed_at   timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT maintenance_tickets_close_after_open CHECK (closed_at IS NULL OR closed_at >= opened_at),
    CONSTRAINT maintenance_tickets_closed_iff_closed CHECK ((status = 'closed') = (closed_at IS NOT NULL))
);

-- The two hot-query indexes (sql/ops/PLANS.md): both partial, so they only hold the few unfinished rows.
-- ops_sim/db.py OPEN_JOB_AT_STATION: the open job at a station.
CREATE INDEX IF NOT EXISTS rebalancing_jobs_open_station_idx
    ON rebalancing_jobs (station_id) WHERE status IN ('open', 'assigned');
-- ops_sim/db.py OPEN_TICKET_ON_DOCK: the unfinished ticket on a dock.
CREATE INDEX IF NOT EXISTS maintenance_tickets_open_dock_idx
    ON maintenance_tickets (dock_id) WHERE status <> 'closed';

CREATE OR REPLACE TRIGGER stations_set_updated_at BEFORE UPDATE ON stations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER docks_set_updated_at BEFORE UPDATE ON docks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER vans_set_updated_at BEFORE UPDATE ON vans
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER rebalancing_jobs_set_updated_at BEFORE UPDATE ON rebalancing_jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE OR REPLACE TRIGGER maintenance_tickets_set_updated_at BEFORE UPDATE ON maintenance_tickets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE stations REPLICA IDENTITY FULL;
ALTER TABLE docks REPLICA IDENTITY FULL;
ALTER TABLE vans REPLICA IDENTITY FULL;
ALTER TABLE rebalancing_jobs REPLICA IDENTITY FULL;
ALTER TABLE maintenance_tickets REPLICA IDENTITY FULL;

-- Publication for Debezium (pgoutput): exactly these five tables. The replication slot is created by the
-- connector (slot.name dockwatch_ops) and dropped by `python tasks.py cdc-reset`.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dockwatch_ops') THEN
        CREATE PUBLICATION dockwatch_ops
            FOR TABLE stations, docks, vans, rebalancing_jobs, maintenance_tickets;
    ELSE
        ALTER PUBLICATION dockwatch_ops
            SET TABLE stations, docks, vans, rebalancing_jobs, maintenance_tickets;
    END IF;
END
$$;
