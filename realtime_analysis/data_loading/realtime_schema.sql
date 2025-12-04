-- Schema objects for GTFS-Realtime ingestion
-- Run with: cat realtime_analysis/data_loading/realtime_schema.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS mobilitydb;

CREATE TABLE IF NOT EXISTS rt_vehicle_positions (
    id                  bigserial PRIMARY KEY,
    fetch_timestamp     timestamptz NOT NULL,
    entity_timestamp    timestamptz NOT NULL,
    trip_instance_id    text,
    trip_id             text,
    route_id            text,
    direction_id        smallint,
    start_time          text,
    start_date          date,
    vehicle_id          text,
    vehicle_label       text,
    license_plate       text,
    current_stop_sequence integer,
    stop_id             text,
    current_status      text,
    schedule_relationship text,
    occupancy_status    text,
    bearing             double precision,
    speed_mps           double precision,
    latitude            double precision NOT NULL,
    longitude           double precision NOT NULL,
    geom                geometry(Point, 4326),
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rt_vehicle_positions_trip_instance
    ON rt_vehicle_positions (trip_instance_id, entity_timestamp);

CREATE INDEX IF NOT EXISTS idx_rt_vehicle_positions_route_time
    ON rt_vehicle_positions (route_id, entity_timestamp);

CREATE INDEX IF NOT EXISTS idx_rt_vehicle_positions_geom
    ON rt_vehicle_positions
    USING GIST (geom);

CREATE TABLE IF NOT EXISTS rt_trip_updates (
    id                       bigserial PRIMARY KEY,
    fetch_timestamp          timestamptz NOT NULL,
    entity_timestamp         timestamptz,
    trip_instance_id         text,
    trip_id                  text,
    route_id                 text,
    start_time               text,
    start_date               date,
    vehicle_id               text,
    stop_sequence            integer,
    stop_id                  text,
    arrival_time             timestamptz,
    arrival_delay_seconds    integer,
    departure_time           timestamptz,
    departure_delay_seconds  integer,
    schedule_relationship    text,
    stop_schedule_relationship text,
    created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rt_trip_updates_trip_instance
    ON rt_trip_updates (trip_instance_id, stop_sequence);

CREATE INDEX IF NOT EXISTS idx_rt_trip_updates_trip
    ON rt_trip_updates (trip_id, start_date, stop_sequence);

CREATE TABLE IF NOT EXISTS realtime_trips_mdb (
    trip_instance_id text PRIMARY KEY,
    trip_id          text,
    route_id         text,
    service_date     date,
    vehicle_id       text,
    trip             tgeompoint,
    traj             geometry,
    starttime        timestamptz,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_realtime_trips_mdb_route
    ON realtime_trips_mdb (route_id, service_date);

CREATE INDEX IF NOT EXISTS idx_realtime_trips_mdb_traj
    ON realtime_trips_mdb
    USING GIST (traj);

CREATE INDEX IF NOT EXISTS idx_realtime_trips_mdb_trip
    ON realtime_trips_mdb
    USING GIST (trip);

DROP VIEW IF EXISTS rt_trip_instances;
CREATE VIEW rt_trip_instances AS
WITH base AS (
    SELECT
        COALESCE(trip_instance_id, trip_id || '_' || to_char(entity_timestamp, 'YYYYMMDDHH24MISS')) AS trip_instance_id,
        trip_id,
        route_id,
        vehicle_id,
        start_date,
        entity_timestamp
    FROM rt_vehicle_positions
    WHERE trip_id IS NOT NULL OR vehicle_id IS NOT NULL
)
SELECT
    trip_instance_id,
    MIN(trip_id) AS trip_id,
    MIN(route_id) AS route_id,
    MIN(vehicle_id) AS vehicle_id,
    MIN(start_date) AS start_date,
    MIN(entity_timestamp) AS first_observed
FROM base
GROUP BY trip_instance_id;


