-- Realtime Analysis Queries for Vancouver GTFS Realtime Data
-- Materialized views for common realtime analysis patterns
-- Run with: cd realtime_analysis/queries && python sql/run_sql.py
-- IMPORTANT: Run this AFTER ingesting realtime data (step 3 of realtime workflow)

-- ============================================
-- BASE VIEW: Deduplicated Trip Updates
-- ============================================
-- This view provides clean, deduplicated trip update data for all realtime analyses
DROP MATERIALIZED VIEW IF EXISTS rt_trip_updates_deduped;
CREATE MATERIALIZED VIEW rt_trip_updates_deduped AS
WITH ranked_updates AS (
    SELECT
        rtu.trip_instance_id,
        rtu.trip_id,
        rtu.route_id,
        rtu.start_date AS service_date,
        rtu.stop_sequence,
        rtu.stop_id,
        rtu.arrival_time AS actual_arrival,
        rtu.departure_time AS actual_departure,
        rtu.arrival_delay_seconds,
        rtu.departure_delay_seconds,
        rtu.fetch_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY rtu.trip_instance_id, rtu.stop_sequence
            ORDER BY rtu.fetch_timestamp DESC
        ) AS rn
    FROM rt_trip_updates rtu
    WHERE rtu.arrival_time IS NOT NULL
)
SELECT
    trip_instance_id,
    trip_id,
    route_id,
    service_date,
    stop_sequence,
    stop_id,
    actual_arrival,
    actual_departure,
    arrival_delay_seconds,
    departure_delay_seconds
FROM ranked_updates
WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_rt_trip_updates_deduped_trip 
    ON rt_trip_updates_deduped (trip_instance_id, stop_sequence);
CREATE INDEX IF NOT EXISTS idx_rt_trip_updates_deduped_route 
    ON rt_trip_updates_deduped (route_id, service_date);
CREATE INDEX IF NOT EXISTS idx_rt_trip_updates_deduped_arrival 
    ON rt_trip_updates_deduped (actual_arrival);

-- ============================================
-- SPEED COMPARISON VIEW
-- ============================================
-- Compares scheduled vs actual speeds for all segments with realtime data
DROP MATERIALIZED VIEW IF EXISTS realtime_speed_comparison;
CREATE MATERIALIZED VIEW realtime_speed_comparison AS
WITH with_next_stop AS (
    SELECT
        d.trip_instance_id,
        d.trip_id,
        d.route_id,
        d.service_date,
        d.stop_sequence,
        d.stop_id,
        d.actual_arrival,
        d.arrival_delay_seconds,
        LEAD(d.stop_sequence) OVER w AS next_stop_sequence,
        LEAD(d.stop_id) OVER w AS next_stop_id,
        LEAD(d.actual_arrival) OVER w AS next_actual_arrival
    FROM rt_trip_updates_deduped d
    WINDOW w AS (PARTITION BY d.trip_instance_id ORDER BY d.stop_sequence)
)
SELECT
    w.trip_instance_id,
    w.trip_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    w.route_id,
    w.service_date,
    w.stop_sequence,
    w.next_stop_sequence,
    w.stop_id,
    w.next_stop_id,
    s1.stop_name AS from_stop_name,
    s2.stop_name AS to_stop_name,
    rs.seg_length AS segment_length_m,
    EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) AS scheduled_seconds,
    EXTRACT(EPOCH FROM (w.next_actual_arrival - w.actual_arrival)) AS actual_seconds,
    w.arrival_delay_seconds,
    EXTRACT(hour FROM w.actual_arrival) AS hour_of_day,
    EXTRACT(dow FROM w.actual_arrival) AS day_of_week,
    -- Calculated speeds in km/h
    (rs.seg_length / NULLIF(EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)), 0) * 3.6) AS scheduled_speed_kmh,
    (rs.seg_length / NULLIF(EXTRACT(EPOCH FROM (w.next_actual_arrival - w.actual_arrival)), 0) * 3.6) AS actual_speed_kmh
FROM with_next_stop w
JOIN route_segments rs
    ON rs.trip_id = w.trip_id
    AND rs.stop1_sequence = w.stop_sequence
LEFT JOIN routes r ON r.route_id = w.route_id
LEFT JOIN stops s1 ON s1.stop_id = w.stop_id
LEFT JOIN stops s2 ON s2.stop_id = w.next_stop_id
WHERE w.next_actual_arrival IS NOT NULL
  AND rs.seg_length > 10
  AND EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) > 0
  AND EXTRACT(EPOCH FROM (w.next_actual_arrival - w.actual_arrival)) > 0
  AND EXTRACT(EPOCH FROM (w.next_actual_arrival - w.actual_arrival)) < 3600;

CREATE INDEX IF NOT EXISTS idx_realtime_speed_comparison_trip 
    ON realtime_speed_comparison (trip_instance_id, stop_sequence);
CREATE INDEX IF NOT EXISTS idx_realtime_speed_comparison_route 
    ON realtime_speed_comparison (route_id, service_date);
CREATE INDEX IF NOT EXISTS idx_realtime_speed_comparison_hour 
    ON realtime_speed_comparison (hour_of_day);

-- ============================================
-- DELAY ANALYSIS VIEW
-- ============================================
-- Segment-level delay analysis with time period categorization
DROP MATERIALIZED VIEW IF EXISTS realtime_delay_analysis;
CREATE MATERIALIZED VIEW realtime_delay_analysis AS
WITH with_next AS (
    SELECT
        d.trip_instance_id,
        d.trip_id,
        d.route_id,
        d.service_date,
        d.stop_sequence AS from_seq,
        d.stop_id AS from_stop_id,
        d.actual_arrival AS from_arrival,
        d.arrival_delay_seconds AS from_delay,
        LEAD(d.stop_sequence) OVER w AS to_seq,
        LEAD(d.stop_id) OVER w AS to_stop_id,
        LEAD(d.actual_arrival) OVER w AS to_arrival,
        LEAD(d.arrival_delay_seconds) OVER w AS to_delay
    FROM rt_trip_updates_deduped d
    WINDOW w AS (PARTITION BY d.trip_instance_id ORDER BY d.stop_sequence)
)
SELECT
    w.trip_instance_id,
    w.trip_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    w.route_id,
    w.service_date,
    w.from_seq,
    w.to_seq,
    w.from_stop_id,
    w.to_stop_id,
    s1.stop_name AS from_stop_name,
    s2.stop_name AS to_stop_name,
    ST_Y(s1.stop_loc::geometry) AS from_lat,
    ST_X(s1.stop_loc::geometry) AS from_lon,
    ST_Y(s2.stop_loc::geometry) AS to_lat,
    ST_X(s2.stop_loc::geometry) AS to_lon,
    rs.seg_length AS segment_length_m,
    rs.seg_geom,
    ST_AsGeoJSON(rs.seg_geom) AS seg_geojson,
    EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) AS scheduled_seconds,
    EXTRACT(EPOCH FROM (w.to_arrival - w.from_arrival)) AS actual_seconds,
    w.from_delay,
    w.to_delay,
    (w.to_delay - w.from_delay) AS segment_delay_change,
    -- Delay in minutes
    (EXTRACT(EPOCH FROM (w.to_arrival - w.from_arrival)) - 
     EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time))) / 60.0 AS segment_delay_minutes,
    EXTRACT(hour FROM w.from_arrival) AS hour_of_day,
    EXTRACT(dow FROM w.from_arrival) AS day_of_week,
    CASE 
        WHEN EXTRACT(dow FROM w.from_arrival) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    CASE
        WHEN EXTRACT(hour FROM w.from_arrival) BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(hour FROM w.from_arrival) BETWEEN 16 AND 18 THEN 'Evening Rush'
        WHEN EXTRACT(hour FROM w.from_arrival) BETWEEN 9 AND 16 THEN 'Midday'
        WHEN EXTRACT(hour FROM w.from_arrival) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_period
FROM with_next w
JOIN route_segments rs 
    ON rs.trip_id = w.trip_id 
    AND rs.stop1_sequence = w.from_seq
LEFT JOIN routes r ON r.route_id = w.route_id
LEFT JOIN stops s1 ON s1.stop_id = w.from_stop_id
LEFT JOIN stops s2 ON s2.stop_id = w.to_stop_id
WHERE w.to_arrival IS NOT NULL
  AND rs.seg_length > 10
  AND EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) > 0
  AND EXTRACT(EPOCH FROM (w.to_arrival - w.from_arrival)) > 0
  AND EXTRACT(EPOCH FROM (w.to_arrival - w.from_arrival)) < 3600;

CREATE INDEX IF NOT EXISTS idx_realtime_delay_analysis_trip 
    ON realtime_delay_analysis (trip_instance_id, from_seq);
CREATE INDEX IF NOT EXISTS idx_realtime_delay_analysis_time_period 
    ON realtime_delay_analysis (time_period, hour_of_day);
CREATE INDEX IF NOT EXISTS idx_realtime_delay_analysis_geom 
    ON realtime_delay_analysis USING GIST (seg_geom);

-- ============================================
-- HEADWAY ANALYSIS VIEW
-- ============================================
-- Headway statistics (time between consecutive vehicles) for bus bunching analysis
DROP MATERIALIZED VIEW IF EXISTS realtime_headway_stats;
CREATE MATERIALIZED VIEW realtime_headway_stats AS
WITH stop_arrivals AS (
    SELECT
        rtu.route_id,
        r.route_short_name,
        r.route_long_name,
        rtu.stop_id,
        s.stop_name,
        ST_Y(s.stop_loc::geometry) AS stop_lat,
        ST_X(s.stop_loc::geometry) AS stop_lon,
        rtu.trip_instance_id,
        rtu.trip_id,
        rtu.arrival_time,
        EXTRACT(hour FROM rtu.arrival_time) AS hour_of_day,
        EXTRACT(dow FROM rtu.arrival_time) AS day_of_week,
        CASE 
            WHEN EXTRACT(dow FROM rtu.arrival_time) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE
            WHEN EXTRACT(hour FROM rtu.arrival_time) BETWEEN 7 AND 9 THEN 'Morning Rush'
            WHEN EXTRACT(hour FROM rtu.arrival_time) BETWEEN 16 AND 18 THEN 'Evening Rush'
            WHEN EXTRACT(hour FROM rtu.arrival_time) BETWEEN 9 AND 16 THEN 'Midday'
            WHEN EXTRACT(hour FROM rtu.arrival_time) BETWEEN 18 AND 22 THEN 'Evening'
            ELSE 'Night'
        END AS time_period
    FROM rt_trip_updates rtu
    JOIN routes r ON r.route_id = rtu.route_id
    LEFT JOIN stops s ON s.stop_id = rtu.stop_id
    WHERE rtu.arrival_time IS NOT NULL
      AND rtu.stop_id IS NOT NULL
),
with_prev AS (
    SELECT
        *,
        LAG(arrival_time) OVER (
            PARTITION BY route_id, stop_id
            ORDER BY arrival_time
        ) AS prev_arrival,
        LAG(trip_instance_id) OVER (
            PARTITION BY route_id, stop_id
            ORDER BY arrival_time
        ) AS prev_trip_instance_id
    FROM stop_arrivals
)
SELECT
    route_id,
    route_short_name,
    route_long_name,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    trip_instance_id,
    prev_trip_instance_id,
    arrival_time,
    prev_arrival,
    EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) / 60.0 AS headway_minutes,
    hour_of_day,
    day_of_week,
    day_type,
    time_period
FROM with_prev
WHERE prev_arrival IS NOT NULL
  AND trip_instance_id != prev_trip_instance_id
  AND EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) > 0
  AND EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) < 7200;

CREATE INDEX IF NOT EXISTS idx_realtime_headway_stats_route_stop 
    ON realtime_headway_stats (route_id, stop_id, arrival_time);
CREATE INDEX IF NOT EXISTS idx_realtime_headway_stats_time_period 
    ON realtime_headway_stats (time_period, hour_of_day);

-- ============================================
-- SCHEDULE TIMES COMPARISON VIEW
-- ============================================
-- Compare scheduled vs actual arrival/departure times for on-time performance analysis
DROP MATERIALIZED VIEW IF EXISTS realtime_schedule_times;
CREATE MATERIALIZED VIEW realtime_schedule_times AS
SELECT
    d.trip_instance_id,
    d.trip_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    d.route_id,
    d.service_date,
    d.stop_sequence,
    d.stop_id,
    s.stop_name,
    ST_Y(s.stop_loc::geometry) AS stop_lat,
    ST_X(s.stop_loc::geometry) AS stop_lon,
    ts.arrival_time AS scheduled_arrival_interval,
    d.actual_arrival,
    d.actual_departure,
    d.arrival_delay_seconds,
    d.departure_delay_seconds,
    -- Delay in minutes
    d.arrival_delay_seconds / 60.0 AS delay_minutes,
    EXTRACT(hour FROM d.actual_arrival) AS hour_of_day,
    EXTRACT(dow FROM d.actual_arrival) AS day_of_week,
    CASE 
        WHEN EXTRACT(dow FROM d.actual_arrival) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM rt_trip_updates_deduped d
JOIN routes r ON r.route_id = d.route_id
LEFT JOIN stops s ON s.stop_id = d.stop_id
LEFT JOIN transit_stops ts 
    ON ts.trip_id = d.trip_id 
    AND ts.stop_sequence = d.stop_sequence
WHERE d.arrival_delay_seconds IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_realtime_schedule_times_trip 
    ON realtime_schedule_times (trip_instance_id, stop_sequence);
CREATE INDEX IF NOT EXISTS idx_realtime_schedule_times_route 
    ON realtime_schedule_times (route_id, service_date);
CREATE INDEX IF NOT EXISTS idx_realtime_schedule_times_hour 
    ON realtime_schedule_times (hour_of_day, day_of_week);

-- ============================================
-- LOAD VS DELAY VIEW
-- ============================================
-- Correlates vehicle occupancy_status with segment-level delay metrics
-- Uses latest occupancy per trip_instance_id and joins to realtime_delay_analysis
DROP MATERIALIZED VIEW IF EXISTS realtime_load_delay;
CREATE MATERIALIZED VIEW realtime_load_delay AS
WITH latest_occ AS (
    SELECT DISTINCT ON (vp.trip_instance_id)
        vp.trip_instance_id,
        vp.route_id,
        vp.entity_timestamp,
        vp.occupancy_status,
        CASE vp.occupancy_status
            WHEN 'EMPTY' THEN 0
            WHEN 'MANY_SEATS_AVAILABLE' THEN 1
            WHEN 'FEW_SEATS_AVAILABLE' THEN 2
            WHEN 'STANDING_ROOM_ONLY' THEN 3
            WHEN 'CRUSHED_STANDING_ROOM_ONLY' THEN 4
            WHEN 'FULL' THEN 5
            WHEN 'NOT_ACCEPTING_PASSENGERS' THEN 6
            ELSE NULL
        END AS occupancy_level
    FROM rt_vehicle_positions vp
    WHERE vp.occupancy_status IS NOT NULL
    ORDER BY vp.trip_instance_id, vp.entity_timestamp DESC
)
SELECT
    d.trip_instance_id,
    d.trip_id,
    d.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    d.service_date,
    d.from_stop_id,
    d.to_stop_id,
    d.from_stop_name,
    d.to_stop_name,
    d.segment_length_m,
    d.scheduled_seconds,
    d.actual_seconds,
    d.segment_delay_minutes,
    d.hour_of_day,
    d.day_of_week,
    d.day_type,
    d.time_period,
    d.seg_geom,
    lo.occupancy_status,
    lo.occupancy_level,
    CASE 
        WHEN lo.occupancy_level IS NULL THEN 'Unknown'
        WHEN lo.occupancy_level <= 1 THEN 'Plenty of seats'
        WHEN lo.occupancy_level = 2 THEN 'Few seats left'
        WHEN lo.occupancy_level = 3 THEN 'Standing room'
        WHEN lo.occupancy_level >= 4 THEN 'Crowded'
    END AS occupancy_bucket
FROM realtime_delay_analysis d
JOIN latest_occ lo 
    ON d.trip_instance_id = lo.trip_instance_id
LEFT JOIN routes r ON r.route_id = d.route_id
WHERE d.segment_delay_minutes BETWEEN -30 AND 60
  AND d.segment_length_m > 10;

CREATE INDEX IF NOT EXISTS idx_realtime_load_delay_route 
    ON realtime_load_delay (route_id, service_date);
CREATE INDEX IF NOT EXISTS idx_realtime_load_delay_occ 
    ON realtime_load_delay (occupancy_bucket);
CREATE INDEX IF NOT EXISTS idx_realtime_load_delay_geom 
    ON realtime_load_delay USING GIST (seg_geom);

