-- Create trip_segments table directly from actual_trips and stops
-- This creates segments between consecutive stops without needing trip_stops table

CREATE TABLE trip_segments AS
WITH trip_stop_samples AS (
    -- Sample actual trips at stop locations using MobilityDB functions
    SELECT 
        a.trip_id AS actual_trip_id,
        s.stop_id,
        s.stop_name,
        nearestApproachInstant(a.trip, ST_Transform(s.stop_loc::geometry, 3059)) AS stop_instant,
        nearestApproachDistance(a.trip, ST_Transform(s.stop_loc::geometry, 3059)) AS approach_distance
    FROM actual_trips a
    CROSS JOIN stops s
    WHERE nearestApproachDistance(a.trip, ST_Transform(s.stop_loc::geometry, 3059)) < 10
),
trip_stops_with_times AS (
    SELECT 
        actual_trip_id,
        stop_id,
        stop_name,
        getTimestamp(stop_instant) AS actual_time,
        ST_Transform(getValue(stop_instant), 4326) AS trip_geom,
        ROW_NUMBER() OVER (PARTITION BY actual_trip_id ORDER BY getTimestamp(stop_instant)) as stop_sequence
    FROM trip_stop_samples
),
segments_with_lag AS (
    SELECT 
        actual_trip_id,
        stop_id AS end_stop_id,
        actual_time AS end_time_actual,
        stop_sequence,
        -- Use LAG to get the previous stop's information
        LAG(stop_id) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence) AS start_stop_id,
        LAG(actual_time) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence) AS start_time_actual
    FROM trip_stops_with_times
)
SELECT *
FROM segments_with_lag
WHERE start_stop_id IS NOT NULL;