WITH scheduled_headways AS (
    WITH trip_times AS (
        SELECT
            t.route_id,
            r.route_short_name,
            ts.stop_id,
            ts.arrival_time
        FROM trips t
        JOIN transit_stops ts ON ts.trip_id = t.trip_id
        JOIN routes r ON r.route_id = t.route_id
        WHERE r.route_type = '3'
          AND ts.arrival_time IS NOT NULL
    ),
    with_prev AS (
        SELECT
            *,
            LAG(arrival_time) OVER (
                PARTITION BY route_id, stop_id
                ORDER BY arrival_time
            ) AS prev_arrival
        FROM trip_times
    )
    SELECT
        route_id,
        route_short_name,
        stop_id,
        AVG(EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) / 60.0) AS scheduled_headway_minutes
    FROM with_prev
    WHERE prev_arrival IS NOT NULL
      AND EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) > 0
      AND EXTRACT(EPOCH FROM (arrival_time - prev_arrival)) < 7200
    GROUP BY route_id, route_short_name, stop_id
),
actual_headways AS (
    SELECT
        h.route_id,
        h.route_short_name,
        h.stop_id,
        h.stop_name,
        h.stop_lat,
        h.stop_lon,
        h.headway_minutes
    FROM realtime_headway_stats h
    JOIN routes r ON h.route_id = r.route_id
    WHERE r.route_type = '3'  -- BUS
)
SELECT
    a.route_id,
    a.route_short_name,
    a.stop_id,
    a.stop_name,
    a.stop_lat,
    a.stop_lon,
    -- Observed headway stats
    COUNT(*) AS observations,
    AVG(a.headway_minutes) AS avg_actual_headway_min,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a.headway_minutes) AS median_actual_headway_min,
    STDDEV(a.headway_minutes) AS std_actual_headway_min,
    -- Scheduled comparison
    sh.scheduled_headway_minutes,
    (AVG(a.headway_minutes) - sh.scheduled_headway_minutes) AS headway_delta_min,
    -- Service reliability signals
    (SUM(CASE WHEN a.headway_minutes < 3 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*),0) * 100.0) AS bunching_rate_pct,
    (SUM(
        CASE 
            WHEN sh.scheduled_headway_minutes IS NOT NULL AND a.headway_minutes > sh.scheduled_headway_minutes * 1.8 THEN 1
            WHEN sh.scheduled_headway_minutes IS NULL AND a.headway_minutes > 20 THEN 1
            ELSE 0
        END
     )::FLOAT / NULLIF(COUNT(*),0) * 100.0) AS gap_rate_pct,
    ST_SetSRID(ST_MakePoint(a.stop_lon, a.stop_lat), 4326) AS geom
FROM actual_headways a
LEFT JOIN scheduled_headways sh
    ON sh.route_id = a.route_id
   AND sh.stop_id = a.stop_id
GROUP BY
    a.route_id,
    a.route_short_name,
    a.stop_id,
    a.stop_name,
    a.stop_lat,
    a.stop_lon,
    sh.scheduled_headway_minutes
HAVING COUNT(*) >= 3
ORDER BY headway_delta_min DESC NULLS LAST, observations DESC;

