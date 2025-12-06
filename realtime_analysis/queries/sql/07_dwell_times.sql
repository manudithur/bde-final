-- Dwell Times Query for QGIS
-- Shows BUS stops with average dwell durations

SELECT
    dt.stop_id,
    dt.stop_name,
    dt.stop_lat,
    dt.stop_lon,
    dt.route_short_name,
    AVG(dt.dwell_minutes) AS avg_dwell_minutes,
    STDDEV(dt.dwell_minutes) AS std_dwell_minutes,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY dt.dwell_minutes) AS p90_dwell_minutes,
    COUNT(*) AS total_observations,
    ST_SetSRID(ST_MakePoint(dt.stop_lon, dt.stop_lat), 4326) AS geom
FROM realtime_dwell_times dt
GROUP BY dt.stop_id, dt.stop_name, dt.stop_lat, dt.stop_lon, dt.route_short_name
HAVING COUNT(*) >= 3
ORDER BY avg_dwell_minutes DESC;


