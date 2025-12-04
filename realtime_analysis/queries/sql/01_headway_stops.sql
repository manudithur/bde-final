-- Headway Analysis - Stops Query for QGIS
-- Shows BUS stops with headway statistics (bunching hotspots)

SELECT
    h.stop_id,
    h.stop_name,
    h.stop_lat,
    h.stop_lon,
    h.route_short_name,
    AVG(h.headway_minutes) AS avg_headway,
    COUNT(*) AS total_observations,
    SUM(CASE WHEN h.headway_minutes < 3 THEN 1 ELSE 0 END) AS bunched_count,
    (SUM(CASE WHEN h.headway_minutes < 3 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100) AS bunching_rate,
    ST_SetSRID(ST_MakePoint(h.stop_lon, h.stop_lat), 4326) AS geom
FROM realtime_headway_stats h
JOIN routes r ON h.route_id = r.route_id
WHERE r.route_type = '3'
GROUP BY h.stop_id, h.stop_name, h.stop_lat, h.stop_lon, h.route_short_name
HAVING COUNT(*) >= 5
ORDER BY bunching_rate DESC;


