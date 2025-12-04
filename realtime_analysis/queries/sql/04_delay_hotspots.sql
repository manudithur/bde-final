-- Delay Hotspots Query for QGIS
-- Shows segments with significant delays (hotspots)

SELECT
    d.from_stop_id,
    d.to_stop_id,
    d.from_stop_name,
    d.to_stop_name,
    d.route_short_name,
    AVG(d.segment_delay_minutes) AS avg_delay_minutes,
    COUNT(*) AS total_observations,
    d.seg_geom AS geom
FROM realtime_delay_analysis d
JOIN routes r ON d.route_id = r.route_id
WHERE r.route_type = '3'
    AND d.segment_delay_minutes > 1
    AND d.seg_geom IS NOT NULL
GROUP BY d.from_stop_id, d.to_stop_id, d.from_stop_name, d.to_stop_name, 
         d.route_short_name, d.seg_geom
HAVING COUNT(*) >= 3
ORDER BY avg_delay_minutes DESC;


