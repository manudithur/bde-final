-- Delay Segments Query for QGIS
-- Shows BUS route segments with delay information

SELECT
    d.from_stop_id,
    d.to_stop_id,
    d.from_stop_name,
    d.to_stop_name,
    d.route_short_name,
    AVG(d.segment_delay_minutes) AS avg_delay_minutes,
    STDDEV(d.segment_delay_minutes) AS std_delay_minutes,
    COUNT(*) AS total_observations,
    d.seg_geom AS geom
FROM realtime_delay_analysis d
JOIN routes r ON d.route_id = r.route_id
WHERE r.route_type = '3'
    AND d.segment_delay_minutes BETWEEN -30 AND 60
    AND d.seg_geom IS NOT NULL
GROUP BY d.from_stop_id, d.to_stop_id, d.from_stop_name, d.to_stop_name, 
         d.route_short_name, d.seg_geom
HAVING COUNT(*) >= 3
ORDER BY avg_delay_minutes DESC;


