-- Speed Comparison Segments Query for QGIS
-- Shows BUS segments with scheduled vs actual speed comparison

SELECT
    s.stop_id,
    s.next_stop_id,
    s.from_stop_name,
    s.to_stop_name,
    s.route_short_name,
    AVG(s.scheduled_speed_kmh) AS avg_scheduled_speed,
    AVG(s.actual_speed_kmh) AS avg_actual_speed,
    AVG(s.actual_speed_kmh - s.scheduled_speed_kmh) AS avg_speed_difference,
    COUNT(*) AS total_observations,
    -- Note: This query doesn't include geometry as realtime_speed_comparison 
    -- doesn't have segment geometry. Use delay_segments for spatial visualization.
    NULL::geometry AS geom
FROM realtime_speed_comparison s
JOIN routes r ON s.route_id = r.route_id
WHERE r.route_type = '3'
    AND s.scheduled_speed_kmh IS NOT NULL
    AND s.actual_speed_kmh IS NOT NULL
    AND s.scheduled_speed_kmh > 0 AND s.scheduled_speed_kmh < 150
    AND s.actual_speed_kmh > 0 AND s.actual_speed_kmh < 150
GROUP BY s.stop_id, s.next_stop_id, s.from_stop_name, s.to_stop_name, s.route_short_name
ORDER BY avg_speed_difference DESC;


