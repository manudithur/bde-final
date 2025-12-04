-- Speed Comparison with Geometry Query for QGIS
-- Shows BUS segments with speed comparison using delay analysis geometry

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
    d.seg_geom AS geom
FROM realtime_speed_comparison s
JOIN routes r ON s.route_id = r.route_id
LEFT JOIN realtime_delay_analysis d 
    ON s.trip_instance_id = d.trip_instance_id 
    AND s.stop_sequence = d.from_seq
WHERE r.route_type = '3'
    AND s.scheduled_speed_kmh IS NOT NULL
    AND s.actual_speed_kmh IS NOT NULL
    AND s.scheduled_speed_kmh > 0 AND s.scheduled_speed_kmh < 150
    AND s.actual_speed_kmh > 0 AND s.actual_speed_kmh < 150
    AND d.seg_geom IS NOT NULL
GROUP BY s.stop_id, s.next_stop_id, s.from_stop_name, s.to_stop_name, 
         s.route_short_name, d.seg_geom
ORDER BY avg_speed_difference DESC;


