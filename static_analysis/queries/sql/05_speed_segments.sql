-- Speed Segments Query for QGIS
-- Shows BUS route segments with their calculated speeds

SELECT 
    rs.route_id || rs.stop1_sequence || rs.stop2_sequence AS segment_id,
    rs.route_id,
    r.route_short_name,
    AVG(rs.seg_length / EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) * 3.6) AS speed_kmh,
    rs.seg_geom AS geom
FROM route_segments rs
JOIN routes r ON rs.route_id = r.route_id
WHERE r.route_type = '3'
    AND rs.stop2_arrival_time <> rs.stop1_arrival_time
    AND rs.seg_length > 0
    AND rs.seg_geom IS NOT NULL
    AND (rs.seg_length / EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) * 3.6) > 0
    AND (rs.seg_length / EXTRACT(EPOCH FROM (rs.stop2_arrival_time - rs.stop1_arrival_time)) * 3.6) < 150
GROUP BY rs.route_id, rs.stop1_sequence, rs.stop2_sequence, rs.seg_geom, r.route_short_name;

