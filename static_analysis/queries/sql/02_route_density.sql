-- Route Density Query for QGIS
-- Shows all segments colored by number of BUS routes
-- Use gradient colors in QGIS to visualize density across the entire city

SELECT 
    rs.stop1_id || rs.stop2_id AS segment_id,
    COUNT(DISTINCT rs.route_id) AS num_routes,
    rs.seg_geom AS geom
FROM route_segments rs
JOIN routes r ON rs.route_id = r.route_id
WHERE rs.seg_geom IS NOT NULL
    AND r.route_type = '3'
GROUP BY rs.stop1_id, rs.stop2_id, rs.seg_geom
ORDER BY COUNT(DISTINCT rs.route_id) DESC;

