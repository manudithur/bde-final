-- Route Duplication Segments Query for QGIS
-- Shows all segments shared by multiple BUS routes (duplication hotspots)
-- Use gradient colors in QGIS to visualize duplication across the entire city

SELECT 
    rs.stop1_id || rs.stop2_id AS segment_id,
    COUNT(DISTINCT rs.route_id) AS num_routes_sharing,
    rs.seg_geom AS geom
FROM route_segments rs
JOIN routes r ON rs.route_id = r.route_id
WHERE r.route_type = '3'
    AND rs.seg_geom IS NOT NULL
GROUP BY rs.stop1_id, rs.stop2_id, rs.seg_geom
HAVING COUNT(DISTINCT rs.route_id) >= 2
ORDER BY COUNT(DISTINCT rs.route_id) DESC;

