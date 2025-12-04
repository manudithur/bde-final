-- Route Duplication Pairs Query for QGIS
-- Shows all route pairs with overlap (non-spatial, for attribute table)

SELECT 
    rd.route1,
    rd.route2,
    r1.route_short_name AS route1_name,
    r2.route_short_name AS route2_name,
    rd.shared_segments,
    rd.route1_total_segments,
    rd.route2_total_segments,
    rd.overlap_percentage
FROM route_duplication rd
JOIN routes r1 ON rd.route1 = r1.route_id
JOIN routes r2 ON rd.route2 = r2.route_id
WHERE r1.route_type = '3' AND r2.route_type = '3'
ORDER BY rd.overlap_percentage DESC, rd.shared_segments DESC;

