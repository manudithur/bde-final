-- Route Visualization Query for QGIS
-- Shows all BUS routes with their geometries
-- Self-contained: creates route visualization from base tables

WITH route_trips AS (
    SELECT 
        r.route_id,
        COUNT(DISTINCT t.trip_id) AS num_trips
    FROM routes r
    JOIN trips t ON r.route_id = t.route_id
    WHERE r.route_type = '3'
    GROUP BY r.route_id
),
route_geometries AS (
    SELECT 
        rs.route_id,
        ST_Collect(rs.seg_geom) AS route_geometry
    FROM route_segments rs
    JOIN routes r ON rs.route_id = r.route_id
    WHERE r.route_type = '3'
        AND rs.seg_geom IS NOT NULL
    GROUP BY rs.route_id
)
SELECT 
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COALESCE(rt.num_trips, 0) AS num_trips,
    rg.route_geometry AS geom
FROM routes r
LEFT JOIN route_trips rt ON r.route_id = rt.route_id
LEFT JOIN route_geometries rg ON r.route_id = rg.route_id
WHERE r.route_type = '3'
    AND rg.route_geometry IS NOT NULL
ORDER BY rt.num_trips DESC NULLS LAST;

