-- Population Density vs Transit Coverage Query for QGIS
-- Shows population density areas with transit route density

WITH population_areas AS (
    SELECT 
        id,
        geom,
        population_density,
        CAST(a AS DOUBLE PRECISION) AS area_km2
    FROM population_density
    WHERE geom IS NOT NULL
        AND population_density IS NOT NULL
        AND a IS NOT NULL
),
bus_route_segments AS (
    SELECT DISTINCT
        rs.stop1_id || rs.stop2_id AS segment_id,
        rs.seg_geom
    FROM route_segments rs
    JOIN routes r ON rs.route_id = r.route_id
    WHERE r.route_type = '3'
        AND rs.seg_geom IS NOT NULL
)
SELECT 
    pa.id,
    pa.population_density,
    COUNT(DISTINCT brs.segment_id) AS num_segments,
    COALESCE(SUM(ST_Length(ST_Intersection(pa.geom, brs.seg_geom)::geography)) / 1000, 0) AS route_length_km,
    pa.area_km2,
    COALESCE(SUM(ST_Length(ST_Intersection(pa.geom, brs.seg_geom)::geography)) / 1000, 0) / 
        NULLIF(CAST(pa.area_km2 AS DOUBLE PRECISION), 0) AS route_density_km_per_km2,
    pa.geom AS geom
FROM population_areas pa
LEFT JOIN bus_route_segments brs 
    ON ST_Intersects(pa.geom, brs.seg_geom)
GROUP BY pa.id, pa.population_density, pa.geom, pa.area_km2;

