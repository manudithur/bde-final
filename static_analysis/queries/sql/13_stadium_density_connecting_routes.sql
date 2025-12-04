-- Stadium to High-Density Areas Connecting Routes Query for QGIS
-- Shows complete routes (all segments) that connect stadiums to high-density population areas
-- Use this layer in QGIS along with stadiums and population_density layers

WITH high_density_areas AS (
    -- Identify high-density population areas (top quartile)
    SELECT 
        id,
        geom,
        population_density,
        CAST(pop AS DOUBLE PRECISION) AS population,
        CAST(a AS DOUBLE PRECISION) AS area_km2
    FROM population_density
    WHERE geom IS NOT NULL
        AND population_density IS NOT NULL
        AND population_density > (
            SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY population_density)
            FROM population_density
            WHERE population_density IS NOT NULL
        )
),
routes_near_stadiums AS (
    -- Find routes that have stops within 600m of stadiums (using spatial index)
    SELECT DISTINCT
        s.id AS stadium_id,
        s.name AS stadium_name,
        s.team,
        t.route_id
    FROM football_stadiums s
    JOIN stops st 
        ON ST_DWithin(s.geom::geography, st.stop_loc::geography, 600)
    JOIN stop_times stt ON st.stop_id = stt.stop_id
    JOIN trips t ON stt.trip_id = t.trip_id
    JOIN routes r ON t.route_id = r.route_id
    WHERE r.route_type = '3'
),
routes_through_dense_areas AS (
    -- Find routes that pass through high-density areas
    SELECT DISTINCT
        hda.id AS density_area_id,
        hda.population_density,
        rs.route_id
    FROM high_density_areas hda
    JOIN route_segments rs 
        ON ST_Intersects(rs.seg_geom, hda.geom)
    JOIN routes r ON rs.route_id = r.route_id
    WHERE r.route_type = '3'
),
connecting_route_ids AS (
    -- Find routes that both serve stadiums AND pass through dense areas
    SELECT DISTINCT
        ras.stadium_id,
        ras.stadium_name,
        ras.team,
        rda.density_area_id,
        rda.population_density,
        ras.route_id
    FROM routes_near_stadiums ras
    JOIN routes_through_dense_areas rda 
        ON ras.route_id = rda.route_id
),
route_geometries AS (
    -- Pre-compute complete route geometries once (like 01_route_visualization.sql)
    SELECT 
        rs.route_id,
        ST_Collect(rs.seg_geom) AS route_geometry,
        ST_Length(ST_Collect(rs.seg_geom)::geography) / 1000 AS total_length_km,
        COUNT(DISTINCT rs.stop1_id || rs.stop2_id) AS num_segments
    FROM route_segments rs
    JOIN routes r ON rs.route_id = r.route_id
    WHERE rs.seg_geom IS NOT NULL
        AND r.route_type = '3'
    GROUP BY rs.route_id
),
complete_connecting_routes AS (
    -- Join connecting routes with pre-computed geometries
    SELECT 
        cri.stadium_id,
        cri.stadium_name,
        cri.team,
        cri.density_area_id,
        cri.population_density,
        cri.route_id,
        r.route_short_name,
        r.route_long_name,
        COALESCE(rg.num_segments, 0) AS num_segments,
        COALESCE(rg.total_length_km, 0) AS total_route_length_km,
        rg.route_geometry
    FROM connecting_route_ids cri
    JOIN routes r ON cri.route_id = r.route_id
    LEFT JOIN route_geometries rg ON cri.route_id = rg.route_id
    WHERE rg.route_geometry IS NOT NULL
)
SELECT 
    stadium_id,
    stadium_name,
    team,
    density_area_id,
    population_density,
    route_id,
    route_short_name,
    route_long_name,
    num_segments,
    total_route_length_km,
    route_geometry AS geom
FROM complete_connecting_routes
ORDER BY stadium_name, population_density DESC, total_route_length_km;

