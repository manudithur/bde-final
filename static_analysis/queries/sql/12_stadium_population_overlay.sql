-- Stadium vs Population Density Connectivity Query for QGIS
-- Shows how stadiums connect to high-density population areas via transit
-- Requires football_stadiums table (created via run_sql helper)

WITH high_density_areas AS (
    -- Identify high-density population areas (top quartile or above threshold)
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
        s.geom AS stadium_geom,
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
        hda.population,
        hda.area_km2,
        hda.geom,
        rs.route_id
    FROM high_density_areas hda
    JOIN route_segments rs 
        ON ST_Intersects(rs.seg_geom, hda.geom)
    JOIN routes r ON rs.route_id = r.route_id
    WHERE r.route_type = '3'
),
route_statistics AS (
    -- Pre-compute route statistics to avoid subqueries
    SELECT 
        route_id,
        COUNT(DISTINCT (stop1_id || stop2_id)) AS total_segments,
        COALESCE(SUM(ST_Length(seg_geom::geography)) / 1000, 0) AS total_length_km
    FROM route_segments
    WHERE seg_geom IS NOT NULL
    GROUP BY route_id
),
stadium_to_density_connectivity AS (
    -- Find routes that both serve stadiums AND pass through dense areas
    SELECT 
        ras.stadium_id,
        ras.stadium_name,
        ras.team,
        ras.stadium_geom,
        rda.density_area_id,
        rda.population_density,
        rda.population,
        rda.area_km2,
        ras.route_id,
        COALESCE(rs_stats.total_segments, 0) AS connecting_segments,
        COALESCE(rs_stats.total_length_km, 0) AS direct_route_length_km,
        ST_DistanceSphere(ras.stadium_geom, ST_Centroid(rda.geom)) AS distance_m
    FROM routes_near_stadiums ras
    JOIN routes_through_dense_areas rda 
        ON ras.route_id = rda.route_id
    LEFT JOIN route_statistics rs_stats 
        ON ras.route_id = rs_stats.route_id
),
stadium_connectivity_stats AS (
    -- Aggregate connectivity metrics per stadium (only connected areas)
    SELECT 
        stadium_id,
        stadium_name,
        team,
        stadium_geom,
        COUNT(DISTINCT CASE WHEN connecting_segments > 0 THEN density_area_id END) AS num_high_density_areas_connected,
        SUM(CASE WHEN connecting_segments > 0 THEN population ELSE 0 END) AS total_population_connected,
        AVG(CASE WHEN connecting_segments > 0 THEN population_density END) AS avg_density_connected,
        MAX(CASE WHEN connecting_segments > 0 THEN population_density END) AS max_density_connected,
        SUM(connecting_segments) AS total_connecting_segments,
        SUM(direct_route_length_km) AS total_route_length_km,
        AVG(distance_m) AS avg_distance_to_dense_areas_m,
        MIN(distance_m) AS nearest_dense_area_distance_m
    FROM stadium_to_density_connectivity
    GROUP BY stadium_id, stadium_name, team, stadium_geom
),
stadium_transit_coverage AS (
    -- Get overall transit coverage around stadium (600m buffer)
    SELECT 
        s.id AS stadium_id,
        COUNT(DISTINCT (rs.stop1_id || rs.stop2_id)) AS num_segments_near_stadium,
        COALESCE(SUM(ST_Length(
            ST_Intersection(
                ST_Buffer(s.geom::geography, 600)::geometry,
                rs.seg_geom
            )::geography
        )) / 1000, 0) AS route_length_km_near_stadium
    FROM football_stadiums s
    LEFT JOIN route_segments rs 
        ON ST_Intersects(ST_Buffer(s.geom::geography, 600)::geometry, rs.seg_geom)
    LEFT JOIN routes r ON rs.route_id = r.route_id
    WHERE (r.route_type = '3' OR r.route_type IS NULL)
    GROUP BY s.id
)
SELECT 
    scs.stadium_name,
    scs.team,
    COALESCE(scs.num_high_density_areas_connected, 0) AS num_high_density_areas_connected,
    COALESCE(scs.total_population_connected, 0) AS total_population_connected,
    COALESCE(scs.avg_density_connected, 0) AS avg_density_connected,
    COALESCE(scs.max_density_connected, 0) AS max_density_connected,
    COALESCE(scs.total_connecting_segments, 0) AS total_connecting_segments,
    COALESCE(scs.total_route_length_km, 0) AS total_route_length_km,
    COALESCE(scs.avg_distance_to_dense_areas_m, 0) AS avg_distance_to_dense_areas_m,
    COALESCE(scs.nearest_dense_area_distance_m, 0) AS nearest_dense_area_distance_m,
    COALESCE(stc.num_segments_near_stadium, 0) AS num_segments_near_stadium,
    COALESCE(stc.route_length_km_near_stadium, 0) AS route_length_km_near_stadium,
    -- Connectivity score: segments per million people in connected dense areas
    CASE 
        WHEN scs.total_population_connected > 0 THEN
            scs.total_connecting_segments / (scs.total_population_connected / 1000000.0)
        ELSE 0
    END AS connectivity_score_segments_per_million,
    scs.stadium_geom AS geom
FROM stadium_connectivity_stats scs
LEFT JOIN stadium_transit_coverage stc ON scs.stadium_id = stc.stadium_id
ORDER BY scs.total_population_connected DESC, scs.num_high_density_areas_connected DESC;

