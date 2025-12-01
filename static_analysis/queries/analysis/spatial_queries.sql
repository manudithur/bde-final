-- Spatial Analysis Queries for Vancouver GTFS Schedule Data
-- Adapted from Prague analysis with Vancouver-specific modifications

-- Route type distribution
WITH route_types(route_type, name) AS (
	SELECT '0', 'streetcar' UNION
	SELECT '1', 'subway' UNION
	SELECT '2', 'rail' UNION
	SELECT '3', 'bus' UNION
	SELECT '4', 'ferry' UNION
	SELECT '5', 'cable tram' UNION
	SELECT '6', 'aerial' UNION
	SELECT '7', 'funicular' UNION
	SELECT '11', 'trolley' UNION
	SELECT '12', 'monorail'
),
route_groups AS 
(
	SELECT 
	  route_type,
	  COUNT(*) AS qty,
	  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS perc
	FROM routes
	GROUP BY route_type
)
SELECT name, qty, perc 
FROM route_groups g JOIN route_types t ON g.route_type = t.route_type
ORDER BY perc DESC;

-- Route aggregation per segment
DROP MATERIALIZED VIEW IF EXISTS segment_route_density;
CREATE MATERIALIZED VIEW segment_route_density AS
SELECT
    stop1_id || stop2_id as segment_id,
    seg_geom,
    COUNT(DISTINCT route_id) AS num_routes
FROM
    route_segments
WHERE
    seg_geom IS NOT NULL
GROUP BY
    stop1_id,
    stop2_id,
    seg_geom;

-- Create Vancouver boundary polygon (approximate bounding box)
-- Greater Vancouver Area coordinates
DROP TABLE IF EXISTS vancouver_boundary;
CREATE TABLE vancouver_boundary (
    id integer PRIMARY KEY,
    name text,
    geom geometry(Polygon, 4326)
);

INSERT INTO vancouver_boundary (id, name, geom) VALUES
(1, 'Vancouver Area', 
 ST_MakePolygon(
   ST_MakeLine(ARRAY[
     ST_SetSRID(ST_MakePoint(-123.3, 49.0), 4326),  -- SW
     ST_SetSRID(ST_MakePoint(-122.3, 49.0), 4326),  -- SE
     ST_SetSRID(ST_MakePoint(-122.3, 49.5), 4326),  -- NE
     ST_SetSRID(ST_MakePoint(-123.3, 49.5), 4326),  -- NW
     ST_SetSRID(ST_MakePoint(-123.3, 49.0), 4326)   -- Close polygon
   ])
 )
);

-- Create football stadiums table-- Drop dependent objects first
DROP MATERIALIZED VIEW IF EXISTS trajectories_center_stadiums;
DROP TABLE IF EXISTS stadium_trip_intervals;
DROP TABLE IF EXISTS football_stadiums CASCADE;

CREATE TABLE football_stadiums (
    id serial PRIMARY KEY,
    name text NOT NULL,
    team text,
    latitude float,
    longitude float,
    geom geometry(Point, 4326)
);

-- Insert major Vancouver landmarks/stadiums
-- BC Place: 49.27596, -123.11274 (corrected coordinates)
-- Rogers Arena: 49.277821, -123.109085 (corrected coordinates)
-- Pacific Coliseum: 49.2848, -123.0390 (corrected coordinates)
INSERT INTO football_stadiums (name, team, latitude, longitude, geom) VALUES
('BC Place', 'Vancouver Whitecaps/BC Lions', 49.27596, -123.11274, 
 ST_SetSRID(ST_MakePoint(-123.11274, 49.27596), 4326)),
('Rogers Arena', 'Vancouver Canucks', 49.277821, -123.109085,
 ST_SetSRID(ST_MakePoint(-123.109085, 49.277821), 4326)),
('Pacific Coliseum', 'Vancouver Giants', 49.2848, -123.0390,
 ST_SetSRID(ST_MakePoint(-123.0390, 49.2848), 4326));

CREATE INDEX IF NOT EXISTS idx_football_stadiums_geom ON football_stadiums USING GIST (geom);

-- Trajectories from city center to each stadium
-- Vancouver city center approximately at 49.2827° N, 123.1207° W
CREATE MATERIALIZED VIEW trajectories_center_stadiums AS
SELECT
    s.name,
    s.team,
    ST_SetSRID(ST_MakePoint(-123.1207, 49.2827), 4326) AS center_geom,
    s.geom AS stadium_geom,
    ST_DistanceSphere(
        ST_SetSRID(ST_MakePoint(-123.1207, 49.2827), 4326),
        s.geom
    ) / 1000.0 AS distance_km,
    ST_MakeLine(
        ST_SetSRID(ST_MakePoint(-123.1207, 49.2827), 4326),
        s.geom
    ) AS geom
FROM football_stadiums s
ORDER BY distance_km;

-- Stadium transit access analysis
DROP TABLE IF EXISTS stadium_trip_intervals;
DROP MATERIALIZED VIEW IF EXISTS stadium_transit_access;

CREATE MATERIALIZED VIEW stadium_transit_access AS
SELECT 
  s.name AS stadium_name,
  s.team,
  -- Stops within 500m (using ST_DistanceSphere in meters)
  (SELECT COUNT(*) 
   FROM stops st 
   WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500) AS stops_500m,
  -- Unique routes serving nearby stops
  (SELECT COUNT(DISTINCT t.route_id)
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   JOIN trips t ON stt.trip_id = t.trip_id
   WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500) AS unique_routes_500m,
  -- SkyTrain routes
  (SELECT COUNT(DISTINCT t.route_id)
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   JOIN trips t ON stt.trip_id = t.trip_id
   JOIN routes r ON t.route_id = r.route_id
   WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500
     AND r.route_type = '1') AS skytrain_routes,
  -- Bus routes
  (SELECT COUNT(DISTINCT t.route_id)
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   JOIN trips t ON stt.trip_id = t.trip_id
   JOIN routes r ON t.route_id = r.route_id
   WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500
     AND r.route_type = '3') AS bus_routes,
  -- Nearest SkyTrain distance and station name
  COALESCE((SELECT MIN(ST_DistanceSphere(s.geom, st.stop_loc::geometry))
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   JOIN trips t ON stt.trip_id = t.trip_id
   JOIN routes r ON t.route_id = r.route_id
   WHERE r.route_type = '1'), 9999) AS nearest_skytrain_distance_m,
  COALESCE((SELECT st.stop_name
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   JOIN trips t ON stt.trip_id = t.trip_id
   JOIN routes r ON t.route_id = r.route_id
   WHERE r.route_type = '1'
   ORDER BY ST_DistanceSphere(s.geom, st.stop_loc::geometry)
   LIMIT 1), 'No SkyTrain nearby') AS nearest_skytrain_station,
  -- Daily trips count
  (SELECT COUNT(DISTINCT stt.trip_id)
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500) AS trips_per_day,
  -- Nearest stop distance
  COALESCE((SELECT MIN(ST_DistanceSphere(s.geom, st.stop_loc::geometry))
   FROM stops st), 9999) AS nearest_stop_distance_m
FROM football_stadiums s
ORDER BY 
  (SELECT COUNT(DISTINCT stt.trip_id)
   FROM stops st
   JOIN stop_times stt ON st.stop_id = stt.stop_id
   WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 500) DESC;

CREATE INDEX IF NOT EXISTS idx_stadium_transit_access_name ON stadium_transit_access (stadium_name);

-- Legacy table for backward compatibility (trips by time intervals)
CREATE TABLE stadium_trip_intervals (
    stadium_name TEXT,
    team TEXT,
    interv TEXT,
    trips_nearby INTEGER
);

INSERT INTO stadium_trip_intervals (stadium_name, team, interv, trips_nearby)
WITH instants AS (
  SELECT 
    s.name AS stadium_name,
    s.team,
    t.trip_id,
    getTimestamp(unnest(instants(t.trip))) AS instant_time
  FROM football_stadiums s
  JOIN scheduled_trips_mdb t ON ST_DWithin(s.geom, t.traj, 500)  -- Increased to 500m
), ranges AS (
  SELECT 
    stadium_name,
    team,
    trip_id,
    instant_time,
    (EXTRACT(HOUR FROM instant_time)::int / 2) * 2 AS range_start
  FROM instants
)
SELECT
  stadium_name,
  team,
  lpad(range_start::text, 2, '0') || ':00–' || lpad((range_start+1)::text, 2, '0') || ':59' AS interv,
  COUNT(DISTINCT trip_id) AS trips_nearby
FROM ranges
GROUP BY stadium_name, team, range_start
ORDER BY stadium_name, range_start;

-- Calculate average speeds of segments
DROP MATERIALIZED VIEW IF EXISTS schedule_speeds;
CREATE MATERIALIZED VIEW schedule_speeds AS
SELECT 
    s.route_id || stop1_sequence || stop2_sequence as id,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    seg_geom
FROM route_segments s
WHERE stop2_arrival_time <> stop1_arrival_time
  AND seg_length > 0
GROUP BY s.route_id, stop1_sequence, stop2_sequence, seg_geom;

-- Count segments with speeds over 50 km/h
DROP MATERIALIZED VIEW IF EXISTS segments_over_50kmh;
CREATE MATERIALIZED VIEW segments_over_50kmh AS
SELECT 
    ROW_NUMBER() OVER () AS id,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    seg_geom
FROM route_segments s
JOIN routes r ON s.route_id = r.route_id
WHERE stop2_arrival_time <> stop1_arrival_time 
  AND r.route_type = '3'
  AND seg_length > 0
GROUP BY s.route_id, stop1_sequence, stop2_sequence, seg_geom
HAVING AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) > 50;

-- Create indices for performance
CREATE INDEX IF NOT EXISTS idx_vancouver_boundary_geom ON vancouver_boundary USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_scheduled_trips_mdb_traj ON scheduled_trips_mdb USING GIST (traj);
CREATE INDEX IF NOT EXISTS idx_route_segments_geom ON route_segments USING GIST (seg_geom);

-- ============================================
-- ROUTE DUPLICATION ANALYSIS
-- ============================================
-- Identify routes with high duplication (routes that share many segments)
DROP MATERIALIZED VIEW IF EXISTS highly_duplicated_routes;
DROP MATERIALIZED VIEW IF EXISTS route_duplication;
CREATE MATERIALIZED VIEW route_duplication AS
WITH route_segment_pairs AS (
    SELECT DISTINCT
        rs1.route_id AS route1,
        rs2.route_id AS route2,
        COUNT(DISTINCT CONCAT(rs1.stop1_id, rs1.stop2_id)) AS shared_segments
    FROM route_segments rs1
    JOIN route_segments rs2 
        ON rs1.stop1_id = rs2.stop1_id 
        AND rs1.stop2_id = rs2.stop2_id
        AND rs1.route_id < rs2.route_id
    WHERE rs1.seg_geom IS NOT NULL
        AND rs2.seg_geom IS NOT NULL
    GROUP BY rs1.route_id, rs2.route_id
    HAVING COUNT(DISTINCT CONCAT(rs1.stop1_id, rs1.stop2_id)) >= 5
),
route_segment_counts AS (
    SELECT 
        route_id,
        COUNT(DISTINCT CONCAT(stop1_id, stop2_id)) AS total_segments
    FROM route_segments
    WHERE seg_geom IS NOT NULL
    GROUP BY route_id
)
SELECT 
    rsp.route1,
    rsp.route2,
    rsp.shared_segments,
    rsc1.total_segments AS route1_total_segments,
    rsc2.total_segments AS route2_total_segments,
    ROUND(rsp.shared_segments::numeric / GREATEST(rsc1.total_segments, rsc2.total_segments) * 100, 2) AS overlap_percentage
FROM route_segment_pairs rsp
JOIN route_segment_counts rsc1 ON rsp.route1 = rsc1.route_id
JOIN route_segment_counts rsc2 ON rsp.route2 = rsc2.route_id
ORDER BY overlap_percentage DESC, shared_segments DESC;

-- Routes with highest duplication (top candidates for elimination)
DROP MATERIALIZED VIEW IF EXISTS highly_duplicated_routes;
CREATE MATERIALIZED VIEW highly_duplicated_routes AS
SELECT 
    route_id,
    COUNT(*) AS num_duplicate_pairs,
    MAX(overlap_percentage) AS max_overlap_percentage,
    AVG(overlap_percentage) AS avg_overlap_percentage,
    SUM(shared_segments) AS total_shared_segments
FROM (
    SELECT route1 AS route_id, overlap_percentage, shared_segments FROM route_duplication
    UNION ALL
    SELECT route2 AS route_id, overlap_percentage, shared_segments FROM route_duplication
) combined
GROUP BY route_id
HAVING COUNT(*) >= 3  -- Routes that duplicate with at least 3 other routes
ORDER BY num_duplicate_pairs DESC, max_overlap_percentage DESC;

-- ============================================
-- ENHANCED SPEED ANALYSIS
-- ============================================
-- Speed statistics by route
DROP MATERIALIZED VIEW IF EXISTS route_speed_stats;
CREATE MATERIALIZED VIEW route_speed_stats AS
SELECT 
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT rs.trip_id) AS num_trips,
    COUNT(*) AS num_segments,
    ROUND((AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6))::numeric, 2) AS avg_speed_kmh,
    ROUND((MIN(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6))::numeric, 2) AS min_speed_kmh,
    ROUND((MAX(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6))::numeric, 2) AS max_speed_kmh,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6))::numeric, 2) AS median_speed_kmh
FROM route_segments rs
JOIN routes r ON rs.route_id = r.route_id
WHERE stop2_arrival_time <> stop1_arrival_time
    AND seg_length > 0
GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
HAVING COUNT(*) > 0
ORDER BY avg_speed_kmh DESC;

-- Segments with unusually high speeds (potential data quality issues or highways)
DROP MATERIALIZED VIEW IF EXISTS high_speed_segments;
CREATE MATERIALIZED VIEW high_speed_segments AS
SELECT 
    rs.route_id,
    r.route_short_name,
    r.route_type,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    COUNT(*) AS segment_count,
    ST_Collect(seg_geom) AS combined_geom
FROM route_segments rs
JOIN routes r ON rs.route_id = r.route_id
WHERE stop2_arrival_time <> stop1_arrival_time
    AND seg_length > 0
    AND seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6 > 60  -- Over 60 km/h
GROUP BY rs.route_id, r.route_short_name, r.route_type
HAVING COUNT(*) >= 3  -- At least 3 high-speed segments
ORDER BY speed_kmh DESC;

-- ============================================
-- ROUTE VISUALIZATION DATA
-- ============================================
-- Create simplified route lines for visualization
DROP MATERIALIZED VIEW IF EXISTS route_visualization;
CREATE MATERIALIZED VIEW route_visualization AS
SELECT 
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    CASE r.route_type
        WHEN '1' THEN 'Subway'
        WHEN '2' THEN 'Rail'
        WHEN '3' THEN 'Bus'
        WHEN '4' THEN 'Ferry'
        ELSE 'Other'
    END AS mode_name,
    COUNT(DISTINCT t.trip_id) AS num_trips,
    ST_Simplify(ST_Collect(t.traj), 0.0001) AS route_geometry  -- Simplify for visualization
FROM routes r
JOIN scheduled_trips_mdb t ON r.route_id = t.route_id
WHERE t.traj IS NOT NULL
GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type;

-- Create indices for new views
CREATE INDEX IF NOT EXISTS idx_route_visualization_geom ON route_visualization USING GIST (route_geometry);


