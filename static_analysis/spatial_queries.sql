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

-- Create football stadiums table
DROP TABLE IF EXISTS football_stadiums;
CREATE TABLE football_stadiums (
    id serial PRIMARY KEY,
    name text NOT NULL,
    team text,
    latitude float,
    longitude float,
    geom geometry(Point, 4326)
);

-- Insert major Vancouver landmarks/stadiums
INSERT INTO football_stadiums (name, team, latitude, longitude, geom) VALUES
('BC Place', 'Vancouver Whitecaps/BC Lions', 49.2778, -123.1088, 
 ST_SetSRID(ST_MakePoint(-123.1088, 49.2778), 4326)),
('Rogers Arena', 'Vancouver Canucks', 49.2778, -123.1088,
 ST_SetSRID(ST_MakePoint(-123.1088, 49.2778), 4326)),
('Pacific Coliseum', 'Vancouver Giants', 49.2850, -123.0300,
 ST_SetSRID(ST_MakePoint(-123.0300, 49.2850), 4326));

CREATE INDEX IF NOT EXISTS idx_football_stadiums_geom ON football_stadiums USING GIST (geom);

-- Trajectories from city center to each stadium
-- Vancouver city center approximately at 49.2827° N, 123.1207° W
DROP MATERIALIZED VIEW IF EXISTS trajectories_center_stadiums;
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

-- Identify trips near football stadiums by time intervals
DROP TABLE IF EXISTS stadium_trip_intervals;
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
  JOIN scheduled_trips_mdb t ON ST_DWithin(s.geom, t.traj, 200)
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
    route_id || stop1_sequence || stop2_sequence as id,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    seg_geom
FROM route_segments s
WHERE stop2_arrival_time <> stop1_arrival_time
  AND seg_length > 0
GROUP BY route_id, stop1_sequence, stop2_sequence, seg_geom;

-- Count segments with speeds over 50 km/h
DROP MATERIALIZED VIEW IF EXISTS segments_over_50kmh;
CREATE MATERIALIZED VIEW segments_over_50kmh AS
SELECT 
    ROW_NUMBER() OVER () AS id,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    seg_geom
FROM route_segments s
JOIN routes USING (route_id)
WHERE stop2_arrival_time <> stop1_arrival_time 
  AND route_type = '3'
  AND seg_length > 0
GROUP BY route_id, stop1_sequence, stop2_sequence, seg_geom
HAVING AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) > 50;

-- Generate 1km x 1km grid over Vancouver
DROP TABLE IF EXISTS vancouver_grid CASCADE;
CREATE TABLE vancouver_grid AS
SELECT
  row_number() OVER () AS id,
  (ST_SquareGrid(
      1000,
      ST_Transform(geom, 3857)  -- Web Mercator for grid generation
   )).geom
FROM vancouver_boundary;

DROP TABLE IF EXISTS vancouver_grid_clipped CASCADE;
CREATE TABLE vancouver_grid_clipped AS
SELECT
  g.id,
  ST_Intersection(g.geom, ST_Transform(m.geom, 3857)) AS geom
FROM vancouver_grid g
JOIN vancouver_boundary m
  ON ST_Intersects(g.geom, ST_Transform(m.geom, 3857))
WHERE NOT ST_IsEmpty(ST_Intersection(g.geom, ST_Transform(m.geom, 3857)));

-- Count trips per grid cell
DROP TABLE IF EXISTS grid_trip_counts CASCADE;
CREATE TABLE grid_trip_counts AS
SELECT
  g.id AS grid_id,
  COUNT(DISTINCT t.trip_id) AS trips_count,
  ST_Transform(g.geom, 4326) AS geom
FROM vancouver_grid_clipped g
LEFT JOIN scheduled_trips_mdb t
  ON ST_Intersects(ST_Transform(t.traj, 3857), g.geom)
GROUP BY g.id, g.geom
ORDER BY g.id;

-- Create indices for performance
CREATE INDEX IF NOT EXISTS idx_vancouver_boundary_geom ON vancouver_boundary USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_scheduled_trips_mdb_traj ON scheduled_trips_mdb USING GIST (traj);
CREATE INDEX IF NOT EXISTS idx_route_segments_geom ON route_segments USING GIST (seg_geom);


