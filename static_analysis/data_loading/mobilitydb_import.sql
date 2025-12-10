-- MobilityDB Importer for Vancouver GTFS Schedule Data
-- Adapted from Prague GTFS Analysis for Vancouver transit system

-- Ensure MobilityDB extension is installed
CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

-- Create transit_stops table (renamed from trip_stops for uniqueness)
DROP TABLE IF EXISTS transit_stops;
CREATE TABLE transit_stops (
  trip_id text,
  stop_sequence integer,
  num_stops integer,
  route_id text,
  service_id text,
  shape_id text,
  stop_id text,
  arrival_time interval,
  perc float
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting transit_stops';x
  INSERT INTO transit_stops (trip_id, stop_sequence, num_stops, route_id, service_id, shape_id, stop_id, arrival_time)
  SELECT t.trip_id, stop_sequence,
         MAX(stop_sequence) OVER (PARTITION BY t.trip_id),
         route_id, service_id, t.shape_id, st.stop_id, arrival_time
  FROM trips t JOIN stop_times st ON t.trip_id = st.trip_id;
END;
$$;

-- Create trajectories table from shapes (if not exists)
-- gtfs-to-sql uses shape_pt_loc (geography) instead of separate lat/lon columns
DROP TABLE IF EXISTS trajectories CASCADE;
CREATE TABLE trajectories AS
SELECT 
  shape_id,
  ST_MakeLine(
    shape_pt_loc::geometry 
    ORDER BY shape_pt_sequence
  ) AS traj
FROM shapes
WHERE shape_id IS NOT NULL
GROUP BY shape_id;

-- For trips without shape_id, create stop-to-stop line segments
-- This creates simple trajectories from consecutive stops
-- gtfs-to-sql uses stop_loc (geography) column
DROP TABLE IF EXISTS stop_based_trajectories CASCADE;
CREATE TABLE stop_based_trajectories AS
SELECT DISTINCT
  t.trip_id,
  t.shape_id,
  ST_MakeLine(
    s.stop_loc::geometry 
    ORDER BY st.stop_sequence
  ) AS traj
FROM trips t
JOIN stop_times st ON t.trip_id = st.trip_id
JOIN stops s ON st.stop_id = s.stop_id
WHERE (t.shape_id IS NULL OR t.shape_id = '')
GROUP BY t.trip_id, t.shape_id;

-- Create spatial index on trajectories
CREATE INDEX IF NOT EXISTS idx_trajectories_traj ON trajectories USING GIST (traj);
CREATE INDEX IF NOT EXISTS idx_trajectories_shape_id ON trajectories (shape_id);

-- gtfs-to-sql already creates stop_loc as geography(Point,4326)
-- No need to add it, but we'll use it as geometry in queries
-- Create index if it doesn't exist (gtfs-to-sql may have created it)
CREATE INDEX IF NOT EXISTS idx_stops_loc ON stops USING GIST (stop_loc);

DO $$
BEGIN
  RAISE NOTICE '...Updating transit_stops with percentages (trips with shapes)';
  -- Update percentages for trips with shape_id using trajectories
  -- Convert stop_loc from geography to geometry for ST_LineLocatePoint
  UPDATE transit_stops t
  SET perc = CASE
    WHEN stop_sequence =  1 THEN 0::float
    WHEN stop_sequence =  num_stops THEN 1.0::float
    ELSE ST_LineLocatePoint(g.traj, s.stop_loc::geometry)
  END
  FROM trajectories g, stops s
  WHERE t.shape_id = g.shape_id
    AND t.shape_id IS NOT NULL
    AND t.shape_id != ''
    AND t.stop_id = s.stop_id;
  
  RAISE NOTICE '...Updating transit_stops with percentages (trips without shapes)';
  -- Update percentages for trips without shape_id using stop-based trajectories
  UPDATE transit_stops t
  SET perc = CASE
    WHEN stop_sequence =  1 THEN 0::float
    WHEN stop_sequence =  num_stops THEN 1.0::float
    ELSE ST_LineLocatePoint(sbt.traj, s.stop_loc::geometry)
  END
  FROM stop_based_trajectories sbt, stops s
  WHERE t.trip_id = sbt.trip_id
    AND (t.shape_id IS NULL OR t.shape_id = '')
    AND t.stop_id = s.stop_id;
END;
$$;

-- Create route_segments table (renamed from trip_segs)
DROP TABLE IF EXISTS route_segments CASCADE;
CREATE TABLE route_segments (
  trip_id text,
  route_id text,
  service_id text,
  stop1_sequence integer,
  stop2_sequence integer,
  num_stops integer,
  stop1_id text,
  stop2_id text,
  shape_id text,
  stop1_arrival_time interval,
  stop2_arrival_time interval,
  perc1 float,
  perc2 float,
  seg_geom geometry,
  seg_length float,
  no_points integer,
  PRIMARY KEY (trip_id, stop1_sequence)
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting route_segments';
  INSERT INTO route_segments (trip_id, route_id, service_id, stop1_sequence, stop2_sequence,
                         num_stops, stop1_id, stop2_id,
                         shape_id, stop1_arrival_time, stop2_arrival_time, perc1, perc2)
  WITH temp AS (
    SELECT t.trip_id, t.route_id, t.service_id, t.stop_sequence,
           LEAD(stop_sequence) OVER w AS stop_sequence2,
           MAX(stop_sequence) OVER (PARTITION BY trip_id),
           t.stop_id, LEAD(t.stop_id) OVER w,
           t.shape_id, t.arrival_time, LEAD(arrival_time) OVER w,
           t.perc, LEAD(perc) OVER w
    FROM transit_stops t
    WINDOW w AS (PARTITION BY trip_id ORDER BY stop_sequence)
  )
  SELECT * FROM temp WHERE stop_sequence2 IS NOT null;
END;
$$;

DO $$
BEGIN
  RAISE NOTICE '...Updating route_segments geometry (trips with shapes)';
  -- Update geometry for trips with shape_id
  UPDATE route_segments t
  SET seg_geom = CASE
    WHEN perc1 > perc2 THEN seg_geom
    ELSE ST_LineSubstring(g.traj, perc1, perc2)
  END
  FROM trajectories g
  WHERE t.shape_id = g.shape_id
    AND t.shape_id IS NOT NULL
    AND t.shape_id != '';
  
  RAISE NOTICE '...Updating route_segments geometry (trips without shapes)';
  -- Update geometry for trips without shape_id using stop-based trajectories
  UPDATE route_segments t
  SET seg_geom = CASE
    WHEN perc1 > perc2 THEN seg_geom
    ELSE ST_LineSubstring(sbt.traj, perc1, perc2)
  END
  FROM stop_based_trajectories sbt
  WHERE t.trip_id = sbt.trip_id
    AND (t.shape_id IS NULL OR t.shape_id = '');
END;
$$;

-- Remove segments with NULL geometry
DELETE FROM route_segments
WHERE trip_id IN (
  SELECT trip_id
  FROM route_segments
  WHERE seg_geom IS NULL
);

DO $$
BEGIN
  RAISE NOTICE '...Updating route_segments length and point count';
  UPDATE route_segments t
  SET seg_length = ST_Length(seg_geom::geography), no_points = ST_NumPoints(seg_geom);
END;
$$;

-- Create trip_points table
DROP TABLE IF EXISTS trip_points;
CREATE TABLE trip_points (
  trip_id text,
  route_id text,
  service_id text,
  stop1_sequence integer,
  point_sequence integer,
  point_geom geometry,
  point_arrival_time interval,
  PRIMARY KEY (trip_id, stop1_sequence, point_sequence)
);

-- Insert trip_points in batches to avoid memory issues
-- Process by trip_id to break up the work
CREATE INDEX IF NOT EXISTS idx_route_segments_trip_id ON route_segments(trip_id, stop1_sequence);

DO $$
DECLARE
  trip_rec RECORD;
  batch_count INTEGER := 0;
  total_trips INTEGER;
BEGIN
  -- Get total count of unique trips
  SELECT COUNT(DISTINCT trip_id) INTO total_trips FROM route_segments;
  RAISE NOTICE 'Total unique trips to process: %', total_trips;
  
  -- Process one trip at a time to avoid memory issues
  FOR trip_rec IN 
    SELECT DISTINCT trip_id FROM route_segments ORDER BY trip_id
  LOOP
    batch_count := batch_count + 1;
    
    IF batch_count % 100 = 0 THEN
      RAISE NOTICE 'Processing trip % of %: %', batch_count, total_trips, trip_rec.trip_id;
    END IF;
    
    INSERT INTO trip_points (trip_id, route_id, service_id, stop1_sequence,
                             point_sequence, point_geom, point_arrival_time)
    WITH temp1 AS (
      SELECT trip_id, route_id, service_id, stop1_sequence,
             stop2_sequence, num_stops, stop1_arrival_time, stop2_arrival_time, seg_length,
             (dp).path[1] AS point_sequence, no_points, (dp).geom as point_geom
      FROM route_segments, ST_DumpPoints(seg_geom) AS dp
      WHERE route_segments.trip_id = trip_rec.trip_id
    ),
    temp2 AS (
      SELECT trip_id, route_id, service_id, stop1_sequence,
             stop1_arrival_time, stop2_arrival_time, seg_length,  point_sequence,
             no_points, point_geom
      FROM temp1
      WHERE point_sequence <> no_points OR stop2_sequence = num_stops
    ),
    temp3 AS (
      SELECT trip_id, route_id, service_id, stop1_sequence,
             stop1_arrival_time, stop2_arrival_time, point_sequence, no_points, point_geom, seg_length,
             CASE 
               WHEN seg_length > 0 THEN ST_Length(ST_MakeLine(array_agg(point_geom) OVER w)) / seg_length
               ELSE 0.0
             END AS perc
      FROM temp2
      WHERE seg_length > 0  -- Filter out zero-length segments
      WINDOW w AS (PARTITION BY trip_id, service_id, stop1_sequence ORDER BY point_sequence)
    )
    SELECT trip_id, route_id, service_id, stop1_sequence,
           point_sequence, point_geom,
           CASE
             WHEN point_sequence = 1 THEN stop1_arrival_time
             WHEN point_sequence = no_points THEN stop2_arrival_time
             ELSE stop1_arrival_time + ((stop2_arrival_time - stop1_arrival_time) * perc)
           END AS point_arrival_time
    FROM temp3;
  END LOOP;
  
  RAISE NOTICE '...Finished inserting trip_points for % trips', batch_count;
END;
$$;

-- Create service_dates table from calendar and calendar_dates
-- gtfs-to-sql uses exception_type_v enum, cast to text for comparison
DROP TABLE IF EXISTS service_dates;
CREATE TABLE service_dates AS
WITH calendar_dates_expanded AS (
  SELECT service_id, date
  FROM calendar_dates
  -- exception_type_v enum: 'added' = added service
  WHERE exception_type::text = 'added'
),
calendar_expanded AS (
  SELECT 
    c.service_id,
    d.date
  FROM calendar c
  CROSS JOIN LATERAL generate_series(c.start_date::date, c.end_date::date, '1 day'::interval) AS d(date)
  WHERE 
    (c.monday::text = 'available' AND EXTRACT(DOW FROM d.date) = 1) OR
    (c.tuesday::text = 'available' AND EXTRACT(DOW FROM d.date) = 2) OR
    (c.wednesday::text = 'available' AND EXTRACT(DOW FROM d.date) = 3) OR
    (c.thursday::text = 'available' AND EXTRACT(DOW FROM d.date) = 4) OR
    (c.friday::text = 'available' AND EXTRACT(DOW FROM d.date) = 5) OR
    (c.saturday::text = 'available' AND EXTRACT(DOW FROM d.date) = 6) OR
    (c.sunday::text = 'available' AND EXTRACT(DOW FROM d.date) = 0)
)
SELECT DISTINCT service_id, date
FROM (
  SELECT service_id, date FROM calendar_expanded
  UNION
  SELECT service_id, date FROM calendar_dates_expanded
) combined
WHERE date NOT IN (
  -- exception_type_v enum: 'removed' = removed service
  SELECT date FROM calendar_dates 
  WHERE exception_type::text = 'removed'
);

DROP TABLE IF EXISTS trips_input;
CREATE TABLE trips_input (
  trip_id text,
  route_id text,
  service_id text,
  date date,
  point_geom geometry,
  t timestamptz
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting trips_input';
  INSERT INTO trips_input
  SELECT trip_id, route_id, t.service_id,
         date, point_geom, date + point_arrival_time AS t
  FROM trip_points t
  JOIN service_dates s ON t.service_id = s.service_id
  -- Use first available service date (or adjust as needed)
  WHERE date = (SELECT MIN(date) FROM service_dates);
END;
$$;

-- Create scheduled_trips_mdb table (renamed from trips_mdb)
-- Using MobilityDB tgeompoint for temporal geometry
DROP TABLE IF EXISTS scheduled_trips_mdb CASCADE;
CREATE TABLE scheduled_trips_mdb (
  trip_id text NOT NULL,
  route_id text NOT NULL,
  date date NOT NULL,
  trip tgeompoint,
  PRIMARY KEY (trip_id, date)
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting scheduled_trips_mdb';
  WITH only_first_point_trips_input AS (
    SELECT DISTINCT ON (trip_id, route_id, date, t) trip_id, route_id, date, t, point_geom 
    FROM trips_input
    ORDER BY trip_id, route_id, date, t
  )
  INSERT INTO scheduled_trips_mdb(trip_id, route_id, date, trip)
  SELECT 
    trip_id, 
    route_id, 
    date, 
    tgeompointseq(array_agg(tgeompoint(point_geom, t) ORDER BY t))
  FROM only_first_point_trips_input
  GROUP BY trip_id, route_id, date
  HAVING COUNT(*) > 1;  -- Only trips with multiple points
END;
$$;

ALTER TABLE scheduled_trips_mdb ADD COLUMN IF NOT EXISTS traj geometry;
ALTER TABLE scheduled_trips_mdb ADD COLUMN IF NOT EXISTS starttime timestamp;

DO $$
BEGIN
  RAISE NOTICE '...Updating scheduled_trips_mdb';
  UPDATE scheduled_trips_mdb SET traj = trajectory(trip);
  UPDATE scheduled_trips_mdb SET starttime = startTimestamp(trip);
END;
$$;

-- Create spatial index
CREATE INDEX IF NOT EXISTS idx_scheduled_trips_mdb_traj ON scheduled_trips_mdb USING GIST (traj);
CREATE INDEX IF NOT EXISTS idx_scheduled_trips_mdb_trip ON scheduled_trips_mdb USING GIST (trip);


