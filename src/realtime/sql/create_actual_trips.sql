DROP TABLE IF EXISTS actual_trips;

CREATE TABLE actual_trips(trip_id, trip) AS
WITH positions(trip_id, geom, t) AS (
-- We use DISTINCT since we observed duplicate tuples
SELECT DISTINCT id, ST_Transform(geom, 3059), to_timestamp(timestamp)
FROM vehicle_positions
WHERE geom IS NOT NULL)
SELECT trip_id, tgeompointSeq(array_agg(tgeompoint(geom, t) ORDER BY t)
FILTER (WHERE geom IS NOT NULL)) as trip
FROM positions
GROUP BY trip_id
HAVING tgeompointSeq(array_agg(tgeompoint(geom, t) ORDER BY t) FILTER (WHERE geom IS NOT NULL)) IS NOT NULL;

ALTER TABLE actual_trips ADD COLUMN trajectory geometry;
UPDATE actual_trips SET trajectory = trajectory(trip);