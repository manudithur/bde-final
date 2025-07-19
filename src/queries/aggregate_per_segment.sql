SELECT c.from_stop_id, c.from_stop_name, c.to_stop_id, c.to_stop_name, s.geometry,
COUNT(trip_id) AS notrips, string_agg(DISTINCT c.route_short_name, ',')
AS routes
FROM segments s, connections c
WHERE s.route_id = c.route_id AND s.direction_id = c.direction_id AND
s.start_stop_id = c.from_stop_id AND s.end_stop_id = c.to_stop_id AND
date BETWEEN '2025-07-07' AND '2025-08-03'
GROUP BY c.from_stop_id, c.from_stop_name, c.to_stop_id, c.to_stop_name,
s.geometry
ORDER BY notrips DESC, c.from_stop_name, c.to_stop_name;
