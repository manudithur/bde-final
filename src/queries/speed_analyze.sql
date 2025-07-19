SELECT AVG(s.distance_m /
EXTRACT(EPOCH FROM (c.t_arrival - c.t_departure)) * 3.6) AS avg_speed_kmh,
c.from_stop_id, c.from_stop_name, c.to_stop_id, c.to_stop_name,
s.geometry
FROM connections c, segments s
WHERE c.route_id = s.route_id AND c.direction_id = s.direction_id AND
c.from_stop_id = s.start_stop_id AND c.to_stop_id = s.end_stop_id AND
date BETWEEN '2025-07-07' AND '2025-08-03' AND
EXTRACT(EPOCH FROM (c.t_arrival - c.t_departure)) > 0
GROUP BY c.from_stop_id, c.from_stop_name, c.to_stop_id, c.to_stop_name, s.geometry;