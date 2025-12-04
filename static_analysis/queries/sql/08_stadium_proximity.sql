-- Stadium Proximity Query for QGIS
-- Shows BUS stops within 600m of football stadiums
-- Requires football_stadiums table (created via run_qgis helper)

WITH stadium_stops AS (
    SELECT 
        s.name AS stadium_name,
        s.team,
        s.latitude AS stadium_lat,
        s.longitude AS stadium_lon,
        st.stop_id,
        st.stop_name,
        st.stop_loc::geometry AS stop_geom,
        ST_DistanceSphere(s.geom, st.stop_loc::geometry) AS distance_m
    FROM football_stadiums s
    CROSS JOIN stops st
    WHERE ST_DistanceSphere(s.geom, st.stop_loc::geometry) <= 600
        AND EXISTS (
            SELECT 1 
            FROM stop_times stt
            JOIN trips t ON stt.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            WHERE stt.stop_id = st.stop_id
                AND r.route_type = '3'
        )
)
SELECT 
    stadium_name,
    team,
    stop_id,
    stop_name,
    distance_m,
    stop_geom AS geom
FROM stadium_stops
ORDER BY stadium_name, distance_m;

