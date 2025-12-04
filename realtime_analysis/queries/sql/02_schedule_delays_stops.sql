-- Schedule Times - Delay at Stops Query for QGIS
-- Shows BUS stops with average delay statistics

SELECT
    st.stop_id,
    st.stop_name,
    st.stop_lat,
    st.stop_lon,
    st.route_short_name,
    AVG(st.delay_minutes) AS avg_delay_minutes,
    STDDEV(st.delay_minutes) AS std_delay_minutes,
    COUNT(*) AS total_observations,
    SUM(CASE WHEN st.delay_minutes > 5 THEN 1 ELSE 0 END) AS late_count,
    (SUM(CASE WHEN st.delay_minutes > 5 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100) AS late_rate,
    ST_SetSRID(ST_MakePoint(st.stop_lon, st.stop_lat), 4326) AS geom
FROM realtime_schedule_times st
JOIN routes r ON st.route_id = r.route_id
WHERE r.route_type = '3'
GROUP BY st.stop_id, st.stop_name, st.stop_lat, st.stop_lon, st.route_short_name
HAVING COUNT(*) >= 3
ORDER BY avg_delay_minutes DESC;


