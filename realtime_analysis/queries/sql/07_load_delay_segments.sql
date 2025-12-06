-- Load vs Delay Segments Query for QGIS
-- Shows BUS segments with delays grouped by occupancy bucket

SELECT
    ld.route_id,
    ld.route_short_name,
    ld.route_type,
    ld.occupancy_bucket,
    AVG(ld.segment_delay_minutes) AS avg_delay_minutes,
    STDDEV(ld.segment_delay_minutes) AS std_delay_minutes,
    COUNT(*) AS total_observations,
    ld.seg_geom AS geom
FROM realtime_load_delay ld
WHERE ld.route_type = '3' -- BUS
  AND ld.occupancy_bucket IS NOT NULL
GROUP BY ld.route_id, ld.route_short_name, ld.route_type, ld.occupancy_bucket, ld.seg_geom
HAVING COUNT(*) >= 3
ORDER BY avg_delay_minutes DESC;


