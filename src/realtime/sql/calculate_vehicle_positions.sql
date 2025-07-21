ALTER TABLE vehicle_positions ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);
WITH vehicle_segments AS (
    SELECT 
        vp.*,
        seg.geometry as segment_geom,
        seg.distance_m as segment_length_m,
        stops.stop_loc as dest_stop_geom
    FROM vehicle_positions vp
    JOIN segments seg ON (
        seg.route_id = vp.line_id AND 
        seg.end_stop_id = vp.destination_stop_id
    )
    JOIN stops ON stops.stop_id = vp.destination_stop_id
),
calculated_positions AS (
    SELECT 
        vs.*,
        -- Calculate position along segment based on distance_from_point
        CASE 
            WHEN vs.distance_from_point = 0 THEN vs.dest_stop_geom::geometry
            WHEN vs.distance_from_point >= vs.segment_length_m THEN 
                ST_StartPoint(vs.segment_geom)
            ELSE 
                ST_LineInterpolatePoint(
                    vs.segment_geom,
                    1.0 - (vs.distance_from_point::float / vs.segment_length_m)
                )
        END as calculated_geom
    FROM vehicle_segments vs
)
UPDATE vehicle_positions 
SET geom = cp.calculated_geom
FROM calculated_positions cp
WHERE vehicle_positions.id = cp.id;