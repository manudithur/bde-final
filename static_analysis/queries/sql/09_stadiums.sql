-- Stadiums Query for QGIS
-- Shows all football stadiums with their locations
-- Requires football_stadiums table (created via run_qgis helper)

SELECT 
    name AS stadium_name,
    team,
    latitude,
    longitude,
    geom
FROM football_stadiums;

