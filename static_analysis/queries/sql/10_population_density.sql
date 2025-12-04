-- Population Density Query for QGIS
-- Shows population density areas (requires population_density table)

SELECT 
    id,
    population_density,
    ST_Area(geom::geography) / 1000000 AS area_km2,
    geom
FROM population_density
WHERE geom IS NOT NULL
    AND population_density IS NOT NULL
    AND population_density > 0;

