-- Create tables for Brussels venues and facilities data
-- Run this script to set up the venue tables in your GTFS database

-- Enable PostGIS extension for spatial data
CREATE EXTENSION IF NOT EXISTS postgis;

-- Drop existing tables to recreate with correct constraints
DROP TABLE IF EXISTS cultural_venues CASCADE;
DROP TABLE IF EXISTS sports_facilities CASCADE;
DROP TABLE IF EXISTS venue_points CASCADE;

-- Cultural venues table (optimized schema with spatial data)
CREATE TABLE cultural_venues (
    venue_id VARCHAR(50) PRIMARY KEY,
    venue_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    capacity INTEGER DEFAULT 100,
    municipality VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(10, 8),
    pmr_accessible_bool BOOLEAN, -- Normalized boolean accessibility
    location GEOMETRY(POINT, 4326) -- WGS84 spatial point for QGIS
);

-- Sports facilities table (optimized schema with spatial data)
CREATE TABLE sports_facilities (
    facility_id VARCHAR(50) PRIMARY KEY,
    facility_name VARCHAR(255) NOT NULL,
    type VARCHAR(100),
    capacity INTEGER DEFAULT 100,
    municipality VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(10, 8),
    location GEOMETRY(POINT, 4326) -- WGS84 spatial point for QGIS
);

-- Combined venues points table for QGIS import
CREATE TABLE venue_points (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    venue_type VARCHAR(20) NOT NULL,
    subcategory VARCHAR(100),
    capacity INTEGER,
    municipality VARCHAR(100),
    pmr_accessible BOOLEAN,
    location GEOMETRY(POINT, 4326) -- WGS84 spatial reference for QGIS
);

-- Combined venues view for easier querying (optimized)
CREATE OR REPLACE VIEW all_venues AS
SELECT 
    venue_id as id,
    venue_name as name,
    'cultural' as venue_type,
    category as subcategory,
    municipality,
    capacity,
    latitude,
    longitude
FROM cultural_venues
WHERE venue_name IS NOT NULL AND venue_name != ''

UNION ALL

SELECT 
    facility_id as id,
    facility_name as name,
    'sports' as venue_type,
    type as subcategory,
    municipality,
    capacity,
    latitude,
    longitude
FROM sports_facilities
WHERE facility_name IS NOT NULL AND facility_name != '';

-- Create spatial indexes for performance (PostGIS spatial indexes)
CREATE INDEX IF NOT EXISTS idx_cultural_venues_location ON cultural_venues USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_sports_facilities_location ON sports_facilities USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_venue_points_location ON venue_points USING GIST (location);

-- Create regular indexes
CREATE INDEX IF NOT EXISTS idx_cultural_venues_category ON cultural_venues (category);
CREATE INDEX IF NOT EXISTS idx_sports_facilities_type ON sports_facilities (type);
CREATE INDEX IF NOT EXISTS idx_cultural_venues_capacity ON cultural_venues (capacity);
CREATE INDEX IF NOT EXISTS idx_sports_facilities_capacity ON sports_facilities (capacity);
CREATE INDEX IF NOT EXISTS idx_venue_points_type ON venue_points (venue_type);
CREATE INDEX IF NOT EXISTS idx_venue_points_capacity ON venue_points (capacity);

-- Create function to calculate distance between venues and transit stops
CREATE OR REPLACE FUNCTION calculate_distance_km(
    lat1 DECIMAL(10,8), 
    lon1 DECIMAL(10,8), 
    lat2 DECIMAL(10,8), 
    lon2 DECIMAL(10,8)
) RETURNS DECIMAL(10,3) AS $$
BEGIN
    RETURN (
        6371 * acos(
            cos(radians(lat1)) * cos(radians(lat2)) * 
            cos(radians(lon2) - radians(lon1)) + 
            sin(radians(lat1)) * sin(radians(lat2))
        )
    );
END;
$$ LANGUAGE plpgsql;