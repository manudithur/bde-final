#!/usr/bin/env python3
"""
Complete Brussels venues import script
Cleans, preprocesses, and imports cultural venues and sports facilities data
"""

import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
import os
from typing import Tuple, Optional
from urllib.parse import parse_qs, urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VenuesImporter:
    def __init__(self):
        # Get paths
        script_dir = Path(__file__).parent
        project_root = script_dir.parent.parent
        self.data_dir = project_root / "src" / "data" / "additional_datasets"
        self.scripts_dir = script_dir
        
        # Data files
        self.cultural_csv = self.data_dir / 'venues' / 'cultural_venues.csv'
        self.sports_csv = self.data_dir / 'venues' / 'sports_facilities.csv'
        
        # Database connection
        self.engine = self._create_engine()
        
    def _create_engine(self):
        """Create SQLAlchemy engine from environment variables"""
        try:
            db_host = os.getenv('PGHOST', 'localhost')
            db_port = os.getenv('PGPORT', '5432')
            db_name = os.getenv('PGDATABASE', 'gtfs_brussels')
            db_user = os.getenv('PGUSER', 'postgres')
            db_password = os.getenv('PGPASSWORD', 'password')
            
            connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(connection_string)
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Connected to database: {db_name}@{db_host}")
            return engine
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return None
    
    def extract_coordinates_from_google_maps(self, url: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from Google Maps URLs"""
        if pd.isna(url) or not url:
            return None, None
            
        try:
            if 'query=' in url:
                parsed = urlparse(url)
                query_params = parse_qs(parsed.query)
                if 'query' in query_params:
                    coords = query_params['query'][0].replace('%2C', ',')
                    if ',' in coords:
                        lat_str, lon_str = coords.split(',')
                        return float(lat_str.strip()), float(lon_str.strip())
            
            coord_pattern = r'([+-]?\d+\.?\d*),([+-]?\d+\.?\d*)'
            match = re.search(coord_pattern, url)
            if match:
                lat, lon = match.groups()
                return float(lat), float(lon)
                
        except (ValueError, IndexError, AttributeError):
            pass
            
        return None, None
    
    def normalize_pmr_accessibility(self, value: str) -> Optional[bool]:
        """Convert PMR accessibility flags to boolean"""
        if pd.isna(value) or not value:
            return None
            
        value = str(value).strip().lower()
        
        if any(indicator in value for indicator in ['accessible pmr', 'accessible', 'oui', 'yes', 'true']):
            return True
        if any(indicator in value for indicator in ['pas accessible', 'non accessible', 'non', 'no', 'false']):
            return False
        return None
    
    def clean_capacity(self, capacity) -> int:
        """Clean and normalize capacity values"""
        if pd.isna(capacity):
            return 100
            
        try:
            capacity_str = str(capacity).strip()
            capacity_clean = re.sub(r'[^\d.]', '', capacity_str)
            
            if capacity_clean:
                capacity_num = float(capacity_clean)
                if capacity_num < 1:
                    return 100
                elif capacity_num > 100000:
                    return 100000
                return int(capacity_num)
            else:
                return 100
                
        except (ValueError, TypeError):
            return 100
    
    def clean_text_field(self, text, max_length: int = 255) -> str:
        """Clean and truncate text fields"""
        if pd.isna(text):
            return ''
            
        text_str = str(text).strip()
        text_str = re.sub(r'\s+', ' ', text_str)
        text_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text_str)
        return text_str[:max_length]
    
    def generate_venue_id(self, index: int, venue_type: str, name: str) -> str:
        """Generate standardized venue IDs for missing ones"""
        name_clean = re.sub(r'[^\w\s]', '', str(name).lower())
        name_short = ''.join(name_clean.split())[:10]
        return f"{venue_type}_{index+1:04d}_{name_short}"
    
    def create_tables(self):
        """Create venue tables"""
        if not self.engine:
            return False
            
        try:
            sql_script = """
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
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(sql_script))
                conn.commit()
            
            logger.info("Venue tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def clean_and_import_cultural_venues(self):
        """Clean and import cultural venues data"""
        try:
            logger.info("Processing cultural venues...")
            df = pd.read_csv(self.cultural_csv, on_bad_lines='skip')
            logger.info(f"Loaded {len(df)} cultural venues")
            
            # Clean venue_id
            if 'venue_id' in df.columns:
                mask = df['venue_id'].isna() | (df['venue_id'] == '')
                if mask.any():
                    for idx in df[mask].index:
                        name = df.loc[idx, 'venue_name']
                        df.loc[idx, 'venue_id'] = self.generate_venue_id(idx, 'cultural', name)
            
            # Clean required fields
            df['venue_name'] = df['venue_name'].apply(lambda x: self.clean_text_field(x, 255))
            df = df[df['venue_name'] != '']
            
            # Clean optional fields
            if 'category' in df.columns:
                df['category'] = df['category'].apply(lambda x: self.clean_text_field(x, 100))
            if 'municipality' in df.columns:
                df['municipality'] = df['municipality'].apply(lambda x: self.clean_text_field(x, 100))
                df['municipality'] = df['municipality'].replace('', 'Bruxelles')
            
            # Convert PMR accessibility to boolean
            if 'pmr_accessible' in df.columns:
                df['pmr_accessible_bool'] = df['pmr_accessible'].apply(self.normalize_pmr_accessibility)
            
            # Extract coordinates from Google Maps URLs if missing
            if 'google_maps_url' in df.columns:
                logger.info("Extracting coordinates from Google Maps URLs...")
                coords_extracted = df['google_maps_url'].apply(self.extract_coordinates_from_google_maps)
                extracted_lats = [coord[0] for coord in coords_extracted]
                extracted_lons = [coord[1] for coord in coords_extracted]
                
                if 'latitude' in df.columns and 'longitude' in df.columns:
                    lat_mask = df['latitude'].isna() | (df['latitude'] == 0)
                    lon_mask = df['longitude'].isna() | (df['longitude'] == 0)
                    
                    df.loc[lat_mask, 'latitude'] = pd.Series(extracted_lats, index=df.index)[lat_mask]
                    df.loc[lon_mask, 'longitude'] = pd.Series(extracted_lons, index=df.index)[lon_mask]
                else:
                    df['latitude'] = extracted_lats
                    df['longitude'] = extracted_lons
            
            # Clean capacity
            if 'capacity' in df.columns:
                df['capacity'] = df['capacity'].apply(self.clean_capacity)
            
            # Select only essential columns and create spatial points
            essential_cols = ['venue_id', 'venue_name', 'category', 'capacity', 'municipality', 'latitude', 'longitude', 'pmr_accessible_bool']
            df_clean = df[essential_cols].copy()
            
            # Import to database with spatial points
            with self.engine.connect() as conn:
                # Import basic data first
                df_clean.to_sql('cultural_venues', conn, if_exists='append', index=False, method='multi')
                
                # Update spatial points using lat/lng
                conn.execute(text("""
                    UPDATE cultural_venues 
                    SET location = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) 
                    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                """))
                
                conn.commit()
            
            logger.info(f"Imported {len(df_clean)} cultural venues")
            return True
            
        except Exception as e:
            logger.error(f"Error importing cultural venues: {e}")
            return False
    
    def clean_and_import_sports_facilities(self):
        """Clean and import sports facilities data"""
        try:
            logger.info("Processing sports facilities...")
            df = pd.read_csv(self.sports_csv, on_bad_lines='skip')
            logger.info(f"Loaded {len(df)} sports facilities")
            
            # Generate facility_id for missing ones
            if 'facility_id' in df.columns:
                mask = df['facility_id'].isna() | (df['facility_id'] == '')
                if mask.any():
                    for idx in df[mask].index:
                        name = df.loc[idx, 'facility_name']
                        df.loc[idx, 'facility_id'] = self.generate_venue_id(idx, 'sports', name)
            else:
                df['facility_id'] = [self.generate_venue_id(idx, 'sports', row['facility_name']) 
                                   for idx, row in df.iterrows()]
            
            # Clean required fields
            df['facility_name'] = df['facility_name'].apply(lambda x: self.clean_text_field(x, 255))
            df = df[df['facility_name'] != '']
            
            # Clean optional fields
            if 'type' in df.columns:
                df['type'] = df['type'].apply(lambda x: self.clean_text_field(x, 100))
            if 'municipality' in df.columns:
                df['municipality'] = df['municipality'].apply(lambda x: self.clean_text_field(x, 100))
                df['municipality'] = df['municipality'].replace('', 'Bruxelles')
            
            # Extract coordinates from Google Maps URLs if missing
            if 'google_maps_url' in df.columns:
                logger.info("Extracting coordinates from Google Maps URLs...")
                coords_extracted = df['google_maps_url'].apply(self.extract_coordinates_from_google_maps)
                extracted_lats = [coord[0] for coord in coords_extracted]
                extracted_lons = [coord[1] for coord in coords_extracted]
                
                if 'latitude' in df.columns and 'longitude' in df.columns:
                    lat_mask = df['latitude'].isna() | (df['latitude'] == 0)
                    lon_mask = df['longitude'].isna() | (df['longitude'] == 0)
                    
                    df.loc[lat_mask, 'latitude'] = pd.Series(extracted_lats, index=df.index)[lat_mask]
                    df.loc[lon_mask, 'longitude'] = pd.Series(extracted_lons, index=df.index)[lon_mask]
                else:
                    df['latitude'] = extracted_lats
                    df['longitude'] = extracted_lons
            
            # Clean capacity
            if 'capacity' in df.columns:
                df['capacity'] = df['capacity'].apply(self.clean_capacity)
            
            # Select only essential columns
            essential_cols = ['facility_id', 'facility_name', 'type', 'capacity', 'municipality', 'latitude', 'longitude']
            df_clean = df[essential_cols].copy()
            
            # Import to database
            with self.engine.connect() as conn:
                df_clean.to_sql('sports_facilities', conn, if_exists='append', index=False, method='multi')
                conn.commit()
            
            logger.info(f"Imported {len(df_clean)} sports facilities")
            return True
            
        except Exception as e:
            logger.error(f"Error importing sports facilities: {e}")
            return False
    
    def get_venue_stats(self):
        """Get statistics about imported venues"""
        if not self.engine:
            return
            
        try:
            with self.engine.connect() as conn:
                # Cultural venues stats
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_venues,
                        COUNT(DISTINCT category) as unique_categories,
                        AVG(capacity) as avg_capacity,
                        MIN(capacity) as min_capacity,
                        MAX(capacity) as max_capacity
                    FROM cultural_venues
                    WHERE venue_name IS NOT NULL
                """))
                cultural_stats = result.fetchone()
                
                # Sports facilities stats
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_facilities,
                        COUNT(DISTINCT type) as unique_types,
                        AVG(capacity) as avg_capacity,
                        MIN(capacity) as min_capacity,
                        MAX(capacity) as max_capacity
                    FROM sports_facilities
                    WHERE facility_name IS NOT NULL
                """))
                sports_stats = result.fetchone()
                
                # Top categories
                result = conn.execute(text("""
                    SELECT category, COUNT(*) as count 
                    FROM cultural_venues 
                    WHERE category IS NOT NULL AND category != ''
                    GROUP BY category 
                    ORDER BY count DESC 
                    LIMIT 5
                """))
                top_categories = result.fetchall()
                
                logger.info("=== VENUE IMPORT STATISTICS ===")
                logger.info(f"Cultural Venues: {cultural_stats[0]} total, {cultural_stats[1]} categories")
                logger.info(f"  Capacity range: {cultural_stats[3]}-{cultural_stats[4]} (avg: {cultural_stats[2]:.0f})")
                logger.info(f"Sports Facilities: {sports_stats[0]} total, {sports_stats[1]} types")
                logger.info(f"  Capacity range: {sports_stats[3]}-{sports_stats[4]} (avg: {sports_stats[2]:.0f})")
                logger.info("Top venue categories:")
                for category, count in top_categories:
                    logger.info(f"  {category}: {count}")
            
        except Exception as e:
            logger.error(f"Error getting venue stats: {e}")
    
    def run_import(self):
        """Run the complete import process"""
        logger.info("Starting Brussels venues import...")
        
        if not self.engine:
            return False
        
        try:
            # Create tables
            if not self.create_tables():
                return False
            
            # Import data
            cultural_success = self.clean_and_import_cultural_venues()
            sports_success = self.clean_and_import_sports_facilities()
            
            # Show stats
            self.get_venue_stats()
            
            if cultural_success and sports_success:
                logger.info("Venues import completed successfully")
                return True
            else:
                logger.warning("Import completed with some errors")
                return False
                
        except Exception as e:
            logger.error(f"Import failed: {e}")
            return False

def main():
    """Main entry point"""
    importer = VenuesImporter()
    success = importer.run_import()
    
    if success:
        print("✅ Venues successfully imported to database!")
        print("You can now run venue-based SQL queries.")
    else:
        print("❌ Import failed. Check the logs for details.")

if __name__ == "__main__":
    main()