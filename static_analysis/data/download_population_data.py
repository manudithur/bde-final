#!/usr/bin/env python3
"""
Download and import population density GIS data for Vancouver
Uses CensusMapper API to get pre-joined Census Tract data with population density.
"""

import os
import sys
import requests
import json
from pathlib import Path
import geopandas as gpd
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Vancouver CMA code
VANCOUVER_CMA_CODE = "933"

# Data directory (local, under static_analysis/data/population)
DATA_DIR = Path(__file__).parent / "population"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_and_calculate_density(geojson_path: Path, csv_path: Path = None) -> gpd.GeoDataFrame:
    """Load local GeoJSON file and calculate population density from 'pop' and 'a' columns.

    Assumes:
    - GeoJSON has 'pop' (population) and 'a' (area) columns
    - Calculates density as: population / area
    """
    print("=" * 60)
    print("LOADING POPULATION DATA FROM GEOJSON")
    print("=" * 60)

    geojson_path = Path(geojson_path)

    if not geojson_path.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")
    
    # CSV is no longer used, but kept for backward compatibility
    if csv_path:
        print(f"Note: CSV file is not used - calculating density from GeoJSON only")

    print(f"\nReading GeoJSON: {geojson_path}")
    gdf = gpd.read_file(geojson_path)
    print(f"✓ Loaded {len(gdf)} features from GeoJSON")
    print(f"GeoJSON columns: {list(gdf.columns)}")

    # Calculate population density directly from GeoJSON columns
    print("\nCalculating population density from GeoJSON 'pop' and 'a' columns...")
    
    if "pop" not in gdf.columns or "a" not in gdf.columns:
        raise ValueError(
            "GeoJSON missing required columns 'pop' (population) or 'a' (area). "
            f"Available columns: {list(gdf.columns)}"
        )
    
    # Convert to numeric, handling any string values
    area = pd.to_numeric(gdf["a"], errors="coerce")
    pop = pd.to_numeric(gdf["pop"], errors="coerce")
    
    # Calculate density: population / area
    gdf["population_density"] = pop / area
    
    # Check for any invalid calculations
    valid_count = gdf["population_density"].notna().sum()
    invalid_count = len(gdf) - valid_count
    
    if invalid_count > 0:
        print(f"⚠ Warning: {invalid_count} features have invalid density (missing area or population)")
    else:
        print(f"✓ Calculated population density for all {len(gdf)} features")
    
    # Show sample values
    print(f"  Sample density values: {gdf['population_density'].head(5).tolist()}")
    
    gdf_joined = gdf

    # Final check: count how many have valid density after calculation
    final_valid = gdf_joined["population_density"].notna().sum()
    final_total = len(gdf_joined)
    
    if final_valid < final_total:
        print(f"⚠ Warning: {final_total - final_valid} features still have NaN population_density")
        print(f"  (These will be excluded from database import)")
        # Filter out remaining NaN values before returning
        gdf_joined = gdf_joined[gdf_joined["population_density"].notna()].copy()
        print(f"✓ Final dataset: {len(gdf_joined)} features with valid population density")
    else:
        print(f"✓ All {final_total} features have valid population density")

    return gdf_joined


def import_to_database(gdf: gpd.GeoDataFrame):
    """Import GeoDataFrame to PostgreSQL database."""
    print(f"\nImporting {len(gdf)} features to database...")
    
    try:
        # Ensure CRS is set
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", allow_override=True)
        elif gdf.crs.to_string() != "EPSG:4326":
            print(f"   Converting CRS from {gdf.crs} to EPSG:4326...")
            gdf = gdf.to_crs("EPSG:4326")
        
        # Rename geometry column to geom for PostGIS
        geom_col_name = gdf.geometry.name
        if geom_col_name != 'geom':
            gdf = gdf.rename_geometry("geom")
        gdf = gdf.set_geometry("geom")
        
        # Connect to database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create table
        table_name = "population_density"
        print(f"Creating table {table_name}...")
        
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
        
        # Create table with geometry
        cur.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                geom GEOMETRY(MULTIPOLYGON, 4326)
            );
        """)
        
        # Sanitize column names for PostgreSQL
        def sanitize_column_name(col_name: str) -> str:
            """Sanitize column name for PostgreSQL compatibility."""
            sanitized = col_name.replace('+', '_plus').replace('-', '_').replace(' ', '_')
            sanitized = sanitized.replace('(', '').replace(')', '').replace(',', '_')
            sanitized = sanitized.replace('.', '_').replace('/', '_')
            sanitized = sanitized.strip('_')
            while '__' in sanitized:
                sanitized = sanitized.replace('__', '_')
            if sanitized and not sanitized[0].isalpha() and sanitized[0] != '_':
                sanitized = '_' + sanitized
            if len(sanitized) > 63:
                sanitized = sanitized[:63]
            return sanitized.lower() if sanitized else 'col_' + str(hash(col_name) % 10000)
        
        # Create mapping of original to sanitized column names
        col_mapping = {}
        for col in gdf.columns:
            if col != 'geom':
                sanitized_col = sanitize_column_name(col)
                col_mapping[col] = sanitized_col
        
        # Add columns from GeoDataFrame with sanitized names
        # Skip 'id' column if it exists (we already have a SERIAL id column)
        for col in gdf.columns:
            if col != 'geom':
                sanitized_col = col_mapping[col]
                # Skip if this would conflict with the primary key 'id' column
                if sanitized_col == 'id':
                    print(f"  Skipping column '{col}' (conflicts with primary key 'id')")
                    continue
                
                col_type = "TEXT"
                if gdf[col].dtype == 'int64':
                    col_type = "BIGINT"
                elif gdf[col].dtype == 'float64':
                    col_type = "DOUBLE PRECISION"
                
                # Check if column already exists before adding
                cur.execute(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{table_name}' 
                        AND column_name = '{sanitized_col}'
                    );
                """)
                exists = cur.fetchone()[0]
                
                if not exists:
                    cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{sanitized_col}" {col_type};')
                else:
                    print(f"  Column '{sanitized_col}' already exists, skipping...")
        
        print(f"✓ Table {table_name} created")
        
        # Import data
        print("Importing data...")
        # Filter out 'geom' and 'id' columns (id conflicts with primary key)
        cols = [c for c in gdf.columns if c != 'geom' and col_mapping.get(c, sanitize_column_name(c)) != 'id']
        sanitized_cols = [col_mapping.get(c, sanitize_column_name(c)) for c in cols]
        
        for idx in gdf.index:
            row = gdf.loc[idx]
            values = [row[c] for c in cols]
            geom_wkb = gdf.geometry.loc[idx].wkb
            
            if sanitized_cols:
                col_names = ','.join([f'"{sc}"' for sc in sanitized_cols]) + ', geom'
                placeholders = ','.join(['%s'] * len(sanitized_cols)) + ', ST_GeomFromWKB(%s, 4326)'
                insert_sql = f"""
                    INSERT INTO {table_name} ({col_names})
                    VALUES ({placeholders})
                """
                cur.execute(insert_sql, values + [geom_wkb])
            else:
                insert_sql = f"""
                    INSERT INTO {table_name} (geom)
                    VALUES (ST_GeomFromWKB(%s, 4326))
                """
                cur.execute(insert_sql, [geom_wkb])
        
        # Create spatial index
        print("Creating spatial index...")
        cur.execute(f"""
            CREATE INDEX idx_{table_name}_geom 
            ON {table_name} USING GIST (geom);
        """)
        
        # Find and set population density column
        density_col = None
        for col in gdf.columns:
            col_lower = col.lower()
            if 'density' in col_lower or 'v_ca21_6' in col_lower:
                density_col = col
                break
        
        if density_col:
            sanitized_density_col = col_mapping.get(density_col, sanitize_column_name(density_col))
            # Add population_density column if it doesn't exist
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = '{table_name}' 
                        AND column_name = 'population_density'
                    ) THEN
                        ALTER TABLE {table_name} 
                        ADD COLUMN population_density DOUBLE PRECISION;
                    END IF;
                END $$;
            """)
            
            # Copy density values
            cur.execute(f"""
                UPDATE {table_name}
                SET population_density = "{sanitized_density_col}"::DOUBLE PRECISION
                WHERE "{sanitized_density_col}" IS NOT NULL;
            """)
            print(f"✓ Populated population_density from {density_col}")
        else:
            print("⚠ Could not find density column in data")
        
        conn.close()
        print(f"✓ Successfully imported {len(gdf)} features to {table_name}")
        
    except Exception as e:
        print(f"✗ Error importing to database: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Import Vancouver population density data from local CSV + GeoJSON'
    )
    parser.add_argument(
        '--geo',
        type=str,
        default=str(DATA_DIR / "vancouver_geo.geojson"),
        help='Path to local GeoJSON file with CT geometries '
             '(default: data/population/vancouver_geo.geojson)',
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=str(DATA_DIR / "population_data.csv"),
        help='Path to local CSV file with density data '
             '(default: data/population/population_data.csv)',
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("POPULATION DENSITY DATA IMPORT (LOCAL FILES)")
    print("=" * 60)
    
    geojson_path = Path(args.geo)
    csv_path = Path(args.csv)

    # Load GeoJSON and calculate density
    try:
        gdf = load_and_calculate_density(geojson_path, csv_path)
    except Exception as e:
        print(f"✗ Error preparing joined GeoDataFrame: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Import to database
    print("\nImporting data to database...")
    try:
        import_to_database(gdf)
        print("\n✓ Population density data imported successfully!")
        return 0
    except Exception as e:
        print(f"\n✗ Error importing to database: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
