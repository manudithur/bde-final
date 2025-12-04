#!/usr/bin/env python3
"""
Create materialized views from QGIS queries for easy import into QGIS
This script reads all SQL query files and creates materialized views
that can be directly loaded into QGIS as PostGIS layers.
"""

import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
import re

load_dotenv()

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).resolve().parent

# Default list of stadiums to ensure exist for QGIS queries
STADIUMS = [
    ("BC Place", "Vancouver Whitecaps/BC Lions", 49.27596, -123.11274),
    ("Rogers Arena", "Vancouver Canucks", 49.277821, -123.109085),
    ("Pacific Coliseum", "Vancouver Giants", 49.2848, -123.0390),
]


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


def ensure_stadium_table(conn):
    """Create/populate football_stadiums helper table for stadium queries."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS football_stadiums (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                team TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                geom geometry(Point, 4326)
            );
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_football_stadiums_name ON football_stadiums (name);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_football_stadiums_geom ON football_stadiums USING GIST (geom);"
        )
        for name, team, lat, lon in STADIUMS:
            cur.execute(
                """
                INSERT INTO football_stadiums (name, team, latitude, longitude, geom)
                VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                ON CONFLICT (name) DO UPDATE
                SET team = EXCLUDED.team,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    geom = EXCLUDED.geom;
                """,
                (name, team, lat, lon, lon, lat),
            )
        conn.commit()
    finally:
        cur.close()


def extract_query_from_file(file_path: Path) -> str:
    """Extract the complete query from SQL file, removing comments."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove SQL comments (-- and /* */)
    content = re.sub(r'--.*?$', '', content, flags=re.MULTILINE)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    # Remove trailing semicolon if present
    content = content.rstrip().rstrip(';')
    
    # Check if query starts with WITH (CTE) or SELECT
    content = content.strip()
    if not content:
        return None
    
    # If it starts with CREATE TABLE, WITH, or SELECT, keep the whole thing
    if (content.upper().startswith('CREATE TABLE') or 
        content.upper().startswith('WITH') or 
        content.upper().startswith('SELECT')):
        return content
    
    # Try to find SELECT statement
    select_match = re.search(r'(SELECT\s+.*)', content, re.IGNORECASE | re.DOTALL)
    if select_match:
        return select_match.group(1).strip()
    
    return None


def get_view_name_from_file(file_path: Path) -> str:
    """Generate materialized view name from filename."""
    # Remove .sql extension and prefix numbers
    name = file_path.stem
    # Remove leading numbers and underscores (e.g., "01_route_visualization" -> "route_visualization")
    name = re.sub(r'^\d+_', '', name)
    # Convert to lowercase and replace spaces/hyphens with underscores
    name = name.lower().replace('-', '_').replace(' ', '_')
    return f"qgis_{name}"


def detect_geometry_type(query: str) -> str:
    """Try to detect geometry type from query."""
    query_lower = query.lower()
    
    if 'st_makepoint' in query_lower or 'stop_lat' in query_lower or 'stop_lon' in query_lower:
        return 'POINT'
    elif 'seg_geom' in query_lower or 'route_geometry' in query_lower:
        return 'GEOMETRY'  # Could be LINESTRING or MULTILINESTRING
    elif 'population_density' in query_lower and 'geom' in query_lower:
        return 'MULTIPOLYGON'
    else:
        return 'GEOMETRY'  # Generic fallback


def has_geometry_column(query: str) -> bool:
    """Check if query likely produces a geometry column."""
    query_lower = query.lower()
    # Check if query has 'AS geom' or geometry functions
    has_geom_alias = ' as geom' in query_lower or '\tas geom' in query_lower
    has_geom_functions = any(fn in query_lower for fn in [
        'st_makepoint', 'st_setsrid', 'seg_geom', 'route_geometry',
        'stop_geom', 'stop_loc', '.geom'
    ])
    return has_geom_alias or has_geom_functions


def detect_geometry_type_from_query(query: str) -> str:
    """Detect the likely geometry type from the query."""
    query_lower = query.lower()
    if 'st_makepoint' in query_lower or 'stop_lat' in query_lower:
        return 'POINT'
    elif 'seg_geom' in query_lower:
        return 'LINESTRING'
    elif 'route_geometry' in query_lower:
        return 'MULTILINESTRING'
    elif 'population_density' in query_lower:
        return 'MULTIPOLYGON'
    else:
        return 'GEOMETRY'


def create_materialized_view(conn, view_name: str, query: str, geometry_type: str = 'GEOMETRY'):
    """Create a materialized view from a query."""
    cur = conn.cursor()
    
    try:
        # Drop existing view if it exists
        print(f"  Dropping existing view {view_name} if exists...")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
        
        # Check if query contains CREATE TABLE statements (for stadium queries)
        if query.upper().strip().startswith('CREATE TABLE'):
            # Execute CREATE TABLE statements first
            print(f"  Executing CREATE TABLE statements...")
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in query.split(';') if s.strip()]
            for stmt in statements:
                if stmt.upper().startswith('CREATE TABLE') or stmt.upper().startswith('CREATE INDEX'):
                    cur.execute(stmt)
            conn.commit()
            # Extract the SELECT part for the view
            select_match = re.search(r'(SELECT\s+.*)', query, re.IGNORECASE | re.DOTALL)
            if not select_match:
                print(f"  ⚠ Could not find SELECT statement after CREATE TABLE")
                return False
            query = select_match.group(1).strip()
        
        # Check if this query has a geometry column
        has_geom = has_geometry_column(query)
        geom_type = detect_geometry_type_from_query(query) if has_geom else None
        
        # Create materialized view with a unique gid column (required by QGIS)
        print(f"  Creating materialized view {view_name}...")
        
        # Wrap query to add row_number() as gid for QGIS primary key
        create_sql = f"""
        CREATE MATERIALIZED VIEW {view_name} AS
        SELECT 
            ROW_NUMBER() OVER () AS gid,
            subq.*
        FROM (
            {query}
        ) AS subq;
        """
        
        cur.execute(create_sql)
        
        # Create unique index on gid (QGIS needs a primary key)
        print(f"  Creating unique index on gid...")
        try:
            cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_{view_name}_gid 
                ON {view_name} (gid);
            """)
        except Exception as idx_err:
            print(f"  ⚠ Could not create gid index: {idx_err}")
        
        # If has geometry, create spatial index and register geometry column
        if has_geom:
            print(f"  Creating spatial index on {view_name}...")
            try:
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{view_name}_geom 
                    ON {view_name} USING GIST (geom);
                """)
            except Exception as idx_err:
                print(f"  ⚠ Could not create spatial index: {idx_err}")
            
            # Register geometry column for QGIS compatibility
            print(f"  Registering geometry column (type: {geom_type})...")
            try:
                # Use Populate_Geometry_Columns to register the view
                cur.execute(f"SELECT Populate_Geometry_Columns('{view_name}'::regclass);")
            except Exception as reg_err:
                print(f"  ⚠ Could not auto-register geometry: {reg_err}")
                # Try alternative: update geometry_columns view manually isn't possible
                # but we can add a comment that helps some tools
                try:
                    cur.execute(f"""
                        COMMENT ON COLUMN {view_name}.geom IS 
                        'Geometry column, type={geom_type}, srid=4326';
                    """)
                except:
                    pass
        else:
            print(f"  ℹ No geometry column detected, skipping spatial index")
        
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {view_name};")
        row_count = cur.fetchone()[0]
        
        conn.commit()
        print(f"  ✓ Created {view_name} with {row_count:,} rows")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"  ✗ Error creating {view_name}: {e}")
        return False
    finally:
        cur.close()


def main():
    """Main entry point."""
    print("=" * 60)
    print("QGIS MATERIALIZED VIEWS CREATOR - STATIC ANALYSIS")
    print("=" * 60)
    
    # Get all SQL files in the directory
    sql_files = sorted(SCRIPT_DIR.glob("*.sql"))
    sql_files = [f for f in sql_files if f.name != "README.md"]
    
    if not sql_files:
        print("No SQL query files found in the directory.")
        return 1
    
    print(f"\nFound {len(sql_files)} SQL query files")
    print("\nConnecting to database...")
    
    conn = get_db_connection()
    conn.autocommit = False
    
    # Ensure helper tables exist (e.g., football_stadiums)
    print("\nEnsuring helper tables (football_stadiums)...")
    ensure_stadium_table(conn)
    
    results = []
    
    for sql_file in sql_files:
        print(f"\nProcessing {sql_file.name}...")
        
        query = extract_query_from_file(sql_file)
        if not query:
            print(f"  ⚠ Could not extract query from {sql_file.name}")
            results.append({"file": sql_file.name, "success": False, "error": "Could not extract query"})
            continue
        
        view_name = get_view_name_from_file(sql_file)
        geometry_type = detect_geometry_type(query)
        
        success = create_materialized_view(conn, view_name, query, geometry_type)
        results.append({
            "file": sql_file.name,
            "view": view_name,
            "success": success
        })
    
    conn.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    successful = sum(1 for r in results if r.get("success", False))
    failed = len(results) - successful
    
    print(f"\n✓ Successful: {successful}")
    print(f"✗ Failed:     {failed}")
    
    if successful > 0:
        print("\nCreated materialized views:")
        for r in results:
            if r.get("success", False):
                print(f"  - {r['view']}")
    
    if failed > 0:
        print("\nFailed files:")
        for r in results:
            if not r.get("success", False):
                print(f"  - {r['file']}: {r.get('error', 'Unknown error')}")
    
    print("\n" + "=" * 60)
    print("To use in QGIS:")
    print("1. Connect to your PostgreSQL database in QGIS")
    print("2. Look for materialized views starting with 'qgis_'")
    print("3. Add them as layers directly")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

