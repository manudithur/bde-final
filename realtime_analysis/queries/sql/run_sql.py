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


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


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
    
    # If it starts with WITH, keep the whole thing
    # If it starts with SELECT, keep the whole thing
    if content.upper().startswith('WITH') or content.upper().startswith('SELECT'):
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
    # Remove leading numbers and underscores (e.g., "01_headway_stops" -> "headway_stops")
    name = re.sub(r'^\d+_', '', name)
    # Convert to lowercase and replace spaces/hyphens with underscores
    name = name.lower().replace('-', '_').replace(' ', '_')
    return f"qgis_realtime_{name}"


def detect_geometry_type(query: str) -> str:
    """Try to detect geometry type from query."""
    query_lower = query.lower()
    
    if 'st_makepoint' in query_lower or 'stop_lat' in query_lower or 'stop_lon' in query_lower:
        return 'POINT'
    elif 'seg_geom' in query_lower or 'delay' in query_lower:
        return 'GEOMETRY'  # Could be LINESTRING or MULTILINESTRING
    elif 'null::geometry' in query_lower:
        return None  # No geometry
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
    # Exclude NULL::geometry
    has_null_geom = 'null::geometry' in query_lower
    return (has_geom_alias or has_geom_functions) and not has_null_geom


def create_materialized_view(conn, view_name: str, query: str, geometry_type: str = None):
    """Create a materialized view from a query."""
    cur = conn.cursor()
    
    try:
        # Drop existing view if it exists
        print(f"  Dropping existing view {view_name} if exists...")
        cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
        
        # Check if this query has a geometry column
        has_geom = has_geometry_column(query)
        
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
        
        # Check if this query has a geometry column before trying to create spatial index
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
            print(f"  Registering geometry column...")
            try:
                cur.execute(f"SELECT Populate_Geometry_Columns('{view_name}'::regclass);")
            except Exception as reg_err:
                print(f"  ⚠ Could not auto-register geometry: {reg_err}")
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


def execute_sql_file(conn, file_path: Path) -> bool:
    """Execute a SQL file directly (for files with CREATE MATERIALIZED VIEW statements)."""
    cur = conn.cursor()
    
    try:
        print(f"  Executing SQL file: {file_path.name}...")
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Add CASCADE to DROP MATERIALIZED VIEW statements to handle dependencies
        import re
        # Replace DROP MATERIALIZED VIEW IF EXISTS view_name; with CASCADE version
        sql_content = re.sub(
            r'DROP MATERIALIZED VIEW IF EXISTS (\w+);',
            r'DROP MATERIALIZED VIEW IF EXISTS \1 CASCADE;',
            sql_content
        )
        
        # Execute the SQL file
        cur.execute(sql_content)
        conn.commit()
        print(f"  ✓ Successfully executed {file_path.name}")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"  ✗ Error executing {file_path.name}: {e}")
        return False
    finally:
        cur.close()


def main():
    """Main entry point."""
    print("=" * 60)
    print("SQL MATERIALIZED VIEWS CREATOR - REALTIME ANALYSIS")
    print("=" * 60)
    
    # First, process realtime_queries.sql (contains base materialized views)
    realtime_queries_file = SCRIPT_DIR / "realtime_queries.sql"
    
    print("\nConnecting to database...")
    conn = get_db_connection()
    conn.autocommit = False
    
    results = []
    
    # Step 1: Execute realtime_queries.sql first (base materialized views)
    if realtime_queries_file.exists():
        print("\n" + "=" * 60)
        print("STEP 1: Creating Base Materialized Views")
        print("=" * 60)
        print(f"\nProcessing {realtime_queries_file.name}...")
        success = execute_sql_file(conn, realtime_queries_file)
        results.append({
            "file": realtime_queries_file.name,
            "view": "base_views",
            "success": success
        })
    else:
        print(f"\n⚠ Warning: {realtime_queries_file.name} not found. Skipping base views.")
    
    # Step 2: Process QGIS query files (SELECT statements that create qgis_* views)
    print("\n" + "=" * 60)
    print("STEP 2: Creating QGIS Materialized Views")
    print("=" * 60)
    
    # Get all SQL files except realtime_queries.sql
    sql_files = sorted(SCRIPT_DIR.glob("*.sql"))
    sql_files = [f for f in sql_files if f.name not in ["README.md", "realtime_queries.sql"]]
    
    if not sql_files:
        print("No QGIS SQL query files found in the directory.")
    else:
        print(f"\nFound {len(sql_files)} QGIS SQL query files")
        
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
    print("2. Look for materialized views starting with 'qgis_realtime_'")
    print("3. Add them as layers directly")
    print("=" * 60)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

