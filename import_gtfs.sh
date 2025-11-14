#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# GTFS Import and Processing Script
# Usage: ./import_gtfs.sh [OPTIONS]
# 
# Required parameters:
#   --db-host     Database host (default: BD-ESPACIALES)
#   --db-user     Database user (default: postgres)
#   --db-pass     Database password (default: postgres)
#   --db-name     Database name (default: gtfs_be)
#   --start-date  Start date for segments (YYYY-MM-DD)
#   --end-date    End date for segments (YYYY-MM-DD)
#   --gtfs-path   Path to GTFS data directory (default: data/gtfs_pruned)

set -e  # Exit on any error

# Default values (use env vars if available)
DB_HOST="${PGHOST:-BD-ESPACIALES}"
DB_USER="${PGUSER:-postgres}"
DB_PASS="${PGPASSWORD:-postgres}"
DB_NAME="${PGDATABASE:-gtfs_be}"
START_DATE=""
END_DATE=""
GTFS_PATH="data/gtfs_pruned"
GTFS_ZIP="data/gtfs_pruned.zip"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-host)
            DB_HOST="$2"
            shift 2
            ;;
        --db-user)
            DB_USER="$2"
            shift 2
            ;;
        --db-pass)
            DB_PASS="$2"
            shift 2
            ;;
        --db-name)
            DB_NAME="$2"
            shift 2
            ;;
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --gtfs-path)
            GTFS_PATH="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --db-host     Database host (default: BD-ESPACIALES)"
            echo "  --db-user     Database user (default: postgres)"
            echo "  --db-pass     Database password (default: postgres)"
            echo "  --db-name     Database name (default: gtfs_be)"
            echo "  --start-date  Start date for segments (YYYY-MM-DD)"
            echo "  --end-date    End date for segments (YYYY-MM-DD)"
            echo "  --gtfs-path   Path to GTFS data directory (default: src/data/gtfs_pruned)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "Error: --start-date and --end-date are required"
    echo "Use --help for usage information"
    exit 1
fi

# Validate date format
if ! date -d "$START_DATE" >/dev/null 2>&1 || ! date -d "$END_DATE" >/dev/null 2>&1; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

# Check if GTFS data exists
if [[ ! -d "$GTFS_PATH" ]]; then
    echo "Error: GTFS data directory '$GTFS_PATH' not found"
    echo "Run ./download_data.sh first to download the data"
    exit 1
fi


echo "Starting GTFS import and processing..."
echo "Database: $DB_USER@$DB_HOST/$DB_NAME"
echo "Date range: $START_DATE to $END_DATE"
echo "GTFS path: $GTFS_PATH"

# Set environment variables for PostgreSQL
export PGHOST="$DB_HOST"
export PGUSER="$DB_USER"
export PGPASSWORD="$DB_PASS"
export PGDATABASE="$DB_NAME"

# Test database connection using Docker
echo "Testing database connection to Docker container..."
if ! docker exec BD-ESPACIALES psql -U "$DB_USER" -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
    echo "Error: Cannot connect to database in Docker container '$DB_HOST'"
    echo "Make sure Docker container '$DB_HOST' is running and credentials are correct"
    exit 1
fi

# Force delete and recreate database
echo "Force deleting existing database '$DB_NAME'..."
if docker exec BD-ESPACIALES psql -U "$DB_USER" -d postgres -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Dropping existing database '$DB_NAME'..."
    docker exec BD-ESPACIALES dropdb -U "$DB_USER" "$DB_NAME" --force
fi

echo "Creating fresh database '$DB_NAME'..."
docker exec BD-ESPACIALES createdb -U "$DB_USER" "$DB_NAME"

# Copy GTFS data to Docker container
echo "Copying GTFS data to Docker container..."
docker exec BD-ESPACIALES mkdir -p /tmp/gtfs
docker cp "$GTFS_PATH/." BD-ESPACIALES:/tmp/gtfs/

# Import GTFS data to SQL
echo "Importing GTFS data to database..."

# Run gtfs-to-sql and import to database via Docker
echo "Generating SQL from GTFS files..."
docker exec BD-ESPACIALES sh -c "cd /tmp/gtfs && gtfs-to-sql --require-dependencies --routes-without-agency-id --trips-without-shape-id -- *.txt > /tmp/gtfs_import.sql"

echo "Importing SQL into database..."
if ! docker exec BD-ESPACIALES psql -U "$DB_USER" -d "$DB_NAME" -f /tmp/gtfs_import.sql; then
    echo "Error: Failed to import GTFS data"
    exit 1
fi

echo "GTFS data imported successfully!"

# Create zip file from pruned data
echo "Creating zip file from pruned data..."
if [[ -f "$GTFS_ZIP" ]]; then
    echo "Removing existing zip file..."
    rm "$GTFS_ZIP"
fi

cd "$GTFS_PATH"
if ! zip -r "../gtfs_pruned.zip" *.txt; then
    echo "Error: Failed to create zip file"
    exit 1
fi
cd - >/dev/null

echo "Zip file created successfully!"

# Copy zip file and Python script to Docker container
echo "Copying zip file to Docker container..."
docker cp "$GTFS_ZIP" BD-ESPACIALES:/tmp/gtfs_pruned.zip

echo "Copying Python script to Docker container..."
docker cp src/static/split_into_segments_mapmatched.py BD-ESPACIALES:/tmp/split_into_segments.py

echo "Creating route segments..."
echo "⏳ Processing ~1.6M stop_times records - this may take 15-30 minutes..."
echo "📊 Progress will be shown below:"
echo ""

# Run with interactive terminal and real-time output
if ! docker exec -it BD-ESPACIALES python3 -u /tmp/split_into_segments.py "/tmp/gtfs_pruned.zip" \
    --start-date "$START_DATE" --end-date "$END_DATE" \
    --db-host localhost --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"; then
    echo "Error: Failed to create route segments"
    exit 1
fi

echo "Route segments created successfully!"
echo "GTFS import and processing completed!"