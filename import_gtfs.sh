#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# GTFS Import and Processing Script
# Usage: ./import_gtfs.sh [OPTIONS]
# 
# Required parameters:
#   --db-host     Database host (default: localhost)
#   --db-user     Database user (default: postgres)
#   --db-pass     Database password (default: postgres)
#   --db-name     Database name (default: gtfs_be)
#   --start-date  Start date for segments (YYYY-MM-DD)
#   --end-date    End date for segments (YYYY-MM-DD)
#   --gtfs-path   Path to GTFS data directory (default: src/data/gtfs_pruned)

set -e  # Exit on any error

# Default values (use env vars if available)
DB_HOST="${PGHOST:-localhost}"
DB_USER="${PGUSER:-postgres}"
DB_PASS="${PGPASSWORD:-postgres}"
DB_NAME="${PGDATABASE:-gtfs_be}"
START_DATE=""
END_DATE=""
GTFS_PATH="src/data/gtfs_pruned"
GTFS_ZIP="src/data/gtfs_pruned.zip"

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
            echo "  --db-host     Database host (default: localhost)"
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

if [[ ! -f "$GTFS_ZIP" ]]; then
    echo "Error: GTFS zip file '$GTFS_ZIP' not found"
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

# Test database connection
echo "Testing database connection..."
if ! psql -c "SELECT 1;" >/dev/null 2>&1; then
    echo "Error: Cannot connect to database"
    echo "Make sure PostgreSQL is running and credentials are correct"
    exit 1
fi

# Check if database exists, create if not
if ! psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Creating database '$DB_NAME'..."
    createdb "$DB_NAME"
fi

# Import GTFS data to SQL
echo "Importing GTFS data to database..."
cd "$GTFS_PATH"

# Run gtfs-to-sql and import to database
if ! gtfs-to-sql --require-dependencies -- *.txt | psql -b; then
    echo "Error: Failed to import GTFS data"
    exit 1
fi

cd - >/dev/null

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

# Create route segments
echo "Creating route segments..."
if ! python src/static/split_into_segments.py "$GTFS_ZIP" \
    --start-date "$START_DATE" --end-date "$END_DATE" \
    --db-host "$DB_HOST" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"; then
    echo "Error: Failed to create route segments"
    exit 1
fi

echo "Route segments created successfully!"
echo "GTFS import and processing completed!"