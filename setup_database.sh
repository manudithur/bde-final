#!/bin/bash
# Database setup script

echo "Setting up Vancouver GTFS database..."
sleep 5

if ! docker ps --filter "name=vancouver_gtfs_db" --format "{{.Names}}" | grep -q vancouver_gtfs_db; then
    echo "Error: Database container not running!"
    echo "Run: docker-compose up -d"
    exit 1
fi

echo "Creating database and extensions..."

# Check if database exists by trying to connect to it
if docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs -c "SELECT 1;" > /dev/null 2>&1; then
    echo "Database 'gtfs' already exists"
else
    echo "Creating database 'gtfs'..."
    docker exec -i vancouver_gtfs_db psql -U postgres -c "CREATE DATABASE gtfs;"
    if [ $? -eq 0 ]; then
        echo "✓ Database 'gtfs' created"
    else
        echo "✗ Error: Failed to create database"
        exit 1
    fi
fi

echo "Installing PostGIS extension..."
if docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs -c "CREATE EXTENSION IF NOT EXISTS postgis;" > /dev/null 2>&1; then
    echo "✓ PostGIS extension installed"
else
    echo "✗ Warning: PostGIS installation failed"
    echo "  Trying to continue anyway..."
fi

echo "Installing MobilityDB extension..."
# Check if extension is available first
if docker exec -i vancouver_gtfs_db psql -U postgres -c "SELECT 1 FROM pg_available_extensions WHERE name = 'mobilitydb';" | grep -q 1; then
    docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs -c "CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;"
    if [ $? -eq 0 ]; then
        echo "✓ MobilityDB extension installed"
    else
        echo "✗ Warning: MobilityDB installation failed"
        echo "  Check logs: docker logs vancouver_gtfs_db"
    fi
else
    echo "✗ Error: MobilityDB extension not available in this image"
    echo "  Make sure you're using: mobilitydb/mobilitydb:latest"
    echo "  Current image:"
    docker inspect vancouver_gtfs_db --format='{{.Config.Image}}' 2>/dev/null || echo "  (container not found)"
fi

echo ""
echo "Database setup complete!"
