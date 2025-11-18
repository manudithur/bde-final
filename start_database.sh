#!/bin/bash
# Quick start script for Linux - Starts database and sets it up
# Run this script to start everything

echo "========================================"
echo "Vancouver GTFS Database Setup"
echo "========================================"
echo ""

# Check if Docker is running
echo "Checking Docker..."
if ! docker ps > /dev/null 2>&1; then
    echo "✗ Docker is not running!"
    echo "Please start Docker and try again."
    exit 1
fi
echo "✓ Docker is running"

# Start containers
echo ""
echo "Starting Docker containers..."
docker-compose up -d

if [ $? -ne 0 ]; then
    echo "✗ Failed to start containers"
    exit 1
fi

echo "✓ Containers started"

# Wait for database to be ready
echo ""
echo "Waiting for database to be ready..."
for i in {1..30}; do
    if docker exec -i vancouver_gtfs_db pg_isready -U postgres > /dev/null 2>&1; then
        echo "✓ Database is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "✗ Database did not become ready in time"
        exit 1
    fi
    sleep 1
done

# Run database setup
echo ""
echo "Setting up database..."
./setup_database.sh

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Database is ready to use:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: gtfs"
echo "  User: postgres"
echo "  Password: postgres"
echo ""
echo "Next steps:"
echo "  1. Import GTFS data: cd static_analysis/data/gtfs_pruned"
echo "  2. Run: gtfs-to-sql --require-dependencies -- *.txt | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs"
echo "  3. Import MobilityDB: cat static_analysis/mobilitydb_import.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs"
echo ""
