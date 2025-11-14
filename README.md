# Vancouver GTFS Analysis

Spatial database analysis of Vancouver transit data with proximity analysis.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
pip install -r static_analysis/requirements.txt
pip install -r live_transit/requirements.txt
npm install -g gtfs-via-postgres
```

### 2. Setup Environment

Create `.env` file:
```bash
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=postgres
PGDATABASE=gtfs
```

### 3. Start Database

```bash
# Linux/WSL
chmod +x start_database.sh setup_database.sh check_mobilitydb.sh
./start_database.sh

# Or manually
docker-compose up -d
./setup_database.sh
```

**Important:** If you're switching from a different PostGIS image, you may need to remove old volumes:
```bash
docker-compose down -v  # Removes volumes with old data
docker-compose up -d    # Starts fresh with MobilityDB image
./setup_database.sh     # Sets up database and extensions
```

### 4. Download & Process Data

```bash
bash data/download_data.sh
```

### 5. Import GTFS Data

```bash
cd data/gtfs_pruned
gtfs-to-sql --require-dependencies -- *.txt | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
```

### 6. Import MobilityDB Schema

```bash
cd ../..
cat static_analysis/mobilitydb_import.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
```

### 7. Run Analysis Queries

```bash
cat static_analysis/spatial_queries.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
```

### 8. Generate Visualizations

```bash
cd static_analysis
python route_density_analysis.py
python stadium_proximity_analysis.py
```

### 9. View Data in GIS Applications

**QGIS (Recommended - Free & Open Source):**
- Download: https://qgis.org/
- Connect to database:
  1. Layer → Add Layer → Add PostGIS Layers
  2. New connection:
     - Name: Vancouver GTFS
     - Host: localhost
     - Port: 5432
     - Database: gtfs
     - Username: postgres
     - Password: postgres
  3. Select tables to view (e.g., `stops`, `route_segments`, `scheduled_trips_mdb`)

## Project Structure

```
.
├── data/                    # GTFS data
│   ├── download_data.sh    # Download and process GTFS
│   └── gtfs_pruned/         # Processed GTFS files
├── static_analysis/          # Static schedule analysis
│   ├── mobilitydb_import.sql
│   ├── spatial_queries.sql
│   └── *.py                 # Visualization scripts
├── live_transit/            # Real-time analysis
│   ├── realtime_mobilitydb_import.sql
│   ├── realtime_queries.sql
│   └── *.py                 # Analysis scripts
├── docker-compose.yml       # Database & Valhalla
├── setup_database.sh        # Database setup
└── start_database.sh        # Quick start
```

## Database Commands

```bash
# Connect to database
docker exec -it vancouver_gtfs_db psql -U postgres -d gtfs

# Run SQL file
cat file.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs

# Stop/Start
docker-compose down
docker-compose up -d
```

## Troubleshooting

**Line ending errors (Linux/WSL):**
```bash
find . -name "*.sh" -exec sed -i 's/\r$//' {} \; -exec chmod +x {} \;
```

**Database doesn't exist:**
```bash
docker exec -i vancouver_gtfs_db psql -U postgres -c "CREATE DATABASE gtfs;"
docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs -c "CREATE EXTENSION IF NOT EXISTS postgis;"
```

**Missing tables:** Run Step 5 (GTFS import) before Step 6 (MobilityDB import).

**`valid_shape_id` constraint error:** Make sure you use the `--trips-without-shape-id` flag in Step 5. If you already imported without it, run:
```bash
cat fix_trips_shape_id.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
```

**MobilityDB extension not available:**
```bash
# 1. Make sure you're using the correct image
docker-compose down -v  # Remove old volumes
docker-compose up -d    # Start with new image

# 2. Check if extension is available
chmod +x check_mobilitydb.sh
./check_mobilitydb.sh

# 3. If still failing, check container logs
docker logs vancouver_gtfs_db
```
