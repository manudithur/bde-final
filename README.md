# Vancouver GTFS Analysis

Spatial database analysis of Vancouver transit data with both scheduled GTFS and GTFS-Realtime feeds.

---

## Setup

### Install Dependencies
```bash
pip install -r requirements.txt
pip install -r static_analysis/requirements.txt
pip install -r realtime_analysis/requirements.txt
npm install -g gtfs-via-postgres
```

### Configure Environment
Create a `.env` file in the repo root:
```bash
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=postgres
PGDATABASE=gtfs
```

### Start the Database
```bash
# Linux/WSL
chmod +x start_database.sh setup_database.sh check_mobilitydb.sh
./start_database.sh

# Or manually
docker-compose up -d
./setup_database.sh
```
If you switch PostGIS images, reset the volumes first:
```bash
docker-compose down -v
docker-compose up -d
./setup_database.sh
```

### Useful Database Commands
```bash
# Connect
docker exec -it vancouver_gtfs_db psql -U postgres -d gtfs

# Run arbitrary SQL
cat file.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs

# Restart services
docker-compose down
docker-compose up -d
```

---

## GTFS Static Workflow

1. **Download & preprocess schedule data**
   ```bash
   bash static_analysis/data/download_data.sh
   ```
   Produces `static_analysis/data/gtfs_pruned/`.

2. **Import GTFS tables**
   ```bash
   cd static_analysis/data/gtfs_pruned
   gtfs-to-sql --require-dependencies -- *.txt | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
   ```

3. **Load MobilityDB schema & derived tables**
   ```bash
   cd ../..
   cat static_analysis/data_loading/mobilitydb_import.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
   ```

4. **Run analysis queries**
   ```bash
   cat static_analysis/queries/analysis/spatial_queries.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
   ```

5. **Generate static visualizations**
   ```bash
   cd static_analysis/queries/analysis
   # Run all analyses at once
   python run_all_analyses.py
   
   # Or run individually:
   python visualization/route_visualization.py          # Interactive route maps
   python visualization/route_duplication_analysis.py   # Route duplication analysis
   python visualization/speed_analysis.py               # Speed analysis
   python visualization/route_density_analysis.py       # Route density
   python visualization/stadium_proximity_analysis.py   # Stadium proximity
   ```
   
   **Outputs:**
   - All results are saved to `static_analysis/queries/results/` organized by analysis type
   - Interactive HTML maps: route maps, route duplication maps, stadium proximity maps
   - Statistical charts: duplication heatmaps, speed distributions, route density histograms
   - See `static_analysis/queries/results/README.md` for results organization
   - See `static_analysis/queries/README.md` for query documentation

6. **Inspect in GIS (optional)**
   - Install QGIS from https://qgis.org/
   - Layer → Add Layer → Add PostGIS Layer
   - Connection: Host `localhost`, Port `5432`, DB `gtfs`, User `postgres`, Password `postgres`
   - Load tables such as `stops`, `route_segments`, `scheduled_trips_mdb`

---

## GTFS Realtime Workflow

Use the `realtime_analysis` package to capture TransLink GTFS-Realtime feeds for the same routes analyzed above.

1. **Install extras & set API key**
   ```bash
   pip install -r realtime_analysis/requirements.txt
   export TRANSLINK_GTFSR_API_KEY="your-translink-api-key"
   # Default endpoints:
   #   Positions: https://gtfsapi.translink.ca/v3/gtfsposition
   #   Trip updates: https://gtfsapi.translink.ca/v3/gtfsrealtime
   # Override via GTFS_VEHICLE_POSITIONS_URL / GTFS_TRIP_UPDATES_URL if needed.
   ```

2. **Create realtime tables**
   ```bash
   cat realtime_analysis/realtime_schema.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
   ```

3. **Ingest GTFS-Realtime feeds**
   ```bash
   python -m realtime_analysis.ingest_realtime \
     --route-short-name 99 \
     --duration-minutes 20 \
     --poll-interval 15
   ```
   Use the same route filters as the static study. Pass `--once` for a single snapshot.

4. **Build map-matched actual trajectories**
   ```bash
   python -m realtime_analysis.build_realtime_trajectories --hours 2 --route-short-name 99
   ```
   Raw GPS points are deduplicated and snapped onto the scheduled shape before
   upserting into `realtime_trips_mdb`.

5. **Compare schedule vs actual**
   ```bash
   python -m realtime_analysis.analyze_realtime --route-short-name 99
   ```
   Outputs written to `realtime_analysis/output/`:
   - `trajectory_<trip>.html`
   - `speed_delta_map_<trip>.html`
   - `travel_time_<trip>.png`
   - `segment_metrics_<trip>.csv`

---

## Project Structure

```
.
├── static_analysis/          # Static schedule analysis
│   ├── data/                # GTFS download & preprocessing scripts
│   │   ├── download_data.sh
│   │   └── gtfs_pruned/
│   ├── data_loading/        # Database schema setup
│   │   └── mobilitydb_import.sql
│   ├── queries/             # Analysis queries and visualizations
│   │   ├── analysis/        # Analysis queries
│   │   │   ├── spatial_queries.sql
│   │   │   └── visualization/  # Python scripts for visualizing queries
│   │   ├── run_all_analyses.py  # Run all visualizations
│   │   └── results/         # Output files (PNG, HTML)
│   ├── utility/             # Utility scripts
│   │   ├── check_data.sql
│   │   └── fix_trips_shape_id.sql
├── realtime_analysis/       # Real-time ingestion & comparison
│   ├── realtime_schema.sql
│   ├── ingest_realtime.py
│   ├── build_realtime_trajectories.py
│   ├── analyze_realtime.py
│   └── utils.py
├── docker-compose.yml
├── setup_database.sh
└── start_database.sh
```

---

## Troubleshooting

- **Line ending errors (Linux/WSL):**
  ```bash
  find . -name "*.sh" -exec sed -i 's/\r$//' {} \; -exec chmod +x {} \;
  ```
- **Database missing:**
  ```bash
  docker exec -i vancouver_gtfs_db psql -U postgres -c "CREATE DATABASE gtfs;"
  docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs -c "CREATE EXTENSION IF NOT EXISTS postgis;"
  ```
- **Missing tables:** Run the static GTFS import (steps 2–3) before MobilityDB scripts.
- **`valid_shape_id` constraint errors:** Ensure `gtfs-to-sql` ran with `--trips-without-shape-id`. Otherwise run:
  ```bash
  cat static_analysis/utility/fix_trips_shape_id.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
  ```
- **MobilityDB extension unavailable:**
  ```bash
  docker-compose down -v
  docker-compose up -d
  chmod +x check_mobilitydb.sh
  ./check_mobilitydb.sh
  docker logs vancouver_gtfs_db
  ```
