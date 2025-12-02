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
   
   **Visualization Scripts:**
   - **route_visualization.py** - Creates interactive route maps (all routes, by mode, density heatmap)
   - **route_duplication_analysis.py** - Analyzes and visualizes route duplication
   - **route_density_analysis.py** - Creates route density histograms
   - **speed_analysis.py** - Analyzes vehicle speeds and creates speed distribution charts
   - **stadium_proximity_analysis.py** - Analyzes transit access near stadiums and landmarks
   
   **Outputs:**
   - All results are saved to `static_analysis/queries/results/` organized by analysis type
   - Interactive HTML maps: route maps, route duplication maps, stadium proximity maps
   - Statistical charts: duplication heatmaps, speed distributions, route density histograms
   - See `static_analysis/queries/results/README.md` for list of generated result files
   
   **Requirements:**
   - Database connection (configured via `.env` file)
   - Python dependencies from `static_analysis/requirements.txt`
   - Database must have `data_loading/mobilitydb_import.sql` and `queries/analysis/spatial_queries.sql` run

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

3. **Run realtime analysis queries (optional but recommended)**
   ```bash
   cat realtime_analysis/queries/analysis/realtime_queries.sql | docker exec -i vancouver_gtfs_db psql -U postgres -d gtfs
   ```
   This creates materialized views for faster analysis queries. The visualization scripts will use these views if available, falling back to inline queries otherwise.

4. **Ingest GTFS-Realtime feeds**
   ```bash
   python -m realtime_analysis.ingest_realtime \
     --duration-minutes 20 \
     --poll-interval 30
   ```
   Use the same route filters as the static study. Pass `--once` for a single snapshot.

5. **Build map-matched actual trajectories**
   ```bash
   python -m realtime_analysis.build_realtime_trajectories --hours 2 --route-short-name 99
   ```
   Raw GPS points are deduplicated and snapped onto the scheduled shape before
   upserting into `realtime_trips_mdb`.

6. **Compare schedule vs actual (single trip)**
   ```bash
   python -m realtime_analysis.analyze_realtime --route-short-name 99
   ```
   Outputs written to `realtime_analysis/queries/results/single_trip/`:
   - `trajectory_<trip>.html`
   - `speed_delta_map_<trip>.html`
   - `travel_time_<trip>.png`
   - `segment_metrics_<trip>.csv`

7. **Run comprehensive realtime analyses**
   ```bash
   cd realtime_analysis/queries/analysis
   # Run all analyses at once
   python run_all_analyses.py
   
   # Or run individually:
   python visualization/speed_vs_schedule_analysis.py    # Scheduled vs actual speeds
   python visualization/schedule_times_analysis.py       # Scheduled vs actual times
   python visualization/delay_segments_analysis.py       # Traffic/congestion patterns
   python visualization/headway_analysis.py              # Bus bunching and headway analysis
   ```
   
   **Visualization Scripts:**
   - **speed_vs_schedule_analysis.py** - Compares planned velocities with actual observed speeds
   - **schedule_times_analysis.py** - Compares scheduled arrival/departure times with actual times
   - **delay_segments_analysis.py** - Analyzes traffic patterns and congestion by time/location
   - **headway_analysis.py** - Analyzes bus bunching and headway regularity
   
   **Outputs:**
   - All results saved to `realtime_analysis/queries/results/` organized by analysis type
   - Interactive HTML maps and charts (Plotly-based)
   - CSV files with detailed metrics
   - See `realtime_analysis/queries/results/README.md` for list of generated result files
   
   **Requirements:**
   - Database connection (configured via `.env` file)
   - Python dependencies from `realtime_analysis/requirements.txt`
   - Realtime data ingested via `ingest_realtime.py`
   - Static schedule loaded (required for comparisons)

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
│   │   ├── analysis/        # Analysis queries and scripts
│   │   │   ├── spatial_queries.sql  # SQL queries with materialized views
│   │   │   ├── run_all_analyses.py  # Run all visualization scripts
│   │   │   └── visualization/  # Python scripts for visualizing queries
│   │   └── results/         # Output files (PNG, HTML) - see results/README.md
│   ├── utility/             # Utility scripts
│   │   ├── check_data.sql
│   │   └── fix_trips_shape_id.sql
├── realtime_analysis/       # Real-time ingestion & comparison
│   ├── realtime_schema.sql
│   ├── ingest_realtime.py
│   ├── build_realtime_trajectories.py
│   ├── analyze_realtime.py
│   ├── utils.py
│   ├── queries/             # Realtime analysis queries
│   │   ├── analysis/        # Analysis queries and scripts
│   │   │   ├── realtime_queries.sql  # SQL queries with materialized views
│   │   │   ├── run_all_analyses.py   # Run all visualization scripts
│   │   │   └── visualization/  # Python scripts for visualizing queries
│   │   └── results/         # Output files (HTML, CSV, PNG) - see results/README.md
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
- **Realtime queries slow:** Run `realtime_queries.sql` to create materialized views for better performance.
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
