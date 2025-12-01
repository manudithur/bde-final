# Visualization Scripts

Python scripts that generate visualizations and analysis outputs from the GTFS database.

## Scripts

- **route_visualization.py** - Creates interactive route maps (all routes, by mode, density heatmap)
- **route_duplication_analysis.py** - Analyzes and visualizes route duplication
- **route_density_analysis.py** - Creates route density histograms
- **speed_analysis.py** - Analyzes vehicle speeds and creates speed distribution charts
- **stadium_proximity_analysis.py** - Analyzes trips near stadiums and landmarks

## Usage

Run individual scripts:
```bash
cd static_analysis/queries/analysis/visualization
python route_visualization.py
python route_duplication_analysis.py
python speed_analysis.py
python route_density_analysis.py
python stadium_proximity_analysis.py
```

Or run all analyses:
```bash
cd static_analysis/queries/analysis
python run_all_analyses.py
```

## Outputs

All outputs are saved to `queries/results/` organized by analysis type. See `results/README.md` for details.

## Requirements

All scripts require:
- Database connection (configured via `.env` file)
- Python dependencies from `static_analysis/requirements.txt`
- Database must have `data_loading/mobilitydb_import.sql` and `queries/analysis/spatial_queries.sql` run

