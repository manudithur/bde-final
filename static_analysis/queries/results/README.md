# Analysis Results

This directory contains all output files from the static GTFS analyses, organized by analysis type.

## Directory Structure

```
results/
├── route_visualization/       # Route visualization maps
│   ├── route_map_all.html
│   ├── route_map_by_mode.html
│   └── route_density_heatmap.html
├── route_duplication/         # Route duplication analysis
│   ├── route_duplication_heatmap.png
│   └── route_duplication_statistics.png
├── speed_analysis/            # Speed analysis
│   ├── speed_analysis.png
│   └── high_speed_routes.png
├── route_density/             # Route density analysis
│   └── route_density_histogram.png
└── stadium_proximity/         # Stadium proximity analysis
    └── stadium_proximity_analysis.png
```

## File Types

### Interactive HTML Maps
- `.html` files - Open in a web browser to view interactive maps
- Use layer controls to show/hide different route types or zones
- Zoom and pan to explore the transit network

### Statistical Charts
- `.png` files - Static charts showing analysis results
- High resolution (300 DPI) suitable for reports

## Analysis Descriptions

### Route Visualization
- **route_map_all.html**: All routes on a single map, color-coded by transport mode
- **route_map_by_mode.html**: Routes grouped by mode with layer controls
- **route_density_heatmap.html**: Heatmap showing route density across the city

### Route Duplication
- **route_duplication_heatmap.png**: Heatmap showing overlap percentages between routes
- **route_duplication_statistics.png**: Multiple charts showing duplication statistics

### Speed Analysis
- **speed_analysis.png**: Speed distribution charts and statistics
- **high_speed_routes.png**: Routes with segments exceeding 60 km/h

### Route Density
- **route_density_histogram.png**: Histogram of routes per segment

### Stadium Proximity
- **stadium_proximity_analysis.png**: Trips near stadiums by time intervals

## Notes

- All HTML maps require an internet connection to load base map tiles

