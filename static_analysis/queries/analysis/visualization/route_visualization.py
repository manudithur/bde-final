#!/usr/bin/env python3
"""
Route Visualization for Vancouver Transit (static PNG outputs)

Creates multiple PNG visualizations of routes from different perspectives:
- Mapa de todas las rutas coloreadas por modo de transporte.
- Mapa comparativo por modo (subplots).
- Mapa de densidad de rutas (segmentos coloreados por cantidad de rutas).

Todas las salidas se generan como imágenes PNG de alta resolución,
listas para ser incluidas en informes LaTeX.
"""

import os
import json

import psycopg2
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, MultiLineString
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "results",
    "route_visualization",
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def fetch_route_data_gdf() -> gpd.GeoDataFrame:
    """Fetch route visualization data and return as GeoDataFrame."""
    query = """
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        mode_name,
        num_trips,
        ST_AsGeoJSON(route_geometry) AS geometry
    FROM route_visualization
    WHERE route_geometry IS NOT NULL
    ORDER BY num_trips DESC;
    """

    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        return gpd.GeoDataFrame(columns=["route_id", "geometry"], geometry="geometry")

    geometries = []
    for geojson_str in df["geometry"]:
        if not geojson_str:
            geometries.append(None)
            continue
        try:
            geom = json.loads(geojson_str)
        except Exception:
            geometries.append(None)
            continue

        if "coordinates" not in geom:
            geometries.append(None)
            continue

        coords = geom["coordinates"]
        if geom["type"] == "LineString":
            geometries.append(LineString([(lon, lat) for lon, lat in coords]))
        elif geom["type"] == "MultiLineString":
            lines = [LineString([(lon, lat) for lon, lat in line]) for line in coords]
            geometries.append(MultiLineString(lines))
        else:
            geometries.append(None)

    gdf = gpd.GeoDataFrame(
        df.drop(columns=["geometry"]), geometry=geometries, crs="EPSG:4326"
    )
    gdf = gdf.dropna(subset=["geometry"])
    return gdf


def fetch_route_density_gdf() -> gpd.GeoDataFrame:
    """Fetch route density (segment_route_density) as GeoDataFrame."""
    query = """
    SELECT 
        stop1_id || stop2_id AS segment_id,
        num_routes,
        seg_geom
    FROM segment_route_density
    WHERE seg_geom IS NOT NULL;
    """

    conn = get_db_connection()
    gdf = gpd.read_postgis(query, conn, geom_col="seg_geom")
    conn.close()

    if gdf.empty:
        return gpd.GeoDataFrame(columns=["segment_id", "seg_geom"], geometry="seg_geom")

    gdf = gdf.set_geometry("seg_geom")
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    return gdf


def plot_all_routes(gdf: gpd.GeoDataFrame) -> str:
    """Plot all routes colored by mode."""
    if gdf.empty:
        print("No route data available for plotting.")
        return ""

    fig, ax = plt.subplots(figsize=(10, 8))

    modes = sorted(gdf["mode_name"].dropna().unique().tolist())
    cmap = plt.get_cmap("tab10")

    for i, mode in enumerate(modes):
        subset = gdf[gdf["mode_name"] == mode]
        if subset.empty:
            continue
        color = cmap(i % 10)
        subset.plot(
            ax=ax,
            color=color,
            linewidth=0.8,
            alpha=0.8,
            label=f"{mode} ({len(subset)} rutas)",
        )

    ax.set_title(
        "Red de Rutas de Vancouver por Modo de Transporte",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Longitud")
    ax.set_ylabel("Latitud")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(alpha=0.3)

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "route_map_all.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved '{output_path}'")
    return output_path


def plot_routes_by_mode(gdf: gpd.GeoDataFrame) -> str:
    """Plot separate subplots for each transport mode."""
    if gdf.empty:
        return ""

    modes = sorted(gdf["mode_name"].dropna().unique().tolist())
    n = len(modes)
    if n == 0:
        return ""

    cols = min(3, n)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 4 * rows), squeeze=False)

    for idx, mode in enumerate(modes):
        r = idx // cols
        c = idx % cols
        ax = axes[r][c]
        subset = gdf[gdf["mode_name"] == mode]
        subset.plot(ax=ax, linewidth=0.8, alpha=0.9)
        ax.set_title(f"{mode} (n={len(subset)})")
        ax.set_axis_off()

    # Hide unused subplots
    for idx in range(n, rows * cols):
        r = idx // cols
        c = idx % cols
        axes[r][c].set_visible(False)

    fig.suptitle(
        "Rutas por Modo de Transporte (Vista Comparativa)",
        fontsize=16,
        fontweight="bold",
    )
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    output_path = os.path.join(OUTPUT_DIR, "route_map_by_mode.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved '{output_path}'")
    return output_path


def plot_route_density(gdf_density: gpd.GeoDataFrame) -> str:
    """Plot segment density (num_routes) as colored lines."""
    if gdf_density.empty:
        print("No density data available for plotting.")
        return ""

    fig, ax = plt.subplots(figsize=(10, 8))

    gdf_density.plot(
        ax=ax,
        column="num_routes",
        cmap="inferno",
        linewidth=1.0,
        alpha=0.9,
        legend=True,
        legend_kwds={"label": "Cantidad de rutas por segmento"},
    )

    ax.set_title(
        "Densidad de Rutas por Segmento en Vancouver",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Longitud")
    ax.set_ylabel("Latitud")
    ax.grid(alpha=0.3)

    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "route_density_heatmap.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved '{output_path}'")
    return output_path


def main():
    print("Fetching route data...")
    routes_gdf = fetch_route_data_gdf()
    print(f"Found {len(routes_gdf)} routes")

    if routes_gdf.empty:
        print("No route data found. Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. queries/analysis/spatial_queries.sql")
        return

    print("Generating static route maps (PNG)...")
    plot_all_routes(routes_gdf)
    plot_routes_by_mode(routes_gdf)

    print("Fetching route density data...")
    density_gdf = fetch_route_density_gdf()
    print(f"Found {len(density_gdf)} segments for density analysis")
    if not density_gdf.empty:
        plot_route_density(density_gdf)

    print("\nAll PNG visualizations created successfully!")


if __name__ == "__main__":
    main()

