#!/usr/bin/env python3
"""
Route Duplication Analysis for Vancouver Transit
Identifies routes with high duplication that could potentially be eliminated
"""

import psycopg2
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import contextily as ctx
import folium
from folium import plugins
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results', 'route_duplication')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def fetch_route_duplication():
    """Fetch route duplication data - BUS routes only"""
    query = """
    SELECT 
        rd.route1,
        rd.route2,
        rd.shared_segments,
        rd.route1_total_segments,
        rd.route2_total_segments,
        rd.overlap_percentage
    FROM route_duplication rd
    JOIN routes r1 ON rd.route1 = r1.route_id
    JOIN routes r2 ON rd.route2 = r2.route_id
    WHERE r1.route_type = '3' AND r2.route_type = '3'
    ORDER BY rd.overlap_percentage DESC, rd.shared_segments DESC
    LIMIT 50;
    """
    
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def fetch_highly_duplicated_routes():
    """Fetch routes with highest duplication - BUS routes only"""
    query = """
    SELECT 
        hdr.route_id,
        hdr.num_duplicate_pairs,
        hdr.max_overlap_percentage,
        hdr.avg_overlap_percentage,
        hdr.total_shared_segments
    FROM highly_duplicated_routes hdr
    JOIN routes r ON hdr.route_id = r.route_id
    WHERE r.route_type = '3'
    ORDER BY hdr.num_duplicate_pairs DESC, hdr.max_overlap_percentage DESC;
    """
    
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def fetch_duplicated_segments():
    """Fetch segments that are shared between duplicated routes - BUS routes only"""
    # Get segments with multiple BUS routes directly
    query = """
    SELECT 
        rs.stop1_id || rs.stop2_id AS segment_id,
        COUNT(DISTINCT rs.route_id) AS num_routes_sharing,
        rs.seg_geom
    FROM route_segments rs
    JOIN routes r ON rs.route_id = r.route_id
    WHERE r.route_type = '3'
        AND rs.seg_geom IS NOT NULL
    GROUP BY rs.stop1_id, rs.stop2_id, rs.seg_geom
    HAVING COUNT(DISTINCT rs.route_id) >= 5
    ORDER BY COUNT(DISTINCT rs.route_id) DESC
    LIMIT 50;
    """
    
    conn = get_db_connection()
    try:
        gdf = gpd.read_postgis(query, conn, geom_col="seg_geom")
    except Exception as e:
        print(f"Error fetching duplicated segments: {e}")
        gdf = gpd.GeoDataFrame(columns=["segment_id", "num_routes_sharing", "seg_geom"], geometry="seg_geom")
    finally:
        conn.close()
    
    if gdf.empty:
        return gpd.GeoDataFrame(columns=["segment_id", "num_routes_sharing", "max_overlap_percentage", "seg_geom"], geometry="seg_geom")
    
    # Use num_routes as a proxy for overlap percentage (more routes = higher duplication)
    # Normalize to a percentage-like scale (5 routes = 30%, 10+ routes = 80%)
    max_routes = gdf['num_routes_sharing'].max()
    min_routes = gdf['num_routes_sharing'].min()
    if max_routes > min_routes:
        gdf['max_overlap_percentage'] = ((gdf['num_routes_sharing'] - min_routes) / (max_routes - min_routes) * 50) + 30  # Scale to 30-80%
        gdf['max_overlap_percentage'] = gdf['max_overlap_percentage'].clip(30, 80)
    else:
        gdf['max_overlap_percentage'] = 30
    
    gdf = gdf.set_geometry("seg_geom")
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    
    return gdf

def plot_duplication_heatmap(df):
    """Create geographic map showing duplicated segments"""
    if df.empty:
        print("No duplication data available")
        return
    
    # Fetch duplicated segments with geometry
    print("Fetching duplicated segments with geometry...")
    gdf_segments = fetch_duplicated_segments()
    
    if gdf_segments.empty:
        print("No duplicated segments with geometry found")
        return
    
    # Get top segments by overlap percentage
    gdf_top = gdf_segments.nlargest(100, 'max_overlap_percentage').copy()
    
    # Reproject to Web Mercator for basemap compatibility
    gdf_mercator = gdf_top.to_crs(epsg=3857)
    
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # Plot segments colored by overlap percentage
    gdf_mercator.plot(
        ax=ax,
        column="max_overlap_percentage",
        cmap="YlOrRd",
        linewidth=2.0,
        alpha=0.9,
        legend=True,
        legend_kwds={"label": "Maximum Overlap Percentage (%)", "shrink": 0.8},
    )
    
    # Add basemap
    try:
        ctx.add_basemap(ax, crs=gdf_mercator.crs, source=ctx.providers.CartoDB.Positron)
    except Exception as e:
        print(f"Warning: Could not add basemap: {e}")
    
    ax.set_title(
        'BUS Route Duplication Map - Segments with High Overlap Between Routes',
        fontsize=14,
        fontweight='bold'
    )
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.axis('off')  # Remove axes for cleaner map look
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_heatmap.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', format='png')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_overlap_distribution(df_dup):
    """Create histogram of overlap percentages"""
    if df_dup.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df_dup['overlap_percentage'], bins=30, color='#3794eb', edgecolor='black')
    ax.set_xlabel('Overlap Percentage (%)')
    ax.set_ylabel('Number of Route Pairs')
    ax.set_title('Distribution of BUS Route Overlap Percentages')
    ax.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_overlap_distribution.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_top_duplicated_pairs(df_dup):
    """Create bar chart of top duplicated route pairs"""
    if df_dup.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 8))
    top_pairs = df_dup.head(15)
    ax.barh(range(len(top_pairs)), top_pairs['overlap_percentage'], color='#cc0000')
    ax.set_yticks(range(len(top_pairs)))
    ax.set_yticklabels([f"{row['route1']} - {row['route2']}" 
                         for _, row in top_pairs.iterrows()], fontsize=9)
    ax.set_xlabel('Overlap Percentage (%)')
    ax.set_title('Top 15 Most Duplicated BUS Route Pairs')
    ax.grid(axis='x', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_top_pairs.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_routes_most_duplicates(df_high):
    """Create bar chart of routes with most duplicate pairs"""
    if df_high.empty:
        return
    
    fig, ax = plt.subplots(figsize=(10, 8))
    top_routes = df_high.head(15)
    ax.barh(range(len(top_routes)), top_routes['num_duplicate_pairs'], color='#009900')
    ax.set_yticks(range(len(top_routes)))
    ax.set_yticklabels(top_routes['route_id'], fontsize=9)
    ax.set_xlabel('Number of Duplicate Pairs')
    ax.set_title('BUS Routes with Most Duplicate Pairs (Candidates for Elimination)')
    ax.grid(axis='x', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_most_duplicates.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def plot_max_vs_avg_overlap(df_high):
    """Create bar chart comparing max vs average overlap"""
    if df_high.empty:
        return
    
    fig, ax = plt.subplots(figsize=(12, 6))
    top_routes = df_high.head(15)
    x_pos = range(len(top_routes))
    width = 0.35
    ax.bar([x - width/2 for x in x_pos], top_routes['max_overlap_percentage'], 
           width, label='Max Overlap', color='#cc0000', alpha=0.7)
    ax.bar([x + width/2 for x in x_pos], top_routes['avg_overlap_percentage'], 
           width, label='Avg Overlap', color='#0066cc', alpha=0.7)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(top_routes['route_id'], rotation=45, ha='right', fontsize=9)
    ax.set_ylabel('Overlap Percentage (%)')
    ax.set_title('Max vs Average Overlap for Highly Duplicated BUS Routes')
    ax.legend()
    ax.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_max_vs_avg.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved '{output_path}'")
    plt.close(fig)

def print_summary(df_dup, df_high):
    """Print summary statistics"""
    print("\n" + "="*60)
    print("ROUTE DUPLICATION ANALYSIS SUMMARY")
    print("="*60)
    
    if df_dup.empty:
        print("No route duplication data found.")
        return
    
    print(f"\nTotal route pairs with duplication: {len(df_dup)}")
    print(f"Average overlap percentage: {df_dup['overlap_percentage'].mean():.2f}%")
    print(f"Median overlap percentage: {df_dup['overlap_percentage'].median():.2f}%")
    print(f"Maximum overlap percentage: {df_dup['overlap_percentage'].max():.2f}%")
    
    high_overlap = df_dup[df_dup['overlap_percentage'] > 50]
    print(f"\nRoute pairs with >50% overlap: {len(high_overlap)}")
    
    if not df_high.empty:
        print(f"\nRoutes with high duplication (>=3 duplicate pairs): {len(df_high)}")
        print("\nTop 10 routes with highest duplication:")
        print(df_high.head(10)[['route_id', 'num_duplicate_pairs', 'max_overlap_percentage']].to_string(index=False))
    
    print("\n" + "="*60)

def fetch_duplicate_pairs_for_routes(route_ids):
    """Fetch duplicate pairs for specific routes"""
    if not route_ids:
        return pd.DataFrame()
    
    placeholders = ','.join(['%s'] * len(route_ids))
    query = f"""
    SELECT 
        route1,
        route2,
        shared_segments,
        route1_total_segments,
        route2_total_segments,
        overlap_percentage
    FROM route_duplication
    WHERE route1 IN ({placeholders}) OR route2 IN ({placeholders})
    ORDER BY overlap_percentage DESC;
    """
    
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn, params=route_ids + route_ids)
    conn.close()
    return df

def fetch_route_geometries(route_ids):
    """Fetch route geometries for specified routes"""
    if not route_ids:
        return []
    
    placeholders = ','.join(['%s'] * len(route_ids))
    query = f"""
    SELECT 
        route_id,
        route_short_name,
        route_long_name,
        mode_name,
        num_trips,
        ST_AsGeoJSON(route_geometry)::json AS geometry
    FROM route_visualization
    WHERE route_id IN ({placeholders})
        AND route_geometry IS NOT NULL;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, route_ids)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return rows

def create_duplication_map(df_highly_duplicated, df_duplication):
    """Create interactive HTML map showing routes with most duplicate pairs"""
    if df_highly_duplicated.empty:
        print("No highly duplicated routes to visualize")
        return None
    
    # Get top routes (limit to top 20 for performance)
    top_routes = df_highly_duplicated.head(20)
    route_ids = top_routes['route_id'].tolist()
    
    # Fetch route geometries
    print(f"Fetching geometries for {len(route_ids)} routes...")
    route_geometries = fetch_route_geometries(route_ids)
    
    if not route_geometries:
        print("No route geometries found")
        return None
    
    # Create a map centered on Vancouver
    m = folium.Map(location=[49.2827, -123.1207], zoom_start=11)
    
    # Create a route_info dict from top_routes DataFrame
    route_info = {}
    for _, row in top_routes.iterrows():
        route_info[row['route_id']] = {
            'num_duplicate_pairs': row['num_duplicate_pairs'],
            'max_overlap_percentage': row['max_overlap_percentage'],
            'avg_overlap_percentage': row['avg_overlap_percentage'],
            'total_shared_segments': row['total_shared_segments']
        }
    
    # Fetch duplicate pairs for these routes
    duplicate_pairs = fetch_duplicate_pairs_for_routes(route_ids)
    
    # Group routes by number of duplicate pairs for layer organization
    routes_by_duplication = {}
    for route_id in route_ids:
        if route_id in route_info:
            num_pairs = route_info[route_id]['num_duplicate_pairs']
            if num_pairs not in routes_by_duplication:
                routes_by_duplication[num_pairs] = []
            routes_by_duplication[num_pairs].append(route_id)
    
    # Create color palette based on number of duplicate pairs
    max_pairs = max(routes_by_duplication.keys()) if routes_by_duplication else 1
    
    # Create feature groups for different duplication levels
    for num_pairs in sorted(routes_by_duplication.keys(), reverse=True):
        intensity = min(1.0, num_pairs / max_pairs)
        color = f'#{int(255 * (1 - intensity * 0.5)):02x}{int(255 * intensity * 0.7):02x}00'  # Green to red gradient
        
        feature_group = folium.FeatureGroup(
            name=f'{num_pairs} duplicate pairs ({len(routes_by_duplication[num_pairs])} routes)'
        )
        
        for route_id in routes_by_duplication[num_pairs]:
            # Find geometry for this route
            route_geom = None
            route_short_name = route_id
            route_long_name = ""
            mode_name = "Unknown"
            num_trips = 0
            
            for rid, short_name, long_name, mode, trips, geom in route_geometries:
                if rid == route_id:
                    route_geom = geom
                    route_short_name = short_name or route_id
                    route_long_name = long_name or ""
                    mode_name = mode or "Unknown"
                    num_trips = trips or 0
                    break
            
            if route_geom and 'coordinates' in route_geom:
                # Get duplicate pairs info for this route
                pairs_info = duplicate_pairs[
                    (duplicate_pairs['route1'] == route_id) | 
                    (duplicate_pairs['route2'] == route_id)
                ]
                
                pairs_text = ""
                if not pairs_info.empty:
                    pairs_list = []
                    for _, pair_row in pairs_info.head(5).iterrows():
                        other_route = pair_row['route2'] if pair_row['route1'] == route_id else pair_row['route1']
                        pairs_list.append(f"{other_route} ({pair_row['overlap_percentage']:.1f}%)")
                    pairs_text = "<br>Duplicate pairs: " + ", ".join(pairs_list)
                    if len(pairs_info) > 5:
                        pairs_text += f" (+ {len(pairs_info) - 5} more)"
                
                # Create popup with duplication info
                popup_html = f"""
                <b>Route {route_short_name}</b><br>
                {route_long_name}<br>
                Mode: {mode_name}<br>
                Trips: {num_trips}<br>
                <b>Duplicate Pairs: {route_info[route_id]['num_duplicate_pairs']}</b><br>
                Max Overlap: {route_info[route_id]['max_overlap_percentage']:.1f}%<br>
                Avg Overlap: {route_info[route_id]['avg_overlap_percentage']:.1f}%
                {pairs_text}
                """
                
                coords = route_geom['coordinates']
                weight = 4 if num_pairs >= 7 else (3 if num_pairs >= 5 else 2)
                
                if route_geom['type'] == 'MultiLineString':
                    for line in coords:
                        folium.PolyLine(
                            locations=[[lat, lon] for lon, lat in line],
                            color=color,
                            weight=weight,
                            opacity=0.8,
                            popup=folium.Popup(popup_html, max_width=300)
                        ).add_to(feature_group)
                elif route_geom['type'] == 'LineString':
                    folium.PolyLine(
                        locations=[[lat, lon] for lon, lat in coords],
                        color=color,
                        weight=weight,
                        opacity=0.8,
                        popup=folium.Popup(popup_html, max_width=300)
                    ).add_to(feature_group)
        
        feature_group.add_to(m)
    
    # Add legend
    legend_html = """
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 200px; height: auto; 
                background-color: white; z-index:9999; font-size:14px;
                border:2px solid grey; padding: 10px">
    <h4>Route Duplication</h4>
    <p><b>Color intensity</b> indicates number of duplicate pairs</p>
    <p><i style="color:#FF0000;">Dark red</i> = Most duplicates (candidates for elimination)</p>
    <p><i style="color:#7FBF00;">Light green</i> = Fewer duplicates</p>
    <p><b>Line thickness</b> indicates duplication level</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    folium.LayerControl().add_to(m)
    return m

def main():
    print("Fetching route duplication data...")
    df_duplication = fetch_route_duplication()
    df_highly_duplicated = fetch_highly_duplicated_routes()
    
    if df_duplication.empty and df_highly_duplicated.empty:
        print("No duplication data found. Make sure you've run:")
        print("  1. data_loading/mobilitydb_import.sql")
        print("  2. qgis_queries/run_sql.py (or static_analysis/queries/run_all_analyses.py)")
        return
    
    print_summary(df_duplication, df_highly_duplicated)
    
    print("\nGenerating visualizations...")
    if not df_duplication.empty:
        plot_duplication_heatmap(df_duplication)
        plot_overlap_distribution(df_duplication)
        plot_top_duplicated_pairs(df_duplication)
    if not df_highly_duplicated.empty:
        plot_routes_most_duplicates(df_highly_duplicated)
        plot_max_vs_avg_overlap(df_highly_duplicated)
    
    # Create interactive HTML map
    if not df_highly_duplicated.empty:
        print("\nCreating duplication map...")
        duplication_map = create_duplication_map(df_highly_duplicated, df_duplication)
        if duplication_map:
            output_path = os.path.join(OUTPUT_DIR, 'route_duplication_map.html')
            duplication_map.save(output_path)
            print(f"Saved '{output_path}'")
    
    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()

