#!/usr/bin/env python3
"""
Route Duplication Analysis for Vancouver Transit
Identifies routes with high duplication that could potentially be eliminated
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
    """Fetch route duplication data"""
    query = """
    SELECT 
        route1,
        route2,
        shared_segments,
        route1_total_segments,
        route2_total_segments,
        overlap_percentage
    FROM route_duplication
    ORDER BY overlap_percentage DESC, shared_segments DESC
    LIMIT 50;
    """
    
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def fetch_highly_duplicated_routes():
    """Fetch routes with highest duplication"""
    query = """
    SELECT 
        route_id,
        num_duplicate_pairs,
        max_overlap_percentage,
        avg_overlap_percentage,
        total_shared_segments
    FROM highly_duplicated_routes
    ORDER BY num_duplicate_pairs DESC, max_overlap_percentage DESC;
    """
    
    conn = get_db_connection()
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def plot_duplication_heatmap(df):
    """Create heatmap of route duplication"""
    if df.empty:
        print("No duplication data available")
        return
    
    # Create a matrix of route overlaps
    routes = sorted(set(df['route1'].tolist() + df['route2'].tolist()))
    
    # Create overlap matrix with float dtype to avoid warnings
    overlap_matrix = pd.DataFrame(0.0, index=routes, columns=routes, dtype=float)
    for _, row in df.iterrows():
        r1, r2 = row['route1'], row['route2']
        overlap_matrix.loc[r1, r2] = float(row['overlap_percentage'])
        overlap_matrix.loc[r2, r1] = float(row['overlap_percentage'])
    
    # Only show routes with at least one overlap > 20%
    significant_routes = overlap_matrix.index[overlap_matrix.max(axis=1) > 20]
    if len(significant_routes) > 0:
        overlap_matrix = overlap_matrix.loc[significant_routes, significant_routes]
    
    plt.figure(figsize=(14, 12))
    sns.heatmap(overlap_matrix, annot=True, fmt='.1f', cmap='YlOrRd', 
                cbar_kws={'label': 'Overlap Percentage (%)'}, 
                square=True, linewidths=0.5)
    plt.title('Route Duplication Heatmap - Overlap Percentage Between Routes', 
              fontsize=14, fontweight='bold')
    plt.xlabel('Route ID', fontsize=12)
    plt.ylabel('Route ID', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_heatmap.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved '{output_path}'")
    plt.close()

def plot_duplication_statistics(df_dup, df_high):
    """Create statistics plots for route duplication"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Distribution of overlap percentages
    if not df_dup.empty:
        axes[0, 0].hist(df_dup['overlap_percentage'], bins=30, color='#3794eb', edgecolor='black')
        axes[0, 0].set_xlabel('Overlap Percentage (%)')
        axes[0, 0].set_ylabel('Number of Route Pairs')
        axes[0, 0].set_title('Distribution of Route Overlap Percentages')
        axes[0, 0].grid(axis='y', alpha=0.5)
    
    # 2. Top duplicated route pairs
    if not df_dup.empty:
        top_pairs = df_dup.head(15)
        axes[0, 1].barh(range(len(top_pairs)), top_pairs['overlap_percentage'], color='#cc0000')
        axes[0, 1].set_yticks(range(len(top_pairs)))
        axes[0, 1].set_yticklabels([f"{row['route1']} - {row['route2']}" 
                                     for _, row in top_pairs.iterrows()], fontsize=8)
        axes[0, 1].set_xlabel('Overlap Percentage (%)')
        axes[0, 1].set_title('Top 15 Most Duplicated Route Pairs')
        axes[0, 1].grid(axis='x', alpha=0.5)
    
    # 3. Routes with most duplicate pairs
    if not df_high.empty:
        top_routes = df_high.head(15)
        axes[1, 0].barh(range(len(top_routes)), top_routes['num_duplicate_pairs'], color='#009900')
        axes[1, 0].set_yticks(range(len(top_routes)))
        axes[1, 0].set_yticklabels(top_routes['route_id'], fontsize=8)
        axes[1, 0].set_xlabel('Number of Duplicate Pairs')
        axes[1, 0].set_title('Routes with Most Duplicate Pairs (Candidates for Elimination)')
        axes[1, 0].grid(axis='x', alpha=0.5)
    
    # 4. Average vs Max overlap for highly duplicated routes
    if not df_high.empty:
        top_routes = df_high.head(15)
        x_pos = range(len(top_routes))
        width = 0.35
        axes[1, 1].bar([x - width/2 for x in x_pos], top_routes['max_overlap_percentage'], 
                       width, label='Max Overlap', color='#cc0000', alpha=0.7)
        axes[1, 1].bar([x + width/2 for x in x_pos], top_routes['avg_overlap_percentage'], 
                       width, label='Avg Overlap', color='#0066cc', alpha=0.7)
        axes[1, 1].set_xticks(x_pos)
        axes[1, 1].set_xticklabels(top_routes['route_id'], rotation=45, ha='right', fontsize=8)
        axes[1, 1].set_ylabel('Overlap Percentage (%)')
        axes[1, 1].set_title('Max vs Average Overlap for Highly Duplicated Routes')
        axes[1, 1].legend()
        axes[1, 1].grid(axis='y', alpha=0.5)
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_duplication_statistics.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved '{output_path}'")
    plt.close()

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
        print("  2. queries/analysis/spatial_queries.sql")
        return
    
    print_summary(df_duplication, df_highly_duplicated)
    
    print("\nGenerating visualizations...")
    plot_duplication_statistics(df_duplication, df_highly_duplicated)
    
    if not df_duplication.empty:
        plot_duplication_heatmap(df_duplication)
    
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

