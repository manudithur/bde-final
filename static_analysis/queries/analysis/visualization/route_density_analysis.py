#!/usr/bin/env python3
"""
Route Density Analysis for Vancouver Transit
Generates histogram showing number of routes per segment
"""

import psycopg2
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()

# Output directory (go up 3 levels: visualization/ -> analysis/ -> queries/ -> static_analysis/)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'results', 'route_density')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Query the segment_route_density table
query = "SELECT num_routes FROM segment_route_density;"

def fetch_num_routes():
    """Fetch number of routes per segment from database"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    # Flatten to 1D list
    return [row[0] for row in data]

def plot_histogram(num_routes_list):
    """Plot histogram of routes per segment"""
    plt.figure(figsize=(10, 6))
    plt.hist(num_routes_list, bins=30, color="#3794eb", edgecolor="black")
    plt.xlabel('Number of Routes per Segment')
    plt.ylabel('Count of Segments')
    plt.title('Histogram of Number of Routes per Segment - Vancouver Transit')
    plt.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, 'route_density_histogram.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Histogram saved as '{output_path}'")
    plt.close()

if __name__ == "__main__":
    print("Fetching route density data...")
    num_routes_list = fetch_num_routes()
    print(f"Found {len(num_routes_list)} segments")
    if num_routes_list:
        plot_histogram(num_routes_list)
    else:
        print("No data found. Make sure you've run data_loading/mobilitydb_import.sql and queries/analysis/spatial_queries.sql")


