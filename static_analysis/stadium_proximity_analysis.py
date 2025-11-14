#!/usr/bin/env python3
"""
Stadium Proximity Analysis for Vancouver Transit
Analyzes trips near stadiums/landmarks by time intervals
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "gtfs")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "")

# Query that joins trips and distance of each stadium
query = """
SELECT sti.stadium_name, sti.team, sti.interv, sti.trips_nearby, tcs.distance_km
FROM stadium_trip_intervals sti
JOIN trajectories_center_stadiums tcs
  ON sti.stadium_name = tcs.name
ORDER BY sti.interv, sti.stadium_name;
"""


def fetch_stadium_trips():
    """Fetch stadium trip data from database"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(
        data, columns=["stadium_name", "team", "interv", "trips_nearby", "distance_km"]
    )
    return df


def plot_stadium_trips(df):
    """Plot trips near stadiums by time intervals"""
    # Sort stadiums by average distance (closest = darker)
    avg_distance = (
        df.groupby("stadium_name")
        .agg({"distance_km": "mean"})
        .sort_values("distance_km", ascending=True)  # Closer = darker
        .reset_index()
    )

    n = len(avg_distance)
    cmap = plt.cm.magma
    # Color mapping: closest stadium gets darkest color
    colors = [cmap(i / (n - 1)) for i in range(n)] if n > 1 else [cmap(0)]
    color_dict = dict(zip(avg_distance["stadium_name"], colors))

    pivot = df.pivot(
        index="interv", columns="stadium_name", values="trips_nearby"
    ).fillna(0)

    plt.figure(figsize=(14, 8))

    for stadium in avg_distance["stadium_name"]:
        if stadium in pivot.columns:
            team = df[df["stadium_name"] == stadium]["team"].iloc[0]
            distance = avg_distance.set_index('stadium_name').loc[stadium, 'distance_km']
            label = f"{stadium} ({team}) - {distance:.2f} km"
            plt.plot(
                pivot.index,
                pivot[stadium],
                marker="o",
                label=label,
                color=color_dict[stadium],
                linewidth=2,
                markersize=6
            )

    plt.title("Number of Transit Trips Near Stadiums/Landmarks by Time Interval - Vancouver", 
              fontsize=14, fontweight='bold')
    plt.xlabel("Time Interval (2-hour windows)", fontsize=12)
    plt.ylabel("Number of Trips Nearby", fontsize=12)
    plt.legend(
        title="Stadium (Distance from City Center)",
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
        ncol=1,
        fontsize=9
    )
    plt.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('stadium_proximity_analysis.png', dpi=300, bbox_inches='tight')
    print("Chart saved as 'stadium_proximity_analysis.png'")
    plt.show()


if __name__ == "__main__":
    print("Fetching stadium proximity data...")
    df = fetch_stadium_trips()
    if not df.empty:
        print(f"Found data for {df['stadium_name'].nunique()} stadiums")
        plot_stadium_trips(df)
    else:
        print("No data found. Make sure you've run:")
        print("  1. mobilitydb_import.sql")
        print("  2. spatial_queries.sql (which creates stadium_trip_intervals)")


