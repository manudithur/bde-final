# Library imports
import argparse
import gtfs_functions as gtfs
from sqlalchemy import create_engine

# Function that constructs the segments from the GTFS fles
def load_gtfs_feed(fle_path, start_date, end_date):
    # Load GTFS feed
    feed = gtfs.Feed(fle_path, start_date=start_date, end_date=end_date)
    segments_gdf = feed.segments
    return segments_gdf
# Function that saves the segments into a PostGIS table
def save_to_postgis(gdf, table_name, db_host, db_port, db_user, db_pass, db_name):
    # Construct the connection URL using the provided parameters
    db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    # Create the SQLAlchemy engine using the constructed URL
    engine = create_engine(db_url)
    # Save the GeoDataFrame to PostGIS
    gdf.to_postgis(table_name, engine, if_exists='replace')
    print(f"Data saved to {table_name} table in the {db_name} database.")
def main():
    parser = argparse.ArgumentParser(description='Split GTFS feed into segments and save to PostGIS')
    parser.add_argument('gtfs_folder', help='Path to the GTFS folder containing the data files')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD format)')
    parser.add_argument('--table-name', default='segments', help='PostgreSQL table name (default: segments)')
    parser.add_argument('--db-host', required=True, help='Database host')
    parser.add_argument('--db-port', default='5432', help='Database port (default: 5432)')
    parser.add_argument('--db-user', required=True, help='Database username')
    parser.add_argument('--db-pass', required=True, help='Database password')
    parser.add_argument('--db-name', required=True, help='Database name')
    
    args = parser.parse_args()
    
    # Execute the functions
    gtfs_segments = load_gtfs_feed(args.gtfs_folder, args.start_date, args.end_date)
    save_to_postgis(gtfs_segments, args.table_name, args.db_host, args.db_port, 
                    args.db_user, args.db_pass, args.db_name)

if __name__ == '__main__':
    main()
