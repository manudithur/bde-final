#!/usr/bin/env python3
"""
Clean realtime GTFS data from the database.

This script allows you to remove old realtime data based on various criteria:
- Delete data older than a specified number of days/hours
- Delete data within a specific date range
- Delete all realtime data (with confirmation)
- Target specific tables or all realtime tables
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Set

from realtime_analysis.utility.config import Settings, load_settings
from realtime_analysis.utility.utils import get_connection

LOG = logging.getLogger("realtime_analysis.clean")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# Realtime tables that can be cleaned
REALTIME_TABLES = {
    "vehicle_positions": "rt_vehicle_positions",
    "trip_updates": "rt_trip_updates",
    "trips": "realtime_trips_mdb",
}

# Timestamp columns for each table
TIMESTAMP_COLUMNS = {
    "rt_vehicle_positions": "entity_timestamp",
    "rt_trip_updates": "entity_timestamp",
    "realtime_trips_mdb": "starttime",
}


def get_table_stats(conn, table_name: str) -> dict:
    """Get statistics about a table."""
    with conn.cursor() as cur:
        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cur.fetchone()[0]

        # Get date range if table has timestamp column
        timestamp_col = TIMESTAMP_COLUMNS.get(table_name)
        date_range = None
        if timestamp_col:
            cur.execute(
                f"""
                SELECT 
                    MIN({timestamp_col}) AS min_ts,
                    MAX({timestamp_col}) AS max_ts
                FROM {table_name}
                WHERE {timestamp_col} IS NOT NULL
                """
            )
            result = cur.fetchone()
            if result and result[0]:
                date_range = (result[0], result[1])

        return {
            "row_count": row_count,
            "date_range": date_range,
        }


def print_stats(conn, tables: Set[str], label: str = "Current") -> None:
    """Print statistics for the specified tables."""
    LOG.info(f"\n{label} Statistics:")
    LOG.info("=" * 60)
    for table_name in sorted(tables):
        stats = get_table_stats(conn, table_name)
        LOG.info(f"\n{table_name}:")
        LOG.info(f"  Rows: {stats['row_count']:,}")
        if stats["date_range"]:
            min_ts, max_ts = stats["date_range"]
            LOG.info(f"  Date range: {min_ts} to {max_ts}")
        else:
            LOG.info("  Date range: N/A (no timestamp data)")


def clean_by_age(
    conn,
    table_name: str,
    older_than: timedelta,
    timestamp_col: str,
    dry_run: bool = False,
) -> int:
    """Delete rows older than the specified age."""
    cutoff_time = datetime.now(timezone.utc) - older_than

    with conn.cursor() as cur:
        # Count rows to be deleted
        cur.execute(
            f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE {timestamp_col} < %s
            """,
            (cutoff_time,),
        )
        count = cur.fetchone()[0]

        if count == 0:
            LOG.info(f"  No rows to delete in {table_name}")
            return 0

        if dry_run:
            LOG.info(f"  [DRY RUN] Would delete {count:,} rows from {table_name}")
            return count

        # Delete the rows
        cur.execute(
            f"""
            DELETE FROM {table_name}
            WHERE {timestamp_col} < %s
            """,
            (cutoff_time,),
        )
        deleted = cur.rowcount
        conn.commit()
        LOG.info(f"  Deleted {deleted:,} rows from {table_name}")
        return deleted


def clean_by_date_range(
    conn,
    table_name: str,
    start_date: datetime,
    end_date: datetime,
    timestamp_col: str,
    dry_run: bool = False,
) -> int:
    """Delete rows within the specified date range."""
    with conn.cursor() as cur:
        # Count rows to be deleted
        cur.execute(
            f"""
            SELECT COUNT(*) 
            FROM {table_name}
            WHERE {timestamp_col} >= %s AND {timestamp_col} <= %s
            """,
            (start_date, end_date),
        )
        count = cur.fetchone()[0]

        if count == 0:
            LOG.info(f"  No rows to delete in {table_name}")
            return 0

        if dry_run:
            LOG.info(f"  [DRY RUN] Would delete {count:,} rows from {table_name}")
            return count

        # Delete the rows
        cur.execute(
            f"""
            DELETE FROM {table_name}
            WHERE {timestamp_col} >= %s AND {timestamp_col} <= %s
            """,
            (start_date, end_date),
        )
        deleted = cur.rowcount
        conn.commit()
        LOG.info(f"  Deleted {deleted:,} rows from {table_name}")
        return deleted


def clean_all(conn, table_name: str, dry_run: bool = False) -> int:
    """Delete all rows from a table."""
    with conn.cursor() as cur:
        # Count rows to be deleted
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]

        if count == 0:
            LOG.info(f"  No rows to delete in {table_name}")
            return 0

        if dry_run:
            LOG.info(f"  [DRY RUN] Would delete {count:,} rows from {table_name}")
            return count

        # Delete all rows
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        LOG.info(f"  Deleted all {count:,} rows from {table_name}")
        return count


def clean_realtime_data(
    settings: Settings,
    *,
    older_than_days: Optional[float] = None,
    older_than_hours: Optional[float] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    all_data: bool = False,
    tables: Optional[List[str]] = None,
    dry_run: bool = False,
    confirm: bool = False,
) -> None:
    """Main cleanup function."""

    # Determine which tables to clean
    if tables:
        table_names = {REALTIME_TABLES.get(t, t) for t in tables}
        # Validate table names
        invalid = table_names - set(REALTIME_TABLES.values())
        if invalid:
            LOG.error(f"Invalid table names: {invalid}")
            LOG.error(f"Valid options: {list(REALTIME_TABLES.keys())}")
            sys.exit(1)
    else:
        table_names = set(REALTIME_TABLES.values())

    conn = get_connection(settings)

    try:
        # Show current statistics
        print_stats(conn, table_names, "Before cleanup")

        # Confirm destructive operations
        if all_data and not confirm:
            LOG.warning("\n⚠️  WARNING: This will delete ALL data from the selected tables!")
            response = input("Type 'yes' to confirm: ")
            if response.lower() != "yes":
                LOG.info("Cleanup cancelled.")
                return

        if not dry_run and (all_data or older_than_days or older_than_hours or start_date):
            response = input("\nProceed with cleanup? (yes/no): ")
            if response.lower() != "yes":
                LOG.info("Cleanup cancelled.")
                return

        LOG.info("\nStarting cleanup...")
        total_deleted = 0

        for table_name in sorted(table_names):
            timestamp_col = TIMESTAMP_COLUMNS.get(table_name)

            if all_data:
                deleted = clean_all(conn, table_name, dry_run=dry_run)
                total_deleted += deleted

            elif older_than_days or older_than_hours:
                if not timestamp_col:
                    LOG.warning(
                        f"  Skipping {table_name}: no timestamp column available"
                    )
                    continue

                if older_than_days:
                    older_than = timedelta(days=older_than_days)
                else:
                    older_than = timedelta(hours=older_than_hours)

                deleted = clean_by_age(
                    conn, table_name, older_than, timestamp_col, dry_run=dry_run
                )
                total_deleted += deleted

            elif start_date and end_date:
                if not timestamp_col:
                    LOG.warning(
                        f"  Skipping {table_name}: no timestamp column available"
                    )
                    continue

                deleted = clean_by_date_range(
                    conn, table_name, start_date, end_date, timestamp_col, dry_run=dry_run
                )
                total_deleted += deleted

            else:
                LOG.error("No cleanup criteria specified")
                sys.exit(1)

        LOG.info(f"\nTotal rows {'would be deleted' if dry_run else 'deleted'}: {total_deleted:,}")

        # Show final statistics
        if not dry_run:
            print_stats(conn, table_names, "After cleanup")

    finally:
        conn.close()


def parse_datetime(value: str) -> datetime:
    """Parse a datetime string in ISO format or common formats."""
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Unable to parse datetime: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean realtime GTFS data from the database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete data older than 7 days (dry run)
  python -m realtime_analysis.clean_realtime_data --older-than-days 7 --dry-run

  # Delete data older than 12 hours
  python -m realtime_analysis.clean_realtime_data --older-than-hours 12

  # Delete data from a specific date range
  python -m realtime_analysis.clean_realtime_data --start-date "2024-01-01" --end-date "2024-01-31"

  # Delete all data from specific tables
  python -m realtime_analysis.clean_realtime_data --all --tables vehicle_positions trip_updates

  # Delete all realtime data (requires confirmation)
  python -m realtime_analysis.clean_realtime_data --all --confirm
        """,
    )

    # Cleanup criteria (mutually exclusive)
    criteria_group = parser.add_mutually_exclusive_group(required=True)
    criteria_group.add_argument(
        "--older-than-days",
        type=float,
        help="Delete data older than this many days",
    )
    criteria_group.add_argument(
        "--older-than-hours",
        type=float,
        help="Delete data older than this many hours",
    )
    criteria_group.add_argument(
        "--start-date",
        type=str,
        help="Start date for date range deletion (ISO format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
    )
    criteria_group.add_argument(
        "--all",
        action="store_true",
        dest="all_data",
        help="Delete all data from selected tables",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for date range deletion (required with --start-date)",
    )

    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(REALTIME_TABLES.keys()),
        help="Tables to clean (default: all realtime tables). Options: %(choices)s",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )

    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt (use with caution)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    configure_logging(args.verbose)

    # Validate arguments
    if args.start_date and not args.end_date:
        parser.error("--end-date is required when using --start-date")
    if args.end_date and not args.start_date:
        parser.error("--start-date is required when using --end-date")

    # Parse dates if provided
    start_date = None
    end_date = None
    if args.start_date:
        start_date = parse_datetime(args.start_date)
    if args.end_date:
        end_date = parse_datetime(args.end_date)

    settings = load_settings()

    clean_realtime_data(
        settings,
        older_than_days=args.older_than_days,
        older_than_hours=args.older_than_hours,
        start_date=start_date,
        end_date=end_date,
        all_data=args.all_data,
        tables=args.tables,
        dry_run=args.dry_run,
        confirm=args.confirm,
    )


if __name__ == "__main__":
    main()




