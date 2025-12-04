#!/usr/bin/env python3
"""
Convert GTFS-Realtime vehicle position samples into MobilityDB trajectories.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence

from realtime_analysis.utility.config import load_settings
from realtime_analysis.utility.utils import get_connection, resolve_route_filter

LOG = logging.getLogger("realtime_analysis.build_trajs")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_iso_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def build_trajs(
    *,
    start_ts: datetime,
    end_ts: datetime,
    route_ids: Optional[Sequence[str]],
    route_short_names: Optional[Sequence[str]],
    verbose: bool = False,
) -> int:
    settings = load_settings()
    with get_connection(settings) as conn:
        route_filter = resolve_route_filter(
            conn, route_ids=route_ids, route_short_names=route_short_names
        )

        LOG.info(
            "Building trajectories from %s to %s (routes: %s)",
            start_ts.isoformat(),
            end_ts.isoformat(),
            ", ".join(sorted(route_filter.route_ids)) if route_filter.route_ids else "ALL",
        )

        sql = """
        WITH base AS (
            SELECT
                trip_instance_id,
                trip_id,
                route_id,
                COALESCE(start_date, entity_timestamp::date) AS service_date,
                vehicle_id,
                entity_timestamp,
                fetch_timestamp,
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_instance_id, entity_timestamp
                    ORDER BY fetch_timestamp
                ) AS rn
            FROM rt_vehicle_positions
            WHERE trip_instance_id IS NOT NULL
              AND entity_timestamp BETWEEN %(start_ts)s AND %(end_ts)s
              AND (
                    %(filter_routes)s = FALSE
                    OR route_id = ANY(%(route_ids)s)
              )
        ),
        dedup AS (
            SELECT *
            FROM base
            WHERE rn = 1
        ),
        sched AS (
            SELECT DISTINCT ON (trip_id) trip_id, traj
            FROM scheduled_trips_mdb
            ORDER BY trip_id, date
        ),
        sequences AS (
            SELECT
                d.trip_instance_id,
                d.trip_id,
                d.route_id,
                d.service_date,
                d.vehicle_id,
                tgeompoint(
                    CASE
                        WHEN s.traj IS NOT NULL THEN ST_LineInterpolatePoint(
                            s.traj,
                            ST_LineLocatePoint(s.traj, d.geom)
                        )
                        ELSE d.geom
                    END,
                    d.entity_timestamp
                ) AS point
            FROM dedup d
            LEFT JOIN sched s USING (trip_id)
        ),
        combined AS (
            SELECT
                trip_instance_id,
                -- Take the first non-null value for each field
                (array_agg(DISTINCT trip_id) FILTER (WHERE trip_id IS NOT NULL))[1] AS trip_id,
                (array_agg(DISTINCT route_id) FILTER (WHERE route_id IS NOT NULL))[1] AS route_id,
                (array_agg(DISTINCT service_date) FILTER (WHERE service_date IS NOT NULL))[1] AS service_date,
                (array_agg(DISTINCT vehicle_id) FILTER (WHERE vehicle_id IS NOT NULL))[1] AS vehicle_id,
                array_agg(point ORDER BY getTimestamp(point)) AS points
            FROM sequences
            GROUP BY trip_instance_id
            HAVING COUNT(*) > 1
        ),
        trajs AS (
            SELECT
                trip_instance_id,
                trip_id,
                route_id,
                service_date,
                vehicle_id,
                tgeompointseq(points) AS trip,
                trajectory(tgeompointseq(points)) AS traj
            FROM combined
        )
        INSERT INTO realtime_trips_mdb (
            trip_instance_id, trip_id, route_id, service_date,
            vehicle_id, trip, traj, starttime, updated_at
        )
        SELECT
            trip_instance_id,
            trip_id,
            route_id,
            service_date,
            vehicle_id,
            trip,
            traj,
            startTimestamp(trip) AS starttime,
            now()
        FROM trajs
        ON CONFLICT (trip_instance_id) DO UPDATE
        SET
            trip = EXCLUDED.trip,
            traj = EXCLUDED.traj,
            service_date = EXCLUDED.service_date,
            route_id = EXCLUDED.route_id,
            vehicle_id = EXCLUDED.vehicle_id,
            starttime = EXCLUDED.starttime,
            updated_at = now()
        RETURNING trip_instance_id;
        """

        params = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "filter_routes": route_filter.applies(),
            "route_ids": list(route_filter.route_ids),
        }

        with conn.cursor() as cur:
            cur.execute(sql, params)
            inserted = cur.fetchall()
            conn.commit()

        LOG.info("Updated %s trajectory records.", len(inserted))
        return len(inserted)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build MobilityDB trajectories from realtime vehicle positions."
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=3.0,
        help="Look back this many hours from now when selecting samples (default: 3).",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="ISO-8601 timestamp for the start of the window (overrides --hours).",
    )
    parser.add_argument(
        "--until",
        type=str,
        help="ISO-8601 timestamp for the end of the window (defaults to now).",
    )
    parser.add_argument(
        "--route-id",
        action="append",
        help="Restrict to these GTFS route_ids.",
    )
    parser.add_argument(
        "--route-short-name",
        action="append",
        help="Restrict to these GTFS route_short_names.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    end_ts = parse_iso_timestamp(args.until) or datetime.now(timezone.utc)
    if args.since:
        start_ts = parse_iso_timestamp(args.since)
        if not start_ts:
            raise ValueError("Failed to parse --since timestamp.")
    else:
        start_ts = end_ts - timedelta(hours=args.hours)

    if start_ts >= end_ts:
        raise ValueError("--since/--hours must define a window before --until/now.")

    build_trajs(
        start_ts=start_ts,
        end_ts=end_ts,
        route_ids=args.route_id,
        route_short_names=args.route_short_name,
        verbose=args.verbose,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



