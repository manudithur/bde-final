#!/usr/bin/env python3
"""
Poll the TransLink GTFS-Realtime feeds and persist vehicle positions plus trip
updates into PostgreSQL/MobilityDB.
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

import requests
from google.transit import gtfs_realtime_pb2
from psycopg2.extras import execute_values

from realtime_analysis.config import Settings, load_settings
from realtime_analysis.utils import (
    RouteFilter,
    build_trip_instance_id,
    get_connection,
    parse_service_date,
    resolve_route_filter,
)

LOG = logging.getLogger("realtime_analysis.ingest")


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class GTFSRealtimeIngestor:
    def __init__(
        self,
        settings: Settings,
        *,
        route_ids: Optional[Sequence[str]] = None,
        route_short_names: Optional[Sequence[str]] = None,
    ) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.conn = get_connection(settings)
        self.route_filter = resolve_route_filter(
                self.conn, route_ids=route_ids, route_short_names=route_short_names
            )

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # pragma: no cover - defensive
            pass
        self.session.close()

    # ------------------------------------------------------------------
    # Feed fetching helpers
    # ------------------------------------------------------------------

    def _request_feed(self, url: str) -> gtfs_realtime_pb2.FeedMessage:
        params = {}
        if self.settings.api_key:
            params["apikey"] = self.settings.api_key

        LOG.debug("Fetching %s", url)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed

    def _should_accept_route(self, route_id: Optional[str]) -> bool:
        if not self.route_filter.applies():
            return True
        if route_id is None:
            return False
        return route_id in self.route_filter.route_ids

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_timestamp(value: Optional[int], fallback: datetime) -> datetime:
        if value:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return fallback

    def _parse_vehicle_positions(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
        fetch_ts: datetime,
    ) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            vehicle = entity.vehicle
            trip = vehicle.trip
            vehicle_desc = vehicle.vehicle if vehicle.HasField("vehicle") else None

            route_id = trip.route_id or None
            if not self._should_accept_route(route_id):
                continue

            position = vehicle.position
            if not position or not position.latitude or not position.longitude:
                continue

            start_date = parse_service_date(trip.start_date)
            entity_ts = self._to_timestamp(
                vehicle.timestamp or feed.header.timestamp, fetch_ts
            )

            trip_instance_id = build_trip_instance_id(
                trip_id=trip.trip_id or None,
                start_date=start_date,
                start_time=trip.start_time or None,
                vehicle_id=vehicle_desc.id if vehicle_desc and vehicle_desc.id else None,
                fallback_timestamp=entity_ts,
            )

            rows.append(
                {
                    "fetch_timestamp": fetch_ts,
                    "entity_timestamp": entity_ts,
                    "trip_instance_id": trip_instance_id,
                    "trip_id": trip.trip_id or None,
                    "route_id": route_id,
                    "direction_id": trip.direction_id if trip.HasField("direction_id") else None,
                    "start_time": trip.start_time or None,
                    "start_date": start_date,
                    "vehicle_id": vehicle_desc.id if vehicle_desc else None,
                    "vehicle_label": vehicle_desc.label if vehicle_desc else None,
                    "license_plate": vehicle_desc.license_plate if vehicle_desc else None,
                    "current_stop_sequence": vehicle.current_stop_sequence
                    if vehicle.HasField("current_stop_sequence")
                    else None,
                    "stop_id": vehicle.stop_id or None,
                    "current_status": gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                        vehicle.current_status
                    )
                    if vehicle.HasField("current_status")
                    else None,
                    "schedule_relationship": gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                        trip.schedule_relationship
                    )
                    if trip.HasField("schedule_relationship")
                    else None,
                    "occupancy_status": gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(
                        vehicle.occupancy_status
                    )
                    if vehicle.HasField("occupancy_status")
                    else None,
                    "bearing": position.bearing if position.HasField("bearing") else None,
                    "speed_mps": position.speed if position.HasField("speed") else None,
                    "latitude": position.latitude,
                    "longitude": position.longitude,
                }
            )

        return rows

    def _parse_trip_updates(
        self,
        feed: gtfs_realtime_pb2.FeedMessage,
        fetch_ts: datetime,
    ) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            update = entity.trip_update
            trip = update.trip
            vehicle_desc = update.vehicle if update.HasField("vehicle") else None

            route_id = trip.route_id or None
            if not self._should_accept_route(route_id):
                continue

            start_date = parse_service_date(trip.start_date)
            entity_ts = self._to_timestamp(
                update.timestamp or feed.header.timestamp, fetch_ts
            )

            trip_instance_id = build_trip_instance_id(
                trip_id=trip.trip_id or None,
                start_date=start_date,
                start_time=trip.start_time or None,
                vehicle_id=vehicle_desc.id if vehicle_desc and vehicle_desc.id else None,
                fallback_timestamp=entity_ts,
            )

            for stu in update.stop_time_update:
                arrival_time = (
                    self._to_timestamp(stu.arrival.time, fetch_ts)
                    if stu.HasField("arrival") and stu.arrival.time
                    else None
                )
                departure_time = (
                    self._to_timestamp(stu.departure.time, fetch_ts)
                    if stu.HasField("departure") and stu.departure.time
                    else None
                )

                rows.append(
                    {
                        "fetch_timestamp": fetch_ts,
                        "entity_timestamp": entity_ts,
                        "trip_instance_id": trip_instance_id,
                        "trip_id": trip.trip_id or None,
                        "route_id": route_id,
                        "start_time": trip.start_time or None,
                        "start_date": start_date,
                        "vehicle_id": vehicle_desc.id if vehicle_desc else None,
                        "stop_sequence": stu.stop_sequence
                        if stu.HasField("stop_sequence")
                        else None,
                        "stop_id": stu.stop_id or None,
                        "arrival_time": arrival_time,
                        "arrival_delay_seconds": stu.arrival.delay
                        if stu.HasField("arrival") and stu.arrival.delay
                        else None,
                        "departure_time": departure_time,
                        "departure_delay_seconds": stu.departure.delay
                        if stu.HasField("departure") and stu.departure.delay
                        else None,
                        "schedule_relationship": gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                            trip.schedule_relationship
                        )
                        if trip.HasField("schedule_relationship")
                        else None,
                        "stop_schedule_relationship": gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                            stu.schedule_relationship
                        )
                        if stu.HasField("schedule_relationship")
                        else None,
                    }
                )

        return rows

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist_vehicle_positions(self, rows: List[Dict[str, object]]) -> int:
        if not rows:
            return 0

        sql = """
        INSERT INTO rt_vehicle_positions (
            fetch_timestamp, entity_timestamp, trip_instance_id, trip_id,
            route_id, direction_id, start_time, start_date,
            vehicle_id, vehicle_label, license_plate,
            current_stop_sequence, stop_id, current_status,
            schedule_relationship, occupancy_status,
            bearing, speed_mps, latitude, longitude, geom
        )
        VALUES %s
        """

        template = """
        (
            %(fetch_timestamp)s,
            %(entity_timestamp)s,
            %(trip_instance_id)s,
            %(trip_id)s,
            %(route_id)s,
            %(direction_id)s,
            %(start_time)s,
            %(start_date)s,
            %(vehicle_id)s,
            %(vehicle_label)s,
            %(license_plate)s,
            %(current_stop_sequence)s,
            %(stop_id)s,
            %(current_status)s,
            %(schedule_relationship)s,
            %(occupancy_status)s,
            %(bearing)s,
            %(speed_mps)s,
            %(latitude)s,
            %(longitude)s,
            ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)
        )
        """

        with self.conn:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, rows, template=template)
        return len(rows)

    def _persist_trip_updates(self, rows: List[Dict[str, object]]) -> int:
        if not rows:
            return 0

        sql = """
        INSERT INTO rt_trip_updates (
            fetch_timestamp, entity_timestamp, trip_instance_id,
            trip_id, route_id, start_time, start_date, vehicle_id,
            stop_sequence, stop_id, arrival_time, arrival_delay_seconds,
            departure_time, departure_delay_seconds,
            schedule_relationship, stop_schedule_relationship
        )
        VALUES %s
        """

        template = """
        (
            %(fetch_timestamp)s,
            %(entity_timestamp)s,
            %(trip_instance_id)s,
            %(trip_id)s,
            %(route_id)s,
            %(start_time)s,
            %(start_date)s,
            %(vehicle_id)s,
            %(stop_sequence)s,
            %(stop_id)s,
            %(arrival_time)s,
            %(arrival_delay_seconds)s,
            %(departure_time)s,
            %(departure_delay_seconds)s,
            %(schedule_relationship)s,
            %(stop_schedule_relationship)s
        )
        """

        with self.conn:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, rows, template=template)
        return len(rows)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def poll_once(self) -> Dict[str, int]:
        fetch_ts = datetime.now(timezone.utc)

        vehicle_feed = self._request_feed(self.settings.vehicle_positions_url)
        vehicle_rows = self._parse_vehicle_positions(vehicle_feed, fetch_ts)
        vehicle_count = self._persist_vehicle_positions(vehicle_rows)

        trip_feed = self._request_feed(self.settings.trip_updates_url)
        trip_rows = self._parse_trip_updates(trip_feed, fetch_ts)
        trip_count = self._persist_trip_updates(trip_rows)

        LOG.info(
            "Stored %s vehicle positions and %s trip updates",
            vehicle_count,
            trip_count,
        )
        return {"vehicle_positions": vehicle_count, "trip_updates": trip_count}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest GTFS-Realtime vehicle positions and trip updates."
    )
    parser.add_argument(
        "--duration-minutes",
        type=int,
        default=None,
        help="Total duration to poll feeds. Defaults to GTFS_RT_DURATION_MINUTES.",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=None,
        help="Polling interval in seconds. Defaults to GTFS_RT_POLL_INTERVAL.",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        default=None,
        help="Override number of polling iterations (disables duration tracking).",
    )
    parser.add_argument(
        "--route-id",
        action="append",
        help="Limit ingestion to the given GTFS route_id (may be repeated).",
    )
    parser.add_argument(
        "--route-short-name",
        action="append",
        help="Limit ingestion to the given route_short_name (may be repeated).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch feeds a single time, ignoring duration and poll interval.",
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

    settings = load_settings()
    duration_minutes = (
        args.duration_minutes
        if args.duration_minutes is not None
        else settings.default_duration_minutes
    )
    poll_interval = (
        args.poll_interval
        if args.poll_interval is not None
        else settings.default_poll_interval
    )

    if args.once:
        max_polls = 1
    elif args.max_polls:
        max_polls = args.max_polls
    else:
        total_seconds = duration_minutes * 60
        max_polls = max(1, math.ceil(total_seconds / poll_interval))

    LOG.info(
        "Starting realtime ingestion: poll_interval=%ss max_polls=%s routes=%s",
        poll_interval,
        max_polls,
        ", ".join(sorted(settings.target_route_ids or []))
        if settings.target_route_ids
        else "ALL",
    )

    ingestor = GTFSRealtimeIngestor(
        settings,
        route_ids=args.route_id or settings.target_route_ids,
        route_short_names=args.route_short_name or settings.target_route_short_names,
    )

    try:
        for idx in range(max_polls):
            start = time.monotonic()
            ingestor.poll_once()
            elapsed = time.monotonic() - start
            remaining = poll_interval - elapsed
            if idx < max_polls - 1 and not args.once and remaining > 0:
                time.sleep(remaining)
    except KeyboardInterrupt:
        LOG.warning("Interrupted by user, shutting down…")
    finally:
        ingestor.close()

    LOG.info("Realtime ingestion complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())


