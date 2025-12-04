"""
Realtime transit analysis package.

This module groups together the realtime ingestion scripts, SQL helpers,
and visualization utilities used to compare Vancouver's scheduled GTFS data
with GTFS-Realtime feeds.
"""

# Re-export utility modules for backward compatibility
from realtime_analysis.utility import config, utils

__all__ = ["config", "utils"]


