#!/usr/bin/env python3

import logging
import math
from typing import Optional
import sys

from haversine import haversine
from shapely import wkt
from shapely.geometry import LineString

Coordinate = tuple[float, float]
Location = tuple[str, str, str]


def init_logging(level=logging.DEBUG):
    logging.basicConfig(level=level,
                        stream=sys.stderr,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

def parse_wkt_linestring(wkt_string: str) -> LineString:
    """helper function to get the src/dst coordinate from a wkt path"""
    try:
        return wkt.loads(wkt_string)
    except Exception as ex:
        logging.warning(f"wkt string {wkt_string} is not valid: {ex}")
        return None

def are_coordinates_close(coordinate1: Coordinate, coordinate2: Coordinate,
                          max_distance_km: Optional[float] = None) -> bool:
    """Check whether two coordinates are close to each other.

        If max distance is specified, this function compares the distance; otherwise, it compares lat/lon numbers."""
    if max_distance_km:
        return haversine(coordinate1, coordinate2) < max_distance_km
    else:
        return math.isclose(coordinate1[0], coordinate2[0]) and math.isclose(coordinate1[1], coordinate2[1])
