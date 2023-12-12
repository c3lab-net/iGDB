import argparse
import csv
import sys
import Querying_Database as qdb
import geopandas as gpd

import pandas as pd

from collections import defaultdict
from geopy.distance import geodesic
from math import inf
from shapely import wkt, shortest_line, LineString, Point
from sqlite3 import Row as sqlite_Row


def find_closest_paths(lat: float, lon: float, db_path: str, max_distance: float) -> gpd.GeoDataFrame:
    querier = qdb.queryDatabase(db_path)
    query = f"""SELECT * FROM standard_paths;"""
    results = querier.execute_query(query, row_factory=sqlite_Row)
    
    df = pd.DataFrame(results, columns=results[0].keys())
    gs = gpd.GeoSeries.from_wkt(df['path_wkt'])
    # The GeoDataFrame takes a coordinate system that it applies to the
    # 'geometry' column. The EPSG:4326 coordinate system is latitude,
    # longitude.
    geodf = gpd.GeoDataFrame(df, geometry=gs, crs="EPSG:4326")
    geodf["intersection"] = geodf["geometry"].shortest_line(Point(lon, lat))
    # Before calculating distances we need to change coordinate systems to one
    # that has a unit of length. The EPSG:3857 coordinate system has units of
    # meters, and is what Google Maps uses.
    geodf["distance"] = geodf["intersection"].to_crs("EPSG:3857").length / 1000

    return geodf[geodf["distance"] < max_distance]


# Taken from stackoverflow
# https://stackoverflow.com/questions/39425093/break-a-shapely-linestring-at-multiple-points
def cut(line, distance, add_p):
    # Cuts a line in two at a distance from its starting point
    # This is taken from shapely manual
    # if distance <= 0:
    #     return [LineString(), LineString(line)]
    # elif distance >= line.length:
    #     return [LineString(line), LineString()]
    if distance <= 0.0 or distance >= line.length:
        print(distance, line, line.length, add_p, file=sys.stderr)
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i+1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)] + [(add_p.x, add_p.y)]),
                LineString([(add_p.x, add_p.y)] + [(cp.x, cp.y)] + coords[i:])]


def distance_of_linestring(ls: LineString) -> float:
    distance = 0
    for i, p1 in enumerate(ls.coords[:-1]):
        p2 = ls.coords[i + 1]
        # Swap because geodesic needs (lat, lon)
        p1 = (p1[1], p1[0])
        p2 = (p2[1], p2[0])
        distance += geodesic(p1, p2).km

    return distance

def add_cloud_regions_to_db(db_path: str, standard_paths: list, city_points: list) -> None:
    querier = qdb.queryDatabase(db_path)
    query_insert_standard_path = f"""INSERT INTO standard_paths VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    querier.execute_many(query_insert_standard_path, standard_paths)
    query_insert_city_points = "INSERT INTO city_points VALUES(?, ?, ?, ?, ?);"
    querier.execute_many(query_insert_city_points, city_points)

def parse_cloud_region_coordinates(cloud_region_coordinates_csv: str) -> dict[str, tuple[float, float]]:
    """Read the csv file containing cloud region coordinates and return a mapping from 'cloud:region' to (lat, lon) tuples."""
    region_to_coords = {}
    with open(cloud_region_coordinates_csv, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        for row in csv_reader:
            cloud_region = f"{row['cloud']}:{row['region']}"
            latitude = float(row['latitude'])
            longitude = float(row['longitude'])
            region_to_coords[cloud_region] = (latitude, longitude)

    return region_to_coords

def add_cloud_regions_to_standard_paths(db_path: str,
                                        cloud_region_coordinates_csv: str,
                                        max_distance_km: float = 5) -> None:
    """Add cloud regions to the standard paths table."""
    print('Adding cloud regions to standard paths table...')
    region_to_coords = parse_cloud_region_coordinates(cloud_region_coordinates_csv)

    new_rows = []
    lines = []
    for region, (lat, lon) in region_to_coords.items():
        print(f"Adding {region} ({lat}, {lon}) to standard paths table...")
        rows = find_closest_paths(lat, lon, db_path, max_distance_km)
        print(f"Found {len(rows)} rows")

        for row in rows.itertuples(index=False):
            linestring: LineString = wkt.loads(row[7])
            distance: float = linestring.project(Point(lon, lat))
            splitted = cut(linestring, distance, Point(lon, lat))
            if len(splitted) < 2:
                continue
            (l1, l2) = splitted
            lines.append(l1)
            lines.append(l2)
            new_rows.append((row[0], row[1], row[2], region, "", "", distance_of_linestring(l1), wkt.dumps(l1), ""))
            new_rows.append((region, "", "", row[3], row[4], row[5], distance_of_linestring(l2), wkt.dumps(l2), ""))
            print(f"{row[0]} to {row[3]}")

    new_city_points = [(region, "", "", lat, lon) for region, (lat, lon) in region_to_coords.items()]
    add_cloud_regions_to_db(db_path, new_rows, new_city_points)

#    TODO add this back in once with a cmd line option
#    kml_string = linestrings_to_kml([""]*len(lines), lines)
#    with open("split_linestrings.kml", "wb") as kml_file:
#        kml_file.write(kml_string)

    print('Finished adding cloud regions to standard paths table.')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--database-path", type=str, default="../database/igdb.db",
                        help='Path to database file, default is `../database/igdb.db`')
    parser.add_argument("-c", "--cloud-region-coordinates-csv", type=str,
                        default="../helper_data/cloud_regions/cloud_region_coordinates.csv",
                        help="""Path to csv file containing cloud region coordinates.
                        The csv file should have four columns: cloud, region, latitude, longitude.""")
    parser.add_argument("-d", "--max-distance", type=float, default=5,
                        help="""Maximum distance, in km, to consider a region to
                        lie along an edge. If the location of the cloud region
                        is less than this distance away from an existing edge,
                        we consider the region to be connected to the edge.""")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    add_cloud_regions_to_standard_paths(args.database_path, args.cloud_region_coordinates_csv, args.max_distance)
