import argparse
import Querying_Database as qdb
import geopandas as gpd

import pandas as pd

from collections import defaultdict
from geopy.distance import geodesic
from math import inf
from shapely import wkt, shortest_line, LineString, Point
from sqlite3 import Row as sqlite_Row

from Create_KML import write_linestrings_to_file, LineStringToKML

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
    if distance <= 0.0 or distance >= line.length:
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

def add_cloud_regions_to_db(db_path: str, rows) -> None:
    querier = qdb.queryDatabase(db_path)
    query = f"""INSERT INTO standard_paths VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    querier.execute_many(query, rows)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--database-path", type=str, default="../database/igdb.db",
                        help='Path to database file, default is `../database/igdb.db`')
    parser.add_argument("-d", "--max-distance", type=float, default=5,
                        help="""Maximum distance, in km, to consider a region to
                        lie along an edge. If the location of the cloud region
                        is less than this distance away from an existing edge,
                        we consider the region to be connected to the edge.""")
    parser.add_argument("--save-kml-file", nargs="?", const="linestrings.kml", default="",
                        help="""Write the newly created edges to a KML file. If the
                        command line option is given without a filename, the default is
                        `linestrings.kml`.""")


    args = parser.parse_args()

    # TODO get these from a file, etc
    region_to_coords = {
                        "aws:us-west-1": (37.2379, -121.7946),
                        "aws:us-east-1": (39.0127, -77.5342),
                       }
    print(args)
    new_rows = []
    for region, (lat, lon) in region_to_coords.items():

        rows = find_closest_paths(lat, lon, args.database_path, args.max_distance)
        print(f"Found {len(rows)} rows")

        for row in rows.itertuples(index=False):
            linestring: LineString = wkt.loads(row[7])
            distance: float = linestring.project(Point(lon, lat))
            l1, l2 = cut(linestring, distance, Point(lon, lat))
            new_rows.append((row[0], row[1], row[2], region, "", "", distance_of_linestring(l1), wkt.dumps(l1), ""))
            new_rows.append((region, "", "", row[3], row[4], row[5], distance_of_linestring(l2), wkt.dumps(l2), ""))
            print(f"{row[0]} to {row[3]}")

    add_cloud_regions_to_db(args.database_path, new_rows)
    
    print(f"Added {len(new_rows)} rows to database")

    if args.save_kml_file:
        lines = []
        for row in new_rows:
            lines.append(LineStringToKML(linestring=wkt.loads(row[7]), name=f"{row[0]} {row[3]}"))
        write_linestrings_to_file(args.save_kml_file, lines)
