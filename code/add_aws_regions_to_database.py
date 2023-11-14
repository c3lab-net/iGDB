import argparse
import Querying_Database as qdb

from collections import defaultdict
from geopy.distance import geodesic
from math import inf
from shapely import wkt, shortest_line, LineString, Point

from linestring_to_kml import linestrings_to_kml


def find_closest_paths(lat: float, lon: float, db_path: str, max_distance: float):
    querier = qdb.queryDatabase(db_path)
    query = f"""SELECT * FROM standard_paths;"""
    results = querier.execute_query(query)
    
    paths = []
    for row in results:
        fc = row[0]
        fs = row[1]
        fcc = row[2]
        tc = row[3]
        ts = row[4]
        tcc = row[5]
        dist_km = float(row[6])
        path_wkt = row[7]
        edge = ((fc, fs, fcc), (tc, ts, tcc))


        linestring = wkt.loads(path_wkt)
        # The linestring is an existing edge in the graph, and the point is the
        # location of the cloud region. We get the shortest line between these
        # two geometries, which gives us the shortest distance from a cloud
        # region to an existing edge in the graph.
        ps = shortest_line(Point(lon, lat), linestring).coords
        # Since the WKT format is (lon, lat) and geopy.geodesic takes
        # (lat, lon) pairs, we need to switch these.
        p1 = (ps[0][1], ps[0][0])
        p2 = (ps[1][1], ps[1][0])
        distance = geodesic(p1, p2)
        if distance.km < max_distance:
            paths.append(row)

    return paths

# Taken from stackoverflow
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


    args = parser.parse_args()

    region_to_coords = {"us-west-1": (37.2379, -121.7946),
                        "us-east-1": (39.0127, -77.5342)}
    print(args)
    lines = []
    for region, (lat, lon) in region_to_coords.items():

        rows = find_closest_paths(lat, lon, args.database_path, args.max_distance)
        print(f"Found {len(rows)} rows")

        new_rows = []
        for row in rows:
            ls = wkt.loads(row[7])
            d = ls.project(Point(lon, lat))
            l1, l2 = cut(ls, d, Point(lon, lat))
            lines.append(l1)
            lines.append(l2)
            new_rows.append((region, "", "", row[3], row[4], row[5], distance_of_linestring(l1), wkt.dumps(l1), ""))
            print(f"{row[0]} to {row[3]}")

        add_cloud_regions_to_db(args.database_path, new_rows)

    kml_string = linestrings_to_kml([""]*len(lines), lines)
    with open("split_linestrings.kml", "wb") as kml_file:
        kml_file.write(kml_string)
