import argparse
import Querying_Database as qdb

from collections import defaultdict
from geopy.distance import geodesic
from math import inf
from shapely import wkt, shortest_line, LineString, Point

from linestring_to_kml import linestrings_to_kml


def find_closest_paths(lat: float, lon: float, db_path: str):
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
        ps = shortest_line(Point(lon, lat), linestring).coords
        p1 = (ps[0][1], ps[0][0])
        p2 = (ps[1][1], ps[1][0])
        distance = geodesic(p1, p2)
        if distance.km < 5:
            paths.append(row)

    return paths


def query_db_for_edges(db_path: str):
    querier = qdb.queryDatabase(db_path)
    query = f"""SELECT * FROM standard_paths;"""
    results = querier.execute_query(query)

    points = defaultdict(list)

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
        lons, lats = linestring.coords.xy
        for lat, lon in zip(lats, lons):
            points[(lat, lon)].append(linestring)

    print(len(points))
    return points




def closest_point(lat: float, lon: float, points):
    min_distance = inf
    min_lat, min_lon = None, None
    for lat_, lon_ in points.keys():
        distance = geodesic((lat_, lon_), (lat, lon))
        if distance < min_distance:
            min_distance = distance
            min_lat = lat_
            min_lon = lon_

    return min_lat, min_lon


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




if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database-path', type=str, required=True,
                        help='Path to database file')

    args = parser.parse_args()

    region_to_coords = {"us-west-1": (37.2379, -121.7946),
                        "us-east-1": (39.0127, -77.5342)}
    print(args)
    points = query_db_for_edges(args.database_path)
    lines = []
    for region, (lat, lon) in region_to_coords.items():

        paths = find_closest_paths(lat, lon, args.database_path)
        print(f"Found {len(paths)} paths")
        for path in paths:
            ls = wkt.loads(path[7])
            d = ls.project(Point(lon, lat))
            l1, l2 = cut(ls, d, Point(lon, lat))
            lines.append(l1)
            lines.append(l2)

            print(f"{path[0]} to {path[3]}")
        continue

    kml_string = linestrings_to_kml([""]*len(lines), lines)
    with open("split_linestrings.kml", "wb") as kml_file:
        kml_file.write(kml_string)
