#!/usr/bin/env python3

import logging
from fastapi import FastAPI, HTTPException
from ConvertToStandardPath_MergeSubmarineWithLandCable import get_all_submarine_to_standard_paths_pairs
from ConvertToStandardPath_SubmarineCable import get_all_submarine_standard_paths
import sqlite3
import networkx as nx
from haversine import haversine
import geopandas as gpd
import sys

from shapely import wkt, Point
from shapely.geometry import LineString, MultiLineString
from Processing_CloudRegions import cut_linestring
from Common import init_logging


Coordinate = tuple[float, float]
Location = tuple[str, str, str]


def city_formatter(city_info: Location) -> Location:
    city, state, country = city_info
    return (city.strip(), state.strip(), country.strip())


def find_closest_points(point: Coordinate, points_set: set[Coordinate], max_distance_km=150) -> list[Coordinate]:
    """find the closest point for a given coordinate in case it is not in the graph"""
    return list(filter(lambda p: haversine(point, p) < max_distance_km, points_set))


def add_edge(G, city1: Location, city2: Location, distance: float, path_wkt: str,
             src_city_coord: Coordinate, dst_city_coord: Coordinate, cable_type: str):
    """helper function to build nx graph with src/dst city, src/dst coordinates, wkt path, cabel type and distance."""
    # Add or update nodes with their coordinates
    G.add_node(city1, coord=src_city_coord)
    G.add_node(city2, coord=dst_city_coord)

    # Add the edge with its properties
    G.add_edge(city1, city2, weight=distance, path_wkt=path_wkt,
               src_city_coord=src_city_coord, dst_city_coord=dst_city_coord, cable_type=cable_type)


def get_all_standard_paths(db_file: str):
    """read standard paths from database"""
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        sql_query = """
        SELECT sp.from_city, sp.from_state, sp.from_country, sp.to_city, sp.to_state, sp.to_country, sp.distance_km, sp.path_wkt
        FROM standard_paths sp
        """
        cursor.execute(sql_query)
        data = cursor.fetchall()
    return data


# fetch all asn locations related to amazon from database
def get_as_locations(db_file, cloud_region_scope) -> list[Coordinate]:
    """read asn locations from database"""
    coordinates: list[Coordinate] = []
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        if cloud_region_scope == 'amazon':
            sql_query = """
            SELECT DISTINCT al.latitude, al.longitude
            FROM asn_asname aa
            JOIN asn_loc al ON aa.asn = al.asn
            WHERE aa.asn_name LIKE '%amazon%' and al.physical_presence = 'True'
                and latitude != 'NULL' and longitude != 'NULL';
            """
        elif cloud_region_scope == 'google':
            sql_query = """
            SELECT DISTINCT al.latitude, al.longitude
            FROM asn_asname aa
            JOIN asn_loc al ON aa.asn = al.asn
            WHERE aa.asn_name LIKE '%google%' and al.physical_presence = 'True'
                and latitude != 'NULL' and longitude != 'NULL';
            """
        cursor.execute(sql_query)
        data = cursor.fetchall()
    for row in data:
        latitude = float(row[0])
        longitude = float(row[1])
        coordinates.append((latitude, longitude))
    return coordinates


def coordinate_reverser(coord: Coordinate) -> Coordinate:
    """lati and longti in wkt path is reversed, use this helper function to reverse that"""
    coord_longti, coord_lati = coord
    return (coord_lati, coord_longti)


def create_linestring_from_latlon_list(latlon_list: list[Coordinate]) -> LineString:
    """helper function to create a linestring from a list of latlon"""
    return LineString([coordinate_reverser(coord) for coord in latlon_list])


def parse_wkt_linestring(wkt_string: str) -> LineString:
    """helper function to get the src/dst coordinate from a wkt path"""
    try:
        return wkt.loads(wkt_string)
    except Exception as ex:
        print(f"wkt string {wkt_string} is not valid: {ex}", file=sys.stderr)
        return None


def graph_build_helper(G: nx.Graph, coord_city_map: dict[Coordinate, Location], coordinates: set[Coordinate],
                       paths: list[tuple], submarine_option=False) -> \
        tuple[nx.Graph, dict[Coordinate, Location], set[Coordinate]]:
    for from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt in paths:

        from_city_info = city_formatter((from_city, from_state, from_country))
        to_city_info = city_formatter((to_city, to_state, to_country))
        if from_city_info == to_city_info:
            continue
        linestring = parse_wkt_linestring(path_wkt)
        if linestring is None:
            print(f"invalid wkt string {path_wkt}", file=sys.stderr)
            continue
        start_city_coord = linestring.coords[0]
        end_city_coord = linestring.coords[-1]
        start_city_coord = coordinate_reverser(start_city_coord)
        end_city_coord = coordinate_reverser(end_city_coord)

        coord_city_map[start_city_coord] = from_city_info
        coordinates.append(start_city_coord)
        coord_city_map[end_city_coord] = to_city_info
        coordinates.append(end_city_coord)

        edge_type = 'land'

        if submarine_option:
            edge_type = "submarine"

        add_edge(G, from_city_info, to_city_info, distance_km,
                 linestring, start_city_coord, end_city_coord, edge_type)
        add_edge(G, to_city_info, from_city_info, distance_km,
                 LineString(linestring.coords[::-1]), end_city_coord, start_city_coord, edge_type)
    return G, coord_city_map, coordinates


def build_up_global_graph(db_file) -> tuple[dict[Coordinate, Location], set[Coordinate], nx.Graph, dict[str, list[Coordinate]]]:
    print("Building up NX graph from paths...")
    submarine_standard_paths = get_all_submarine_standard_paths(db_file)
    submarine_to_standard_paths_pairs = get_all_submarine_to_standard_paths_pairs(
        db_file)
    standard_paths = get_all_standard_paths(db_file)

    all_as_locations = {}
    all_as_locations['amazon'] = get_as_locations(db_file, 'amazon')
    all_as_locations['google'] = get_as_locations(db_file, 'google')
    all_as_locations['all'] = all_as_locations['amazon'] + \
        all_as_locations['google']

    G = nx.DiGraph()
    coord_city_map: dict[Coordinate, Location] = {}
    coord_set: set[Coordinate] = []

    G, coord_city_map, coord_set = graph_build_helper(
        G, coord_city_map, coord_set, standard_paths)
    G, coord_city_map, coord_set = graph_build_helper(
        G, coord_city_map, coord_set, submarine_standard_paths, True)
    G, coord_city_map, coord_set = graph_build_helper(
        G, coord_city_map, coord_set, submarine_to_standard_paths_pairs)

    return coord_city_map, set(coord_set), G, all_as_locations


def is_point_close_to_path(lat: float, lon: float, db_path: str, max_distance: float) -> bool:
    gs = gpd.GeoSeries(db_path)

    # The GeoDataFrame takes a coordinate system that it applies to the
    # 'geometry' column. The EPSG:4326 coordinate system is latitude,
    # longitude.
    geodf = gpd.GeoDataFrame(geometry=gs, crs="EPSG:4326")
    geodf["intersection"] = geodf["geometry"].shortest_line(Point(lon, lat))
    # Before calculating distances we need to change coordinate systems to one
    # that has a unit of length. The EPSG:3857 coordinate system has units of
    # meters, and is what Google Maps uses.
    geodf["distance"] = geodf["intersection"].to_crs("EPSG:3857").length / 1000

    return not geodf[geodf["distance"] < max_distance].empty


def calculate_shortest_path_distance(G: nx.Graph, shortest_path_cities: list[Coordinate], cloud_region_scope: str,
                                     all_as_locations: dict[str, list[Coordinate]]) -> \
        tuple[float, list[Coordinate], str, list[str]]:
    total_distance = 0
    coordinate_list = []
    wkt_list: list[LineString] = []
    cable_type_list = []

    for i in range(len(shortest_path_cities) - 1):
        city1 = shortest_path_cities[i]
        city2 = shortest_path_cities[i + 1]

        total_distance += G[city1][city2]['weight']
        # Only append the coordinates of the source city here
        # for each edge, find all the available asn nodes that are close to the edge
        insertable_as_locs = []
        for lat, lon in all_as_locations[cloud_region_scope]:
            if is_point_close_to_path(lat, lon, G[city1][city2]['path_wkt'], 5):
                insertable_as_locs.append((lat, lon))

        # has asn nodes that are close to the edge
        if len(insertable_as_locs) > 0:
            item_distance_pairs = []

            # sort the asn nodes by distance to the start point of the edge for following cut option
            for coordinate in insertable_as_locs:
                distance = G[city1][city2]['path_wkt'].project(
                    Point(coordinate[1], coordinate[0]))
                item_distance_pairs.append((coordinate, distance))

            sorted_item_distance_pairs = sorted(
                item_distance_pairs, key=lambda x: x[1])

            linestring_to_be_cut = G[city1][city2]['path_wkt']
            linestring_to_be_cut_type = G[city1][city2]['cable_type']

            # append the start point of the edge
            coordinate_list.append(G.nodes[city1]['coord'])
            # append the cable type of the edge for the first segment
            cable_type_list.append(linestring_to_be_cut_type)

            for coordinate, distance in sorted_item_distance_pairs:
                lat, lon = coordinate
                splitted = cut_linestring(
                    linestring_to_be_cut, to_add=Point(lon, lat))
                if len(splitted) < 2:
                    continue
                (l1, l2) = splitted

                # append the first segment of the cutted edge, set the second segment as the next edge to be cut
                wkt_list.append(l1)
                # append all the intermediate asn points
                coordinate_list.append(coordinate)
                # append the cable type of the edge for the intermediate segments
                cable_type_list.append(linestring_to_be_cut_type)
                linestring_to_be_cut = l2

            # append the last segment of the cutted edge
            wkt_list.append(linestring_to_be_cut)

        else:
            wkt_list.append(G[city1][city2]['path_wkt'])
            coordinate_list.append(G.nodes[city1]['coord'])
            cable_type_list.append(G[city1][city2]['cable_type'])

    # Append the coordinates of the last city after the loop
    coordinate_list.append(G.nodes[shortest_path_cities[-1]]['coord'])

    return total_distance, coordinate_list, MultiLineString(wkt_list).wkt, cable_type_list


def connect_nearby_cities(G, name: str, coordinate: Coordinate, nearby_cities: list[Coordinate],
                          coord_city_map: dict[Coordinate, Location]) -> None:
    """helper function to connect the closest cities to the graph"""
    nearby_city: Coordinate
    if coordinate in coord_city_map:
        node_name = coord_city_map[coordinate]
    else:
        node_name = Location((name, "", ""))
        G.add_node(node_name, coord=coordinate)
        coord_city_map[coordinate] = node_name

    for nearby_city in nearby_cities:
        nearby_city_name = coord_city_map[nearby_city]
        distance_km = haversine(coordinate, nearby_city)
        add_edge(G, node_name, nearby_city_name, distance_km,
                 create_linestring_from_latlon_list([coordinate, nearby_city]), coordinate, nearby_city, 'land')
        add_edge(G, nearby_city_name, node_name, distance_km,
                 create_linestring_from_latlon_list([nearby_city, coordinate]), nearby_city, coordinate, 'land')


app = FastAPI()

SAME_CITY_THRESHOLD_KM = 5


@app.get("/physical-route/")
def physical_route(src_latitude: float, src_longitude: float,
                   dst_latitude: float, dst_longitude: float, cloud_region_scope: str) -> dict:
    """
    Get the physical route in (lat, lon) format from src to dst, including both ends.
    """
    src_coordinate = (src_latitude, src_longitude)
    dst_coordinate = (dst_latitude, dst_longitude)
    direct_distance_km = haversine(src_coordinate, dst_coordinate)
    if direct_distance_km < SAME_CITY_THRESHOLD_KM:
        linestring = create_linestring_from_latlon_list(
            [src_coordinate, dst_coordinate])
        return {
            'routers_latlon': [src_coordinate, dst_coordinate],
            'distance_km': direct_distance_km,
            'fiber_wkt_paths': MultiLineString([linestring]).wkt,
            'fiber_types': ['land'],
        }

    # Convert input coordinates to city information
    src_nearby_cities: list[Coordinate] = find_closest_points(
        (src_latitude, src_longitude), app.coord_set)
    dst_nearby_cities: list[Coordinate] = find_closest_points(
        (dst_latitude, dst_longitude), app.coord_set)

    G: nx.Graph = app.G.copy()
    coord_city_map: dict[Coordinate, Location] = app.coord_city_map.copy()
    connect_nearby_cities(G, "src", src_coordinate,
                          src_nearby_cities, coord_city_map)
    connect_nearby_cities(G, "dst", dst_coordinate,
                          dst_nearby_cities, coord_city_map)

    src_city = coord_city_map[src_coordinate]
    dst_city = coord_city_map[dst_coordinate]

    assert src_city and dst_city

    # Find shortest path between cities in the graph
    try:
        shortest_path_cities: list[Location] = nx.shortest_path(
            G, source=src_city, target=dst_city, weight='distance')

        shortest_distance, coordinate_list, wkt_list, cable_type_list = \
            calculate_shortest_path_distance(
                G, shortest_path_cities, cloud_region_scope, app.all_as_locations)

        return {
            'routers_latlon': coordinate_list,
            'distance_km': shortest_distance,
            'fiber_wkt_paths': wkt_list,
            'fiber_types': cable_type_list,
        }
    except nx.NetworkXNoPath:
        raise HTTPException(status_code=400, detail="No shortest path found")


def run():
    app.db_file = '../database/igdb.db'
    app.coord_city_map, app.coord_set, app.G, app.all_as_locations = \
        build_up_global_graph(app.db_file)
    import uvicorn
    uvicorn.run(app, port=8083)


if __name__ == "__main__":
    init_logging(level=logging.DEBUG)
    run()
