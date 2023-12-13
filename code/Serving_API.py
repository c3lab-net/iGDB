#!/usr/bin/env python3

from fastapi import FastAPI, HTTPException
from ConvertToStandardPath_MergeSubmarineWithLandCable import get_all_submarine_to_standard_paths_pairs
from ConvertToStandardPath_SubmarineCable import get_all_submarine_standard_paths, floatFormatter
import sqlite3
import networkx as nx
from haversine import haversine

from shapely import wkt
from shapely.geometry import LineString, MultiLineString


Coordinate=tuple[float, float]
Location=tuple[str, str, str]

def city_formatter(city_info: Location) -> Location:
    city, state, country = city_info
    return (city.strip(), state.strip(), country.strip())


def find_closest_point(point: Coordinate, points_set: set[Coordinate]) -> Coordinate:
    """find the closest point for a given coordinate in case it is not in the graph"""
    return min(points_set, key=lambda p: haversine(point, p))


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


def coordinate_reverser(coord: Coordinate) -> Coordinate:
    """lati and longti in wkt path is reversed, use this helper function to reverse that"""
    coord_longti, coord_lati = coord
    return (coord_lati, coord_longti)


def parse_wkt_linestring(wkt_string: str) -> LineString:
    """helper function to get the src/dst coordinate from a wkt path"""
    try:
        return wkt.loads(wkt_string)
    except Exception as ex:
        print(f"wkt string {wkt_string} is not valid: {ex}")
        return None


def graph_build_helper(G: nx.Graph, coord_city_map: dict[Coordinate, Location], coordinates: set[Coordinate],
                       paths: list[tuple], submarine_option = False) -> \
                        tuple[nx.Graph, dict[Coordinate, Location], set[Coordinate]]:
    for from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt in paths:

        from_city_info = city_formatter((from_city, from_state, from_country))
        to_city_info = city_formatter((to_city, to_state, to_country))
        if from_city_info == to_city_info:
            continue
        linestring = parse_wkt_linestring(path_wkt)
        if linestring is None:
            print(f"invalid wkt string {path_wkt}")
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

        add_edge(G, from_city_info, to_city_info, distance_km, linestring, start_city_coord, end_city_coord, edge_type)
        add_edge(G, to_city_info, from_city_info, distance_km, LineString(linestring.coords[::-1]), end_city_coord, start_city_coord, edge_type)
    return G, coord_city_map, coordinates


def build_up_global_graph(db_file) -> tuple[dict[Coordinate, Location], set[Coordinate], nx.Graph]:
    submarine_standard_paths = get_all_submarine_standard_paths(db_file)
    submarine_to_standard_paths_pairs = get_all_submarine_to_standard_paths_pairs(db_file)
    standard_paths = get_all_standard_paths(db_file)

    G = nx.DiGraph()
    coord_city_map: dict[Coordinate, Location] = {}
    coord_set: set[Coordinate] = []

    G, coord_city_map, coord_set = graph_build_helper(
        G, coord_city_map, coord_set, standard_paths)
    G, coord_city_map, coord_set = graph_build_helper(
        G, coord_city_map, coord_set, submarine_standard_paths, True)
    G, coord_city_map, coord_set = graph_build_helper(
        G, coord_city_map, coord_set, submarine_to_standard_paths_pairs)

    return coord_city_map, coord_set, G


def calculate_shortest_path_distance(G: nx.Graph, shortest_path_cities: list[Coordinate]) -> \
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
        coordinate_list.append(G.nodes[city1]['coord'])
        wkt_list.append(G[city1][city2]['path_wkt'])
        cable_type_list.append(G[city1][city2]['cable_type'])

    # Append the coordinates of the last city after the loop
    coordinate_list.append(G.nodes[shortest_path_cities[-1]]['coord'])

    return total_distance, coordinate_list, MultiLineString(wkt_list).wkt, cable_type_list


app = FastAPI()


@app.get("/physical-route/")
def physical_route(src_latitude: float, src_longitude: float,
                   dst_latitude: float, dst_longitude: float) -> dict:
    """
    Get the physical route in (lat, lon) format from src to dst, including both ends.
    """
    # Convert input coordinates to city information
    src_city_co = find_closest_point((src_latitude, src_longitude), app.coord_set)
    dst_city_co = find_closest_point((dst_latitude, dst_longitude), app.coord_set)

    if src_city_co == dst_city_co:
        return {
            'routers_latlon': [src_city_co, dst_city_co],
            'distance_km': 0,
            'fiber_wkt_paths': MultiLineString([LineString([src_city_co, dst_city_co])]).wkt,
            'fiber_types': ['land'],
        }

    src_city_info = app.coord_city_map[src_city_co]
    dst_city_info = app.coord_city_map[dst_city_co]

    assert src_city_info and dst_city_info

    # Find shortest path between cities in the graph
    try:
        shortest_path_cities: list[Location] = nx.shortest_path(
            app.G, source=src_city_info, target=dst_city_info, weight='distance')

        shortest_distance, coordinate_list, wkt_list, cable_type_list = calculate_shortest_path_distance(
            app.G, shortest_path_cities)

        return {
            'routers_latlon': coordinate_list,
            'distance_km': shortest_distance,
            'fiber_wkt_paths': wkt_list,
            'fiber_types': cable_type_list,
        }
    except nx.NetworkXNoPath:
        raise HTTPException(status_code=400, detail="Shortest path is invalid")


def run():
    app.db_file = '../database/igdb.db'
    app.coord_city_map, app.coord_set, app.G = build_up_global_graph(app.db_file)
    import uvicorn
    uvicorn.run(app, port=8083)


if __name__ == "__main__":
    run()
