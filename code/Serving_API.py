#!/usr/bin/env python3

from fastapi import FastAPI
from ConvertToStandardPath_MargeSubmarineWithLandCable import get_all_submarine_to_standard_paths_pairs
from ConvertToStandardPath_SubmarineCable import get_all_submarine_standard_paths, floatFormatter
import sqlite3
import networkx as nx
import csv
from xml.dom.minidom import Document
from haversine import haversine

# find the closest point for a given coordinate in case it is not in the graph


def find_closest_point(point, points_set):
    return min(points_set, key=lambda p: haversine(point, p))

# helper function to build nx graph with src/dst city, src/dst coordinates, wkt path, cabel type and distance.


def add_edge(G, city1, city2, distance, path_wkt, src_city_coord, dst_city_coord, cable_type):
    # Add or update nodes with their coordinates
    G.add_node(city1, coord=src_city_coord)
    G.add_node(city2, coord=dst_city_coord)

    # Add the edge with its properties
    G.add_edge(city1, city2, weight=distance, path_wkt=path_wkt,
               src_city_coord=src_city_coord, dst_city_coord=dst_city_coord, cable_type=cable_type)

# read standard paths from database


def get_all_standard_paths(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    sql_query = """
    SELECT sp.from_city, sp.from_state, sp.from_country, sp.to_city, sp.to_state, sp.to_country, sp.distance_km, sp.path_wkt
    FROM standard_paths sp
    """
    cursor.execute(sql_query)
    datas = cursor.fetchall()
    conn.close()
    return datas

# lati and longti in wkt path is reversed, use this helper function to reverse that


def coordinate_reverser(coord):
    coord_longti, coord_lati = coord
    return (coord_lati, coord_longti)

# reverse linestring coords order for direct graph


def reverse_linestring_order(linestring):
    # Remove the 'LINESTRING(' prefix and the closing ')'
    clean_string = linestring.replace("LINESTRING(", "").replace(")", "")

    # Split the string into coordinate pairs
    coords = clean_string.split(", ")

    # Reverse the order of the coordinate pairs
    reversed_coords = coords[::-1]

    # Join the reversed coordinates back into a LineString
    reversed_linestring = "LINESTRING(" + ", ".join(reversed_coords) + ")"

    return reversed_linestring

# helper function to get the src/dst coordinate from a wkt path


def parse_wkt_linestring(wkt_string):
    # Remove the LINESTRING prefix and split the string into coordinate pairs
    coord_pairs = wkt_string.replace(
        "LINESTRING(", "").replace(")", "").split(", ")

    # Extract the first and last coordinate pairs
    first_pair = coord_pairs[0]
    last_pair = coord_pairs[-1]

    # Convert the coordinate pairs from strings to tuples of floats
    start_city_coord = tuple(map(float, first_pair.split(" ")))
    end_city_coord = tuple(map(float, last_pair.split(" ")))

    return start_city_coord, end_city_coord


def graph_build_helper(G, coordCityMap, coord_set, path_set, option):
    for from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt in path_set:

        start_city_coord, end_city_coord = parse_wkt_linestring(path_wkt)
        start_city_coord = coordinate_reverser(start_city_coord)
        end_city_coord = coordinate_reverser(end_city_coord)
        from_city_info = city_formatter(
            (from_city, from_state, from_country))
        to_city_info = city_formatter((to_city, to_state, to_country))
        coordCityMap[start_city_coord] = from_city_info

        coord_set.append(start_city_coord)
        coordCityMap[end_city_coord] = to_city_info
        coord_set.append(end_city_coord)

        edge_type = None

        if option == 'submarine_standard_paths':
            edge_type = "submarine"
        else:
            edge_type = 'land'

        add_edge(G, from_city_info,
                    to_city_info, distance_km, path_wkt, start_city_coord, end_city_coord, edge_type)
        add_edge(G, to_city_info,
                    from_city_info, distance_km, reverse_linestring_order(path_wkt), end_city_coord, start_city_coord, edge_type)
    return G, coordCityMap, coord_set


def build_up_global_graph(db_file):
    submarine_standard_paths = get_all_submarine_standard_paths(db_file)

    submarine_to_standard_paths_pairs = get_all_submarine_to_standard_paths_pairs(
        db_file)
    standard_paths = get_all_standard_paths(db_file)
    G = nx.DiGraph()
    coordCityMap = {}
    coord_set = []

    G, coordCityMap, coord_set = graph_build_helper(
        G, coordCityMap, coord_set, standard_paths, 'standard_paths')
    G, coordCityMap, coord_set = graph_build_helper(
        G, coordCityMap, coord_set, submarine_standard_paths, 'submarine_standard_paths')
    G, coordCityMap, coord_set = graph_build_helper(
        G, coordCityMap, coord_set, submarine_to_standard_paths_pairs, 'submarine_to_standard_paths_pairs')

    return coordCityMap, coord_set, G


app = FastAPI()


@app.get("/physical-route/")
def physical_route(src_latitude: float, src_longitude: float,
                   dst_latitude: float, dst_longitude: float) -> list[tuple[float, float]]:
    """
    Get the physical route in (lat, lon) format from src to dst, including both ends.
    """
    # Convert input coordinates to city information
    src_city_co = find_closest_point(
        (src_latitude, src_longitude), coord_set)

    dst_city_co = find_closest_point(
        (dst_latitude, dst_longitude), coord_set)

    src_city_info = coordCityMap[src_city_co]
    dst_city_info = coordCityMap[dst_city_co]

    assert src_city_info and dst_city_info

    # Find shortest path between cities in the graph
    try:
        shortest_path_cities = nx.shortest_path(
            G, source=src_city_info, target=dst_city_info, weight='distance')

        shortest_distance, coordinate_list, wkt_list, cable_type_list = calculate_shortest_path_distance(
            G, shortest_path_cities)

        return (coordinate_list, wkt_list, cable_type_list)
    except nx.NetworkXNoPath:
        print("cannot find the shortest path")
        return ([], [], [])  # or handle the error as you prefer


def run():
    import uvicorn
    uvicorn.run(app, port=8082)


def calculate_shortest_path_distance(G, shortest_path_cities):
    total_distance = 0
    coordinate_list = []
    wkt_list = []
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

    return total_distance, coordinate_list, wkt_list, cable_type_list


def city_formatter(city_info):
    city, state, country = city_info
    return (city.strip(), state.strip(), country.strip())


def parse_csv(filename):
    data = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data


def function_tester():
    filename = 'all_pairs.by_geo.csv'
    parsed_data = parse_csv(filename)
    for item in parsed_data:
        src_city_co = (float(item['src_latitude']),
                       float(item['src_longitude']))
        dst_city_co = (float(item['dst_latitude']),
                       float(item['dst_longitude']))

        print(src_city_co)
        print(dst_city_co)

        src_city_co = find_closest_point(
            src_city_co, coord_set)

        dst_city_co = find_closest_point(
            dst_city_co, coord_set)

        src_city_info = coordCityMap[src_city_co]

        dst_city_info = coordCityMap[dst_city_co]

        shortest_path_cities = nx.shortest_path(
            G, source=src_city_info, target=dst_city_info, weight='distance')

        shortest_distance, coordinate_list, wkt_list, cable_type_list = calculate_shortest_path_distance(
            G, shortest_path_cities)

        print(src_city_co)
        print(dst_city_co)

        print(src_city_info)
        print(dst_city_info)

        print(wkt_list)
        print(cable_type_list)
        print(coordinate_list)
        print("\n")


if __name__ == "__main__":
    db_file = '../database/igdb.db'
    coordCityMap, coord_set, G = build_up_global_graph(db_file)
    run()
    # function_tester()
