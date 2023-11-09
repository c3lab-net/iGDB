#!/usr/bin/env python3

from geopy.distance import geodesic
import sqlite3
import sys
import networkx as nx
import matplotlib.pyplot as plt
import Querying_Database as qdb
from math import inf
from networkx.exception import NetworkXNoPath
from collections import defaultdict

def find_shortest_path(db_file, start_city, start_state, start_country, end_city, end_state, end_country):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Query the nodes and connections from the database
    cursor.execute('SELECT node_name, city, state, country FROM phys_nodes')
    nodes = cursor.fetchall()

    cursor.execute('SELECT from_node, to_node FROM phys_nodes_conn')
    connections = cursor.fetchall()

    # Create a directed graph using networkx
    G = nx.DiGraph()

    # Add nodes to the graph
    for node in nodes:
        G.add_node(node[0], city=node[1], state=node[2], country=node[3])

    # Add edges (connections) to the graph
    for connection in connections:
        if connection[0] not in G.nodes or connection[1] not in G.nodes:
            # print('Bad connection: ', connection, file=sys.stderr)
            continue
        G.add_edge(connection[0], connection[1])
        # assume symmetric connections
        G.add_edge(connection[1], connection[0])

    # Find nodes matching the start and end city/state pairs
    start_nodes = [node[0] for node in nodes if node[1] == start_city and
                   (node[2] == start_state or not start_state) and
                   node[3] == start_country]
    end_nodes = [node[0] for node in nodes if node[1] == end_city and
                 (node[2] == end_state or not end_state) and
                 node[3] == end_country]

    print('# of start nodes:', len(start_nodes))
    print('# of end nodes:', len(end_nodes))

    def _node_str(n):
        return f'{n["city"]}/{n["country"]}'

    # Find the shortest path for each combination of start and end nodes
    all_paths = []
    for start_node in start_nodes:
        for end_node in end_nodes:
            try:
                shortest_path = nx.shortest_path(G, source=start_node, target=end_node)
                shortest_path_location = [_node_str(G.nodes[node]) for node in shortest_path]
                all_paths.append(shortest_path_location)
            except NetworkXNoPath:
                pass
                #print(f"No path found from {start_city}, {start_country} to {end_city}, {end_country}")

    # Print the result
    #print(f"Shortest paths from {start_city}, {start_country} to {end_city}, {end_country}:")
    path_distribution = defaultdict(int)
    for path in all_paths:
        path_distribution[str(path)] += 1

    print(f"Path distribution from {start_city}, {start_country} to {end_city}, {end_country}:")
    for path, count in path_distribution.items():
        print(f"{count}: {path}")

    # Optionally, plot the graph
    """
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, font_weight='bold', node_size=700, node_color='skyblue')
    plt.savefig('nx.png')
    #plt.show()
    """

    # Close the database connection
    conn.close()

def cloud_region_to_location(region: str):
    region_to_ip = {"us-west-1": (37.2379, -121.7946),
                    "us-east-1": (39.0127, -77.5342)}

    if region not in region_to_ip:
        raise Exception()

    cloud_lat, cloud_lon = region_to_ip[region]
    nodes_query = f"""SELECT * FROM city_points;"""
    querier = qdb.queryDatabase("../database/igdb.db")
    results = querier.execute_query(nodes_query)
    if not results:
        raise Exception()

    min_distance, min_city, min_state, min_country = inf, None, None, None
    for row in results:
        city = row[0]
        state = row[1]
        country = row[2]
        lat = float(row[3])
        lon = float(row[4])
        distance = geodesic((cloud_lat, cloud_lon), (lat, lon))
        if distance < min_distance:
            min_distance = distance
            min_city, min_state, min_country = city, state, country

    print(f"Found location: {min_city, min_state, min_country} for cloud region: {region}", file=sys.stderr)
    return min_city, min_state, min_country


def parse_location(location):
    try:
        city, state, country = cloud_region_to_location(location)
        return city, state, country
    except:
        pass

    parts = location.split('/')

    # Set default values
    city = parts[0]
    state = ""
    country = ""

    # Update values based on available parts
    if len(parts) == 2:
        country = parts[1]
    elif len(parts) == 3:
        state = parts[1]
        country = parts[2]

    return city, state, country

def parse_arguments():
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <src> <dst>")
        sys.exit(1)

    # Extract arguments
    src = parse_location(sys.argv[1])
    dst = parse_location(sys.argv[2])

    return src, dst

# Example usage
if __name__ == "__main__":
    # Parse command-line arguments
    src, dst = parse_arguments()

    find_shortest_path('../database/processed.db',
                    src[0], src[1], src[2],
                    dst[0], dst[1], dst[2])

