import sqlite3
import networkx as nx
import matplotlib.pyplot as plt
import itertools
from networkx.exception import NetworkXNoPath
from geopy.distance import geodesic
import ast


def floatFormatter(number):
    """After verification, there is no city that has the same coordinate in 0.01 granuality
    to avoid the precious problem lead to float number, we introduce this floatFormatter to
    format coordinates."""
    return float("{:.2f}".format(number))


def nodeFormatter(coords):
    longitude, latitude = coords
    return (latitude, longitude)


def plot_graph(G):
    """
    This function is a helper function for debugging graph

    Parameters:
    G : a networkX graph

    """
    # Position nodes using a layout algorithm
    pos = nx.spring_layout(G, scale=2)  # scale parameter to spread nodes

    plt.figure(figsize=(50, 50))
    nx.draw(G, pos, with_labels=True, node_size=1000,
            node_color='lightblue', edge_color='gray', font_size=10)

    edge_labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=10)

    plt.title('Graph Visualization of Paths')
    plt.savefig('./graph_visualization.jpg', format='jpg', dpi=300)
    plt.close()
    plt.show()


def convert_multilinestring_to_list(multilinestring):
    """
    This function convert multilinestring to a list of path, each path is a list of nodes, each nodes is a tuple of (longtitude, latitude)

    Parameters:
    multilinestring: The multilinestring data read from the database

    Returns:
    list_of_linestring_lists(list): a list of list in the following format [path1, path2 ... pathx]
    [[node 1, node2 .... nodeX], [node a, node b, node c] .... []].
    each node has the following format (longtitude, latitude)


    """
    # Remove the 'MULTILINESTRING ' prefix and split the string into individual linestrings
    linestrings = multilinestring.replace(
        'MULTILINESTRING ', '').strip('()').split('), (')
    # linestrings = linestrings.replace('"', '')
    list_of_linestring_lists = []  # This will be the final list of lists to return

    # Iterate over each linestring
    for linestring in linestrings:
        points = linestring.split(', ')  # Split the linestring into points
        list_of_points = []  # This will hold the tuples for the current linestring

        # Iterate over each point in the linestring
        for point in points:
            # Split the point into its x and y components and make sure to strip any potential white space
            # which might lead to a ValueError if trying to convert an empty string to float
            xy = point.strip().split(' ')
            if xy:  # Proceed only if the split is non-empty
                # Convert the x and y string components to float
                x, y = map(float, xy)
                # Append the tuple to the list of points
                list_of_points.append((floatFormatter(x), floatFormatter(y)))

        # After all points of a linestring are processed, append the list of points to the list of linestring lists
        list_of_linestring_lists.append(list_of_points)

    return list_of_linestring_lists

# Helper function to add edge


def add_edge(G, point1, point2):
    # Calculate the distance between the two points
    distance = geodesic(point1, point2)
    # Add the edge to the graph with distance as weight
    G.add_edge(point1, point2, weight=distance.kilometers)


def construct_graph_with_networkx(paths):
    """
    This function take a list of paths and return the corresponding graph.

    Parameters:
    paths(list) : a list of list in the following format [path1, path2 ... pathx]
    [[node 1, node2 .... nodeX], [node a, node b, node c] .... []].
    each node has the following format (longtitude, latitude)

    Returns:
    G (networkX graph): the graph generated for corresponding path list.


    """
    # Create a NetworkX graph
    G = nx.Graph()

    paths = [[nodeFormatter(node) for node in path] for path in paths]

    # Add edges for consecutive points within paths
    for path in paths:
        for i in range(len(path) - 1):
            add_edge(G, path[i], path[i + 1])

    # Connect paths by shared points
    for path in paths:
        # all_other_points includes all other points appreared in other paths.
        all_other_points = set(sum((p for p in paths if p is not path), []))

        # Only need to consider first and last points for connection
        for point in [path[0], path[-1]]:
            point_neighbor_list = [neighbor for neighbor in G.neighbors(
                point) if neighbor not in path]
            # if the point also appear in other paths, merge them
            if point_neighbor_list:
                for neighbor in point_neighbor_list:
                    add_edge(G, point, neighbor)
            # if the point does not appear in other paths, find the closest point to it
            # if the distance between them smaller than 10 kilometer, then connect them.
            else:
                if all_other_points:
                    # Find the closest point and its distance
                    closest_point, min_distance = min(
                        ((other_point, geodesic(point, other_point).kilometers)
                            for other_point in all_other_points),
                        key=lambda item: item[1]
                    )
                    threshold = 10
                    # Add edge if the closest distance is under the threshold and not in the current path
                    if min_distance < threshold and closest_point not in path:
                        add_edge(G, point, closest_point)

    return G


def verify_graph_with_cities(graph, start_city, end_city, cable_id):
    """
    Before calculate the distance between a city pair. This function used to verify whether both of the cities appears in
    the graph, if not, update the graph if the city is with in the 125 kilometer circle of any submarine cable points. Connect them with a edge with distance weight.

    Parameters:
    graph(networkx graph) : The original graph 
    start_city (tuple (float, float)): start city
    end_city (tuple (float, float)): end city
    cable_id (str): used for debug

    Returns:
    G (networkX graph): The updated graph 


    """
    all_points = set([p for p in graph])
    for city in [start_city, end_city]:
        if not city in graph:
            closest_point, min_distance = min(
                ((other_point, geodesic(city, other_point).kilometers)
                 for other_point in all_points),
                key=lambda item: item[1]
            )
            threshold = 125
            # Add edge if the closest distance is under the threshold and not in the current path
            if min_distance < threshold:
                add_edge(graph, city, closest_point)
            else:
                raise Exception(
                    f"The minimum distance {min_distance} between {city}, {closest_point} is more than the threshold of {threshold}.")
    return graph


def get_data_from_database(db_file):
    """
    This function get data from database and organize them in to a dictionary in the format {cable_info: cities_on_the_cable}

    Parameters:
    db_file (str): path for the database

    Returns:
    cable_id_to_cities (dict): {cable_info: cities_on_the_cable}
    cable_info is a tuple of (cable_id, cable_wkt)
    cities_on_the_cable is a tuple consist of (city_name, city_latitude, city_longitude)


    """
    global whole_data
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    sql_query = """
    SELECT sc.cable_id, sc.cable_wkt, clp.city_name, clp.state_province, clp.country, lp.latitude, lp.longitude
    FROM submarine_cables sc, cable_landing_points clp, landing_points lp
    WHERE sc.cable_id = clp.cable_id 
    AND clp.city_name = lp.city_name 
    AND (' ' || clp.country) = lp.country;
    """
    cursor.execute(sql_query)
    datas = cursor.fetchall()
    cable_id_to_cities = {}
    cable_id_to_wkt = {}
    for cable_id, cable_wkt, _, _, _, _, _ in datas:
        if cable_id not in cable_id_to_wkt:
            cable_id_to_wkt[cable_id] = set()
        for path in convert_multilinestring_to_list(cable_wkt):
            cable_id_to_wkt[cable_id].add(str(path))

    for cable_id, _, city_name, city_state, city_country, city_latitude, city_longitude in datas:
        if cable_id not in cable_id_to_cities:
            cable_id_to_cities[cable_id] = set()
        cable_id_to_cities[cable_id].add(
            (city_name, city_state, city_country, city_latitude, city_longitude))
    conn.close()

    for cable_id, cable_wkt, _, _, _, _, _ in datas:
        cable_id_to_wkt[cable_id] = [ast.literal_eval(
            str(list_str)) for list_str in cable_id_to_wkt[cable_id]]

    return cable_id_to_cities, cable_id_to_wkt


def coord_list_to_linestring(coords_list):
    """
    Converts a list of coordinate tuples into a WKT LINESTRING format.

    Parameters:
    coords_list (list): A list of tuples, each representing a latitude and longitude pair.

    Returns:
    str: A WKT LINESTRING representation of the input coordinates.
    """
    # Convert each tuple in the list to a string, formatted as "longitude latitude"
    coords_str_list = [f"{lon} {lat}" for lat, lon in coords_list]

    # Join all the coordinate strings with a comma and space
    linestring = ', '.join(coords_str_list)

    # Return the formatted LINESTRING
    return f"LINESTRING({linestring})"


def calculate_distance_between_cable_landing_points(cable_id_to_cities, cable_id_to_wkt):
    """
    This function take a dict consist of {cable_info: cities_on_the_cable} as input 
    and then calculate all path distance with city pairs on this cable.

    Parameters:
    cable_id_to_cities (dict): {cable_info: cities_on_the_cable}
    cable_info is a tuple of (cable_id, cable_wkt)
    cities_on_the_cable is a tuple consist of (city_name, city_latitude, city_longitude)

    Returns:
    the distance for cities pairs are printed out. You can store that in file or database table.


    """
    submarine_standard_paths = []
    for cable_id, cities_info in cable_id_to_cities.items():
        # Build up the graph for cable
        cable_id_path_list = cable_id_to_wkt[cable_id]
        graph = construct_graph_with_networkx(cable_id_path_list)

        # get all city pairs on the cable
        city_pairs = list(itertools.combinations(cities_info, 2))

        # calculate each city pair distance on upper graph
        for city_pair in city_pairs:
            # format the start city and end city and verify them on the graph.
            start_city, end_city = city_pair
            start_city_name, start_city_state, start_city_country, start_city_lati, start_city_longti = start_city
            end_city_name, end_city_state, end_city_country, end_city_lati, end_city_longti = end_city

            start_city_coord = (floatFormatter(
                start_city_lati), floatFormatter(start_city_longti))
            end_city_coord = (floatFormatter(end_city_lati),
                              floatFormatter(end_city_longti))
            graph = verify_graph_with_cities(
                graph, start_city_coord, end_city_coord, cable_id)

            try:
                shortest_path = nx.shortest_path(
                    graph, source=start_city_coord, target=end_city_coord, weight='weight')
                shortest_path_length = nx.shortest_path_length(
                    graph, source=start_city_coord, target=end_city_coord, weight='weight')
                # print(
                #      f"city {start_city_name}, {start_city_state}, {start_city_country}, {start_city_coord}, city {end_city_name}, {end_city_state}, {end_city_country}, {end_city_coord} on {cable_id} has shortest_path_length {shortest_path_length} consist of {shortest_path}")
                submarine_standard_paths.append(((start_city_name, start_city_state, start_city_country,
                                                  end_city_name, end_city_state, end_city_country,
                                                  shortest_path_length, coord_list_to_linestring(shortest_path))))
            except NetworkXNoPath:
                '''This will catch the exception when the cities on the cable will not form a connected graph.
                which means there is no path betweent the city pair. We simply omit it by continue.'''
                continue
                # print(
                #     f"city {start_city_name}, {start_city_state}, {start_city_country}, {start_city_coord}, city {end_city_name}, {end_city_state}, {end_city_country}, {end_city_coord} on {cable_id} does not have shortest_path_length")
    return submarine_standard_paths


def insert_submarine_standard_paths_to_database(db_file, submarine_standard_paths):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='submarine_standard_paths';")
    table_exists = cursor.fetchone()

    # If the table exists, drop it
    if table_exists:
        cursor.execute("DROP TABLE submarine_standard_paths;")

    cursor.execute("CREATE TABLE submarine_standard_paths ( \
        from_city TEXT, \
        from_state TEXT, \
        from_country TEXT, \
        to_city TEXT, \
        to_state TEXT, \
        to_country TEXT, \
        distance_km REAL, \
        path_wkt TEXT \
    );")

    for submarine_standard_path in submarine_standard_paths:
        # Execute insert query
        cursor.execute("INSERT INTO submarine_standard_paths (from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", submarine_standard_path)

    conn.commit()
    conn.close()


def get_all_submarine_standard_paths(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    sql_query = """
    SELECT ssp.from_city, ssp.from_state, ssp.from_country, ssp.to_city, ssp.to_state, ssp.to_country, ssp.distance_km, ssp.path_wkt
    FROM submarine_standard_paths ssp
    """
    cursor.execute(sql_query)
    datas = cursor.fetchall()
    conn.close()
    return datas


def add_submarine_cable_like_standard_path(db_file: str):
    """Add submarine cable in standard path format to a new table."""
    cable_id_to_cities, cable_id_to_wkt = get_data_from_database(db_file)
    submarine_standard_paths = calculate_distance_between_cable_landing_points(
        cable_id_to_cities, cable_id_to_wkt)
    insert_submarine_standard_paths_to_database(
        db_file, submarine_standard_paths)

if __name__ == "__main__":
    db_file = '../database/igdb.db'
    add_submarine_cable_like_standard_path(db_file)
    get_all_submarine_standard_paths(db_file)
