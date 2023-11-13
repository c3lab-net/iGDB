import sqlite3
import math
import networkx as nx
import matplotlib.pyplot as plt
import itertools
from networkx.exception import NetworkXNoPath


def floatFormatter(number):
    return float("{:.2f}".format(number))


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


def haversine(coord1, coord2):
    """
    This function take two input coordinates with format: (longtitude, latitude)
    return the distance between them in kilometer.
    
    Parameters:
    coord1 (tuple(float, float)): start coord
    coord2 (tuple(float, float)): end coord
    
    Returns:
    distance (float): the distance between two coordinates. 
    
    
    """
    
    # Radius of the Earth in kilometers
    R = 6371.0

    # Coordinates in decimal degrees (e.g. 2.294481 to radians)
    lat1, lon1 = math.radians(coord1[1]), math.radians(coord1[0])
    lat2, lon2 = math.radians(coord2[1]), math.radians(coord2[0])

    # Change in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = math.sin(dlat/2)**2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c

    return floatFormatter(distance)

# Helper function to add edge


def add_edge(G, point1, point2):
    # Calculate the distance between the two points
    distance = haversine(point1, point2)
    # Add the edge to the graph with distance as weight
    G.add_edge(point1, point2, weight=distance)


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
                        ((other_point, haversine(point, other_point))
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
    if not start_city in graph:
        closest_point, min_distance = min(
            ((other_point, haversine(start_city, other_point))
             for other_point in all_points),
            key=lambda item: item[1]
        )
        threshold = 125
        # Add edge if the closest distance is under the threshold and not in the current path
        if min_distance < threshold:
            add_edge(graph, start_city, closest_point)
        # else:
            # print(f"{start_city}, {closest_point} can not be merged with distance: {min_distance} for calbe {cable_id}")
    if not end_city in graph:
        closest_point, min_distance = min(
            ((other_point, haversine(end_city, other_point))
             for other_point in all_points),
            key=lambda item: item[1]
        )
        threshold = 125
        # Add edge if the closest distance is under the threshold and not in the current path
        if min_distance < threshold:
            add_edge(graph, end_city, closest_point)
        # else:
        #     print(f"{end_city}, {closest_point} can not be merged with distance: {min_distance} for calbe {cable_id}")
    return graph


def get_data_from_database(db_file):
    """
    This function get data from database and organize them in to a dictionary in the format {cable_info: cities_on_the_cable}
    
    Parameters:
    db_file (str): path for the database
    
    Returns:
    wktToCities (dict): {cable_info: cities_on_the_cable}
    cable_info is a tuple of (cable_id, cable_wkt)
    cities_on_the_cable is a tuple consist of (city_name, city_latitude, city_longitude)
    
    
    """
    global whole_data
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    sql_query = """
    SELECT sc.cable_id, sc.cable_wkt, clp.city_name, lp.latitude, lp.longitude
    FROM submarine_cables sc, cable_landing_points clp, landing_points lp
    WHERE sc.cable_id = clp.cable_id 
    AND clp.city_name = lp.city_name 
    AND (' ' || clp.country) = lp.country;
    """
    cursor.execute(sql_query)
    datas = cursor.fetchall()
    wktToCities = {}
    for cable_id, cable_wkt, city_name, city_latitude, city_longitude in datas:
        if (cable_id, cable_wkt) in wktToCities:
            wktToCities[(cable_id, cable_wkt)].add(
                (city_name, city_latitude, city_longitude))
        else:
            wktToCities[(cable_id, cable_wkt)] = set()
            wktToCities[(cable_id, cable_wkt)].add(
                (city_name, city_latitude, city_longitude))
    conn.close()
    return wktToCities

def calculate_distance_between_cable_landing_points(wktToCities):
    """
    This function take a dict consist of {cable_info: cities_on_the_cable} as input 
    and then calculate all path distance with city pairs on this cable.
    
    Parameters:
    wktToCities (dict): {cable_info: cities_on_the_cable}
    cable_info is a tuple of (cable_id, cable_wkt)
    cities_on_the_cable is a tuple consist of (city_name, city_latitude, city_longitude)
    
    Returns:
    the distance for cities pairs are printed out. You can store that in file or database table.
    
    
    """
    fail_cities = set()
    fail_calbes = set()
    for (cable_id, cable_wkt), cities_info in wktToCities.items():
        
        # Build up the graph for cable
        path_list = convert_multilinestring_to_list(cable_wkt)
        graph = construct_graph_with_networkx(path_list)
        
        # get all city pairs on the cable
        city_pairs = list(itertools.combinations(cities_info, 2))
        
        # calculate each city pair distance on upper graph
        for city_pair in city_pairs:
            # format the start city and end city and verify them on the graph.
            start_city = (floatFormatter(
                city_pair[0][2]), floatFormatter(city_pair[0][1]))
            end_city = (floatFormatter(
                city_pair[1][2]), floatFormatter(city_pair[1][1]))
            graph = verify_graph_with_cities(
                graph, start_city, end_city, cable_id)
            
            if (start_city in graph and end_city in graph):
                try:
                    shortest_path = nx.shortest_path(
                        graph, source=start_city, target=end_city, weight='weight')
                    shortest_path_length = nx.shortest_path_length(
                        graph, source=start_city, target=end_city, weight='weight')
                    print(
                        f"city {city_pair[0][0]}, {start_city}, city {city_pair[1][0]}, {end_city}  has shortest_path_length {shortest_path_length} consist of {shortest_path}")
                except NetworkXNoPath:
                    fail_calbes.add(cable_id)
            else:
                if not start_city in graph:
                    fail_cities.add(start_city)
                    # print(f"city {city_pair[0][0]}, {start_city} not exist in graph for {cable_id}.")
                else:
                    fail_cities.add(end_city)
                    # print(f"city {city_pair[1][0]}, {end_city} not exist in graph for {cable_id}.")


if __name__ == "__main__":
    wktToCities =  get_data_from_database('../database/igdb.db')
    calculate_distance_between_cable_landing_points(wktToCities)
    
