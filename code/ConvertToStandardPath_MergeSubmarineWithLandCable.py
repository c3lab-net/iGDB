import logging
import sqlite3
import sys
from haversine import haversine
from ConvertToStandardPath_SubmarineCable import coord_list_to_linestring


def get_landing_point_coord_from_database(db_file):
    print("\tGetting landing point coordinates from database...")
    landing_point_coord = []
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        """SELECT lp.latitude, lp.longitude, clp.city_name, clp.state_province, clp.country
            FROM cable_landing_points clp, landing_points lp
            WHERE clp.city_name = lp.city_name 
                AND (' ' || clp.country) = lp.country;""")
    datas = cursor.fetchall()
    for data in datas:
        landing_point_coord.append(data)

    conn.close()
    return set(landing_point_coord)


def get_phys_nodes_coord_from_database(db_file):
    print("\tGetting physical nodes coordinates from database...")
    phys_nodes_coord = []
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('SELECT pn.latitude, pn.longitude, pn.city, pn.state, pn.country \
                    FROM phys_nodes pn \
                    WHERE EXISTS ( \
                        SELECT 1  \
                        FROM standard_paths  \
                        WHERE pn.city = standard_paths.from_city OR pn.city = standard_paths.to_city \
                    );')
    datas = cursor.fetchall()
    for data in datas:
        phys_nodes_coord.append(data)
    conn.close()
    return set(phys_nodes_coord)


def insert_submarine_city_mapping_to_standard_path_city_database(db_file, city_mapping):
    print("\tAdding new mapping to database...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='submarine_to_standard_paths';")
    table_exists = cursor.fetchone()

    # If the table exists, drop it
    if table_exists:
        cursor.execute("DROP TABLE submarine_to_standard_paths;")

    cursor.execute("CREATE TABLE submarine_to_standard_paths ( \
        from_city TEXT, \
        from_state TEXT, \
        from_country TEXT, \
        to_city TEXT, \
        to_state TEXT, \
        to_country TEXT, \
        distance_km REAL, \
        path_wkt TEXT \
    );")

    for landing_point, physical_nodes in city_mapping.items():
        from_city, from_state, from_country, from_coord = landing_point
        for physical_node in physical_nodes:
            to_city, to_state, to_country, to_coord, distance = physical_node
            path_wkt = coord_list_to_linestring([from_coord, to_coord])
            # Execute insert query
            cursor.execute("INSERT INTO submarine_to_standard_paths (from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (from_city, from_state, from_country, to_city, to_state, to_country, distance, path_wkt))

    conn.commit()
    conn.close()


def get_all_submarine_to_standard_paths_pairs(db_file):
    logging.info('Loading submarine to standard paths pairs from database ...')
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    sql_query = """
    SELECT stsp.from_city, stsp.from_state, stsp.from_country, stsp.to_city, stsp.to_state, stsp.to_country, stsp.distance_km, stsp.path_wkt
    FROM submarine_to_standard_paths stsp
    """
    cursor.execute(sql_query)
    datas = cursor.fetchall()
    conn.close()

    return datas

def map_landing_point_to_physical_nodes(landing_point_coords, phys_nodes_coords) -> dict[tuple, list[tuple]]:
    print("\tMapping landing point cities to nearby physical nodes...")
    DISTANCE_THRESHOLD_KM = 160
    city_mapping = {}
    for landing_point_lati, landing_point_longti, landing_point_city, landing_point_state, landing_point_country in landing_point_coords:
        landing_point_coord = (landing_point_lati, landing_point_longti)
        for phys_nodes_lati, phys_nodes_longti, phys_nodes_city, phys_nodes_state, phys_nodes_country in phys_nodes_coords:
            phys_nodes_coord = (phys_nodes_lati, phys_nodes_longti)
            if not isinstance(phys_nodes_coord[0], float):
                continue
            distance_km = haversine(landing_point_coord, phys_nodes_coord)
            if distance_km > DISTANCE_THRESHOLD_KM:
                # print(
                    # f"warning for city {landing_point_city}, {landing_point_country} and {phys_nodes_city}, {phys_nodes_country} with distance {distance_km}", file=sys.stderr)
                continue

            landing_point = (landing_point_city, landing_point_state, landing_point_country, landing_point_coord)
            physical_node = (phys_nodes_city, phys_nodes_state, phys_nodes_country, phys_nodes_coord, distance_km)
            if landing_point not in city_mapping:
                city_mapping[landing_point] = []
            city_mapping[landing_point].append(physical_node)
    return city_mapping


def connect_submarine_cable_to_standard_path(db_file: str):
    """Create a new table that connect the submarine cable cities to the closest standard path city."""
    print("Connecting submarine cable to standard path...")
    landing_point_coords = get_landing_point_coord_from_database(db_file)
    phys_nodes_coords = get_phys_nodes_coord_from_database(db_file)
    city_mapping = map_landing_point_to_physical_nodes(landing_point_coords, phys_nodes_coords)
    insert_submarine_city_mapping_to_standard_path_city_database(db_file, city_mapping)
    print("Done.")

if __name__ == "__main__":
    db_file = '../database/igdb.db'
    connect_submarine_cable_to_standard_path(db_file)
    get_all_submarine_to_standard_paths_pairs(db_file)
