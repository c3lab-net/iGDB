#!/usr/bin/env python3

import logging
import sqlite3
from haversine import haversine
from shapely.geometry import LineString

from Common import are_coordinates_close, init_logging, parse_wkt_linestring
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


def get_standard_path_city_coord_from_database(db_file):
    print("\tGetting standard path city coordinates from database...")
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute('''SELECT from_city, from_state, from_country, to_city, to_state, to_country, path_wkt
                        FROM standard_paths;''')
        all_rows = cursor.fetchall()

    def _add_to_mapping(mapping, city, state, country, lat, lon):
        key = (city, state, country)
        value = (lat, lon)
        if key not in mapping:
            mapping[key] = value
        else:
            orig_value = mapping[key]
            if not are_coordinates_close(orig_value, value, max_distance_km=10):
                logging.warning(f'Existing city {key}: {orig_value}; new coordinate: {value};'
                                f'distance: {haversine(orig_value, value)}km')

    city_to_coordinate_mapping = {}
    for from_city, from_state, from_country, to_city, to_state, to_country, path_wkt in all_rows:
        ls: LineString = parse_wkt_linestring(path_wkt)
        # wkt path is (lon, lat)
        from_lon, from_lat = ls.coords[0]
        to_lon, to_lat = ls.coords[-1]
        _add_to_mapping(city_to_coordinate_mapping, from_city, from_state, from_country, from_lat, from_lon)
        _add_to_mapping(city_to_coordinate_mapping, to_city, to_state, to_country, to_lat, to_lon)

    return [(lat, lon, city, state, country) for (city, state, country), (lat, lon) in \
            city_to_coordinate_mapping.items()]


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

    for landing_point, standard_path_city_nodes in city_mapping.items():
        from_city, from_state, from_country, from_coord = landing_point
        for standard_path_city_node in standard_path_city_nodes:
            to_city, to_state, to_country, to_coord, distance = standard_path_city_node
            path_wkt = coord_list_to_linestring([from_coord, to_coord])
            # Execute insert query
            cursor.execute("INSERT INTO submarine_to_standard_paths (from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (from_city, from_state, from_country, to_city, to_state, to_country, distance, path_wkt))

    conn.commit()
    conn.close()


def get_all_submarine_to_standard_paths_pairs(db_file):
    print('Loading submarine to standard paths pairs from database ...')
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

def map_landing_point_to_standard_path_cities(landing_point_coords, standard_path_coords) -> dict[tuple, list[tuple]]:
    print("\tMapping landing point cities to nearby standard path cities...")
    DISTANCE_THRESHOLD_KM = 160
    landing_point_to_standard_path_cities = {}
    for landing_point_lati, landing_point_longti, landing_point_city, landing_point_state, landing_point_country in landing_point_coords:
        landing_point_coord = (landing_point_lati, landing_point_longti)
        landing_point = (landing_point_city, landing_point_state, landing_point_country, landing_point_coord)
        standard_path_city_nodes = []
        for sp_lat, sp_lon, sp_city, sp_state, sp_country in standard_path_coords:
            sp_coord = (sp_lat, sp_lon)
            distance_km = haversine(landing_point_coord, sp_coord)
            if distance_km > DISTANCE_THRESHOLD_KM:
                continue
            standard_path_city_node = (sp_city, sp_state, sp_country, sp_coord, distance_km)
            standard_path_city_nodes.append(standard_path_city_node)

        logging.debug(f'Landing point {landing_point} close city count: {len(standard_path_city_nodes)}')
        if len(standard_path_city_nodes) > 0:
            logging.debug(f'median distance: {standard_path_city_nodes[len(standard_path_city_nodes)//2][4]}km')
        # Map each landing point to three closest cities
        MAX_NODES = 3
        standard_path_city_nodes = sorted(standard_path_city_nodes, key=lambda n: n[4])[:MAX_NODES]
        landing_point_to_standard_path_cities[landing_point] = standard_path_city_nodes
    return landing_point_to_standard_path_cities


def connect_submarine_cable_to_standard_path(db_file: str):
    """Create a new table that connect the submarine cable cities to the closest standard path city."""
    print("Connecting submarine cable to standard path...")
    landing_point_coords = get_landing_point_coord_from_database(db_file)
    standard_path_city_coords = get_standard_path_city_coord_from_database(db_file)
    city_mapping = map_landing_point_to_standard_path_cities(landing_point_coords, standard_path_city_coords)
    insert_submarine_city_mapping_to_standard_path_city_database(db_file, city_mapping)
    print("Done.")

if __name__ == "__main__":
    db_file = '../database/igdb.db'
    init_logging(level=logging.DEBUG)
    connect_submarine_cable_to_standard_path(db_file)
    get_all_submarine_to_standard_paths_pairs(db_file)
