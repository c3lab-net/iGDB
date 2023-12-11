import sqlite3
import sys
from haversine import haversine
from ConvertToStandardPath_SubmarineCable import coord_list_to_linestring


def get_landing_point_coord_from_database(db_file):
    landing_point_coord = []
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT lp.latitude, lp.longitude, lp.city_name, lp.state_province, lp.country FROM landing_points lp;')
    datas = cursor.fetchall()
    for data in datas:
        landing_point_coord.append(data)

    conn.close()
    return set(landing_point_coord)


def get_phys_nodes_coord_from_database(db_file):
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

    for key, value in city_mapping.items():
        from_city, from_state, from_country = key
        to_city, to_state, to_country, distance, landing_point_coord, phys_nodes_coord = value
        path_wkt = coord_list_to_linestring(
            [landing_point_coord, phys_nodes_coord])
        # Execute insert query
        cursor.execute("INSERT INTO submarine_to_standard_paths (from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (from_city, from_state, from_country, to_city, to_state, to_country, distance, path_wkt))

    conn.commit()
    conn.close()


def get_all_submarine_to_standard_paths_pairs(db_file):
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


if __name__ == "__main__":
    db_file = '../database/igdb.db'
    landing_point_coords = get_landing_point_coord_from_database(db_file)
    phys_nodes_coords = get_phys_nodes_coord_from_database(db_file)
    city_mapping = {}
    for landing_point_lati, landing_point_longti, landing_point_city, landing_point_state, landing_point_country in landing_point_coords:
        landing_point_coord = (landing_point_lati, landing_point_longti)
        min_distance = float('inf')
        best_city = None
        best_state = None
        best_country = None
        best_coord = None
        for phys_nodes_lati, phys_nodes_longti, phys_nodes_city, phys_nodes_state, phys_nodes_country in phys_nodes_coords:
            phys_nodes_coord = (phys_nodes_lati, phys_nodes_longti)
            if not isinstance(phys_nodes_coord[0], float):
                continue
            currdistance = haversine(landing_point_coord, phys_nodes_coord)
            if (currdistance < min_distance):
                best_city = phys_nodes_city
                min_distance = currdistance
                best_state = phys_nodes_state
                best_country = phys_nodes_country
                best_coord = phys_nodes_coord

        if (min_distance > 160):
            print(
                f"warning for city {landing_point_city}, {landing_point_country} and {best_city}, {best_country} with distance {min_distance}", file=sys.stderr)
        else:
            city_mapping[(landing_point_city, landing_point_state, landing_point_country)] = (
                best_city, best_state, best_country, min_distance, landing_point_coord, best_coord)
    insert_submarine_city_mapping_to_standard_path_city_database(
        db_file, city_mapping)
    get_all_submarine_to_standard_paths_pairs(db_file)
