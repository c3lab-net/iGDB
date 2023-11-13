import sqlite3
import math

def get_landing_point_coord_from_database(db_file):
    landing_point_coord = []
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('SELECT lp.latitude, lp.longitude, lp.city_name, lp.country FROM landing_points lp;')
    datas = cursor.fetchall()
    for data in datas:
        landing_point_coord.append(data)

    conn.close()
    return set(landing_point_coord)

def get_phys_nodes_coord_from_database(db_file):
    phys_nodes_coord = []
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('SELECT pn.latitude, pn.longitude, pn.city, pn.country \
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

def haversine(coord1, coord2):
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

    return distance


if __name__ == "__main__":
    landing_point_coords = get_landing_point_coord_from_database('../database/igdb.db')
    phys_nodes_coords = get_phys_nodes_coord_from_database('../database/igdb.db')
    print(len(landing_point_coords))
    print(len(phys_nodes_coords))
    mapping = {}
    for landing_point_lati, landing_point_longti, landing_point_city, landing_point_country in landing_point_coords:
        landing_point_coord = (landing_point_lati, landing_point_longti)
        max_distance = float('inf')
        best_city = None
        best_country = None
        for phys_nodes_lati, phys_nodes_longti, phys_nodes_city, phys_nodes_country in phys_nodes_coords:
            phys_nodes_coord = (phys_nodes_lati, phys_nodes_longti)
            if not isinstance(phys_nodes_coord[0], float):
                continue
            currdistance  = haversine(landing_point_coord, phys_nodes_coord)
            if(currdistance < max_distance):
                best_city = phys_nodes_city
                max_distance = currdistance
                best_country = phys_nodes_country
        mapping[landing_point_city] = best_city
        if(max_distance > 160):
            print(f"warning for city {landing_point_city}, {landing_point_country} and {best_city}, {best_country} with distance {max_distance}")

    # print(mapping)