#!/usr/bin/env python3

import csv
import sys

from fastapi.testclient import TestClient
# Assuming initialize_graph is a function that sets up your graph
from Serving_API import app, build_up_global_graph

client = TestClient(app)

def parse_csv(filename):
    data = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data


def setup_test_environment():
    # Initialize graph and mappings here
    app.db_file = '../database/igdb.db'
    app.coord_city_map, app.coord_set, app.G, app.all_as_locations = build_up_global_graph(
        app.db_file)


def test_physical_route():
    setup_test_environment()

    # takes too long for testing all 5k cases, only test the first 100 cases
    test_counter = 0

    filename = 'all_pairs.by_geo.csv'
    parsed_data = parse_csv(filename)
    for item in parsed_data:
        # Define test coordinates (use realistic but controlled values)
        src_latitude, src_longitude = float(
            item['src_latitude']), float(item['src_longitude'])
        dst_latitude, dst_longitude = float(
            item['dst_latitude']), float(item['dst_longitude'])

        # Make the request
        response = client.get(
            f"/physical-route/?src_latitude={src_latitude}&src_longitude={src_longitude}&dst_latitude={dst_latitude}&dst_longitude={dst_longitude}&cloud_region_scope={str('amazon')}")

        # Assertions
        if response.status_code == 200:
            data = response.json()
            assert 'routers_latlon' in data
            assert 'distance_km' in data
            assert 'fiber_wkt_paths' in data
            assert 'fiber_types' in data
        else:
            print(response.status_code, file=sys.stderr)
            print(src_latitude, src_longitude, dst_latitude, dst_longitude, file=sys.stderr)

        test_counter += 1
        if (test_counter > 100):
            break

    # You can add more assertions to validate the response content


if __name__ == "__main__":
    test_physical_route()
