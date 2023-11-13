from xml.dom.minidom import Document
import sqlite3
import argparse


def convert_multilinestring_to_list(multilinestring):
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
                list_of_points.append((x, y))

        # After all points of a linestring are processed, append the list of points to the list of linestring lists
        list_of_linestring_lists.append(list_of_points)

    return list_of_linestring_lists


def convert_land_multilinestring_to_list(multilinestring):
    # Remove the 'MULTILINESTRING ' prefix and split the string into individual linestrings
    linestrings = multilinestring.replace(
        'LINESTRING', '').strip('()').split('), (')
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
                list_of_points.append((x, y))

        # After all points of a linestring are processed, append the list of points to the list of linestring lists
        list_of_linestring_lists.append(list_of_points)

    return list_of_linestring_lists

# Correcting the structure of the provided MULTILINESTRING data to properly format it into KML
# Removing the extra for loop which was causing the unpacking error


def create_file(whole_data):
    # Create a DOM document for KML
    doc = Document()

    # Define the KML element, which is the root of the document
    kml = doc.createElement('kml')
    kml.setAttribute('xmlns', 'http://www.opengis.net/kml/2.2')
    doc.appendChild(kml)

    # Create the Document element, which will contain the placemarks
    document = doc.createElement('Document')
    kml.appendChild(document)

    # Define the name and description for the document
    doc_name = doc.createElement('name')
    doc_name.appendChild(doc.createTextNode('Multiline Strings'))
    document.appendChild(doc_name)

    doc_desc = doc.createElement('description')
    doc_desc.appendChild(doc.createTextNode('KML for multiple line strings'))
    document.appendChild(doc_desc)
    # Create Placemark for each LineString in the MULTILINESTRING
    for line in whole_data:
        placemark = doc.createElement('Placemark')

        # Create a LineString element for this line
        line_string = doc.createElement('LineString')
        coordinates = doc.createElement('coordinates')

        # Convert coordinates to text string
        coordinates_text = ' '.join(
            [f"{lon},{lat},0" for lon, lat in line])  # altitude set to 0
        coordinates.appendChild(doc.createTextNode(coordinates_text))

        line_string.appendChild(coordinates)
        placemark.appendChild(line_string)
        document.appendChild(placemark)
    return doc


def get_data_from_database(db_file, whole_data, cable_id):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT sc.cable_wkt FROM submarine_cables sc WHERE sc.cable_id = ?;', (cable_id,))
    datas = cursor.fetchall()
    for data in datas:
        whole_data = whole_data + convert_multilinestring_to_list(data[0])

    conn.close()
    return whole_data


def get_land_data_from_database(db_file, whole_data, country_id):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT sp.path_wkt FROM standard_paths sp where to_country = ? AND from_country = ?;', (country_id, country_id,))
    datas = cursor.fetchall()
    for data in datas:
        whole_data = whole_data + convert_land_multilinestring_to_list(data[0])

    conn.close()
    return whole_data


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        dest='model', required=True, help='Specify the data model to use: land or submarine.')

    # Subparser for land data
    land_parser = subparsers.add_parser(
        'land', help='For land data, no second argument is required.')
    land_parser.add_argument('country_id', type=str,
                             help='The country for the standard path.')

    # Subparser for submarine data
    submarine_parser = subparsers.add_parser(
        'submarine', help='For submarine data, a cable_id is required.')
    submarine_parser.add_argument(
        'cable_id', type=str, help='The ID of the submarine cable.')

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    # Save the KML to a file
    args = parse_args()
    whole_data = []
    kml_file_path = './multilinestring.kml'
    db_file_path = '../database/igdb.db'
    if args.model == 'submarine':
        kml_file_path = f'{args.cable_id}.kml'
        whole_data = get_data_from_database(
            db_file_path, whole_data, args.cable_id)
    elif args.model == 'land':
        kml_file_path = f'{args.country_id}.kml'
        # Use land database function
        whole_data = get_land_data_from_database(
            db_file_path, whole_data, args.country_id)

    # get_land_data_from_database('../database/igdb.db')
    doc = create_file(whole_data)
    with open(kml_file_path, 'w') as kml_file:
        kml_file.write(doc.toprettyxml(indent="  "))
