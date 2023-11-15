import Querying_Database as qdb
import shapely.geometry as sg
import xml.etree.ElementTree as ET

from shapely import wkt
from typing import List, Optional


def wkt_to_kml(n, linestring):
    """Converts a WKT linestring to a KML linestring."""

#  linestring = sg.LineString(wkt_linestring)
    pl = ET.Element("Placemark")
    ls = ET.SubElement(pl, "LineString")
    name = ET.SubElement(pl, "name")
    name.text = n
    kml_linestring = ET.SubElement(ls, 'coordinates')
    ET.indent(kml_linestring)
    pointstring = ""
    for point in linestring.coords:
        pointstring += f'{point[0]},{point[1]} '

    kml_linestring.text = pointstring
    return pl

def linestrings_to_kml(names, linestrings):
    """Converts a list of WKT linestrings to a KML document."""

    kml_document = ET.Element('kml', xmlns='http://www.opengis.net/kml/2.2')
    kml_document.append(ET.Element('Document'))

    for n, wkt_linestring in zip(names, linestrings):
        kml_linestring = wkt_to_kml(n, wkt_linestring)
        kml_document.find('Document').append(kml_linestring)

    return ET.tostring(kml_document)

def get_linestrings(db_path: str):
    querier = qdb.queryDatabase(db_path)
    query = f"""SELECT * FROM standard_paths sp where sp.to_city == "San Jose" or sp.from_city == "San Jose" limit 900;"""
    results = querier.execute_query(query)
    
    ls = []
    ns = []

    for row in results:
        fc = row[0]
        fs = row[1]
        fcc = row[2]
        tc = row[3]
        ts = row[4]
        tcc = row[5]
        dist_km = float(row[6])
        path_wkt = row[7]
        edge = ((fc, fs, fcc), (tc, ts, tcc))

        linestring = wkt.loads(path_wkt)
        ns.append(f"{fc} {tc}")
        ls.append(linestring)

    return ns, ls 


@dataclass
class LineStringToKML:
    linestring: shapely.LineString
    name: Optional[str]
    description: Optional[str]

    def convert_to_xml_placemark(self) -> ET.Element:
        pl = ET.Element("Placemark")
        ls = ET.SubElement(pl, "LineString")
        if self.name:
            name = ET.SubElement(pl, "name")
            name.text = self.name
        if self.description:        
            description = ET.SubElement
            description.text = self.description
        kml_linestring = ET.SubElement(ls, 'coordinates')
        pointstring = ""
        for point in self.linestring.coords:
            pointstring += f'{point[0]},{point[1]} '

        kml_linestring.text = pointstring
        return pl


def write_linestrings_to_file(file_path: str, ls_info: List[LineStringToKML]):
    
    kml_document = ET.Element('kml', xmlns='http://www.opengis.net/kml/2.2')
    kml_document.append(ET.Element('Document'))
    for info in ls_info:
        kml_placemark = info.convert_to_xml_placemark()
        kml_document.find("Document").append(kml_placemark)


    with open('linestrings.kml', 'wb') as kml_file:
        kml_file.write(kml_string)


names, linestrings = get_linestrings("../database/igdb.db")

kml_string = linestrings_to_kml(names, linestrings)

with open('linestrings.kml', 'wb') as kml_file:
  kml_file.write(kml_string)
