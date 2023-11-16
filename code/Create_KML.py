import Querying_Database as qdb
import shapely.geometry as sg
import xml.etree.ElementTree as ET

from shapely import wkt, LineString
from typing import List, Optional

from dataclasses import dataclass


@dataclass
class LineStringToKML:
    linestring: LineString
    name: Optional[str]
    description: Optional[str] = None

    def convert_to_xml_placemark(self) -> ET.Element:
        pl = ET.Element("Placemark")
        ls = ET.SubElement(pl, "LineString")
        if self.name:
            name = ET.SubElement(pl, "name")
            name.text = self.name
        if self.description:        
            description = ET.SubElement(pl, "description")
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

    kml_string = ET.tostring(kml_document)

    with open(file_path, 'wb') as kml_file:
        kml_file.write(kml_string)

