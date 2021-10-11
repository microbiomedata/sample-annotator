import googlemaps
from datetime import datetime
from typing import Optional, List, Set, Any, Tuple
from dataclasses import dataclass
import logging
import requests

LATLON = Tuple[float, float]

@dataclass
class GeoEngine():
    """
    This can wrap any number of external services

    Currently it only implements fetching of elevation using the googlemaps API
    (API KEY required)

    In future this will wrap ORNL Identify
    """
    googlemaps_api_key: str = None
    client = None
    
    def load_key(self, path: str) -> None:
        with open(path) as stream:
            lines = stream.readlines()
            key = lines[0].strip()
            #print(f'REMOVE THIS: {key}')
            self.googlemaps_api_key = key

    def get_client(self):
        if self.client is None:
            self.client = googlemaps.Client(key=self.googlemaps_api_key)
        return self.client

    def get_elevation(self, latlon: LATLON = (40.714224, -73.961452)):
        results = self.get_client().elevation(latlon)
        
        return results

    
    def get_fao_soil_type(self, latlon: LATLON) -> str:
        #Need to put the import statments elsewhere - Bill?
        import xml.etree.ElementTree as ET
        import csv
        #Routine to calculate the locations from lat/long
        lat = latlon[0]
        lon = latlon[1]
        remX = (lon + 180)%0.5
        remY = (lat + 90)%0.5
        minX = lon-remX
        maxX = lon - remX + 0.5
        minY = lat - remY
        maxY = lat - remY + 0.5
        #Read in the mapping file note need to get this path right
        with open('./sample_annotator/geolocation/zobler_540_MixS_lookup.csv') as mapper:
            mapping = csv.reader(mapper)
            map = list(mapping)
      

        faosoilprefixurl = 'https://webmap.ornl.gov/cgi-bin/mapserv?&INFO_FORMAT=text/xml&WIDTH=5&originator=QAQCIdentify&HEIGHT=5&LAYERS=540_1_band1&REQUEST=GetFeatureInfo&SRS=EPSG:4326&BBOX='
        faosoilmidurl = faosoilprefixurl + str(minX) + ',' + str(minY) +',' + str(maxX) + ',' + str(maxY) 
        faosoilfinalurl = faosoilmidurl + '&VERSION=1.1.1&X=2&Y=2&SERVICE=WMS&QUERY_LAYERS=540_1_band1&map=/sdat/config/mapfile//540/540_1_wms.map'
        response = requests.get(faosoilfinalurl)
        if response.status_code == 200:
            faosoilxml = response.content.decode('utf-8')
            root = ET.fromstring(faosoilxml)
            results =(root[5].text)
            results = results.split(':')
            results = results[1].strip()
            for res in map:
                if res[0] == results:
                    results = res[1]
                    return results
        
        