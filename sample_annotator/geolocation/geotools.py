import googlemaps
from datetime import datetime
from typing import Optional, List, Set, Any
from dataclasses import dataclass
import logging

@dataclass
class GeoEngine():
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

    def get_elevation(self, latlong = (40.714224, -73.961452)):
        results = self.get_client().elevation(latlong)
        return results