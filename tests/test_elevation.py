# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator, GeoEngine

"""Test the module can be imported."""

import unittest

KEYPATH = os.path.join(INPUT_DIR, 'googlemaps-api-key.txt')

class TestElevation(unittest.TestCase):
    """elevation test."""

    @unittest.skip
    def test_elevation(self):
        ge = GeoEngine()
        if os.path.exists(KEYPATH):
            ge.load_key(KEYPATH)
            mtEverestCoord = (27.9881, 86.9250)
            e = ge.get_elevation(mtEverestCoord)
            print(e)
            # Wikipedia says 8,848.86m
            e0 = e[0]
            assert e0.get('elevation') > 8800
            assert e0.get('elevation') < 8900
        else:
            print(f'Skipping geolocation tests')
            print(f'To enable these, add your apikey to {KEYPATH}')
