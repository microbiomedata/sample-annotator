# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator, GeoEngine

"""Test the module can be imported."""

import unittest

KEYPATH = os.path.join(INPUT_DIR, 'googlemaps-api-key.txt')

class TestElevation(unittest.TestCase):
    """elevation test."""

    #@unittest.skip
    def test_elevation(self):
        ge = GeoEngine()
        mtEverestCoord = (27.9881, 86.9250)
        e = ge.get_elevation(mtEverestCoord)
        print(e)
            # The GTOP30 elevation for Mount Everest is 8752
        assert int(e) == 8752
        
        