# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator, GeoEngine

"""Test the module can be imported."""

import unittest

KEYPATH = os.path.join(INPUT_DIR, 'googlemaps-api-key.txt')

class TestElevation(unittest.TestCase):
    """elevation test."""

    def test_elevation(self):
        ge = GeoEngine()
        ge.load_key(KEYPATH)
        e = ge.get_elevation((40.714224, -73.961452))
        print(e)
