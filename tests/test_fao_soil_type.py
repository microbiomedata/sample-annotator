# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator, GeoEngine

"""Test the module can be imported."""

import unittest

class TestSoilType(unittest.TestCase):
    """soil test."""

    def test_soil_type(self):
        ge = GeoEngine()
        # https://www.neonscience.org/field-sites/tall
        testLatLon = (32.95047, -87.393259)
        soil_type = ge.get_fao_soil_type(testLatLon)
        print('Soil type is: ' + soil_type)
        # TODO: need to map CALCIC CAMBISOL => Cambisols
        assert soil_type == 'Cambisols'

if __name__ == '__main__':
    unittest.main()