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
        mtEverestCoord = (27.9881, 86.9250)
        soil_type = ge.get_fao_soil_type(mtEverestCoord)
        print(soil_type)
        assert soil_type == 'Cambisols'
