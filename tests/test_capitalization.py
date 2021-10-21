# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR
import yaml
import pdb

from sample_annotator import SampleAnnotator, GeoEngine

"""Test the ability to capitalize a text slot."""

import unittest

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, 'test_sample_info.yaml')

class TestCapitalization(unittest.TestCase):
    """capitalization test."""

    def test_capitalization(self):
        with open(TEST_DATA) as stream:
            test_obj = yaml.load(stream, Loader=yaml.FullLoader)
        for t in test_obj.get('tests'):
            print(t)
            source = None
            desc = t.get('description', None)
            print(desc)
            pdb.set_trace()
            if desc == 'capitalization test':
                print(t)

