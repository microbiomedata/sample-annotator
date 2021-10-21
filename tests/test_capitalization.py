# -*- coding: utf-8 -*-
import os
import unittest

import yaml

from sample_annotator import capitalizer
# MODEL_DIR, INPUT_DIR, OUTPUT_DIR
from tests import INPUT_DIR

"""Test the ability to capitalize a text slot."""

"""Run as follows to get see test-time printouts:"""

"""python -m pytest -sv  tests/test_capitalization.py"""

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, 'test_sample_info.yaml')


class TestCapitalization(unittest.TestCase):
    """capitalization test."""

    def test_capitalization(self):
        with open(TEST_DATA) as stream:
            test_obj = yaml.load(stream, Loader=yaml.FullLoader)
        for t in test_obj.get('tests'):
            desc = t.get('description', None)
            # pdb.set_trace()
            if desc == 'capitalization test':
                current_input = t['sample']['text']
                processed_input = capitalizer.capitalizer(current_input)
                expected_output = t['output']['text']
                assert processed_input == expected_output
