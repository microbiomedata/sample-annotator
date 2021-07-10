# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator
from sample_annotator.metadata.sample_schema import SampleSchema

"""Test the module can be imported."""

import unittest


class TestAnnotate(unittest.TestCase):
    """annotation test."""

    def test_annotate(self):
        schema = SampleSchema()
        s = schema.get_slot('oxygen')
        print(s)
        assert s is not None
        s = schema.get_slot('total_particulate_carbon')
        assert s is None
        s = schema.get_slot('total_particulate_carbon', use_aliases=True)
        print(s)
        assert s is not None
        assert s.get('name') == 'tot_part_carb'
        s = schema.get_slot('total particulate carbon', use_aliases=True)
        print(s)
        assert s is not None
        assert s.get('name') == 'tot_part_carb'

