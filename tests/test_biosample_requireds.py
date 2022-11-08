# -*- coding: utf-8 -*-
import os
import pprint
import unittest

import yaml
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper
from nmdc_schema.annotation import ControlledTermValue
from nmdc_schema.nmdc import (
    Study,
    Biosample,
)

from sample_annotator import capitalizer

# MODEL_DIR, INPUT_DIR, OUTPUT_DIR
from tests import INPUT_DIR

"""Run as follows to get see test-time printouts:"""

"""python -m pytest -sv  tests/test_capitalization.py"""

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, "test_sample_info.yaml")


class TestBiosampleRequireds(unittest.TestCase):
    """biosample instantiation test."""

    def test_biosample_requireds(self):
        # url = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml"
        # view = SchemaView(url)
        # biosample = view.induced_class("biosample")
        # id
        x = Biosample(
            id="x",
            # part_of="x",
            sample_link="x",
            # fake="x",
            # canary="canary",
            env_broad_scale=ControlledTermValue(),
            env_local_scale=ControlledTermValue(),
            env_medium=ControlledTermValue(),
        )
        # attributes = biosample.attributes
        # for k, v in attributes.items():
        #     if v.required:
        #         print(f"{k}: {v}")
