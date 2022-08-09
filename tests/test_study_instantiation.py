# -*- coding: utf-8 -*-
import os
import pprint
import unittest

import yaml
from linkml_runtime.dumpers import yaml_dumper
from nmdc_schema.annotation import ControlledTermValue
from nmdc_schema.nmdc import (
    Study,
)

from sample_annotator import capitalizer

# MODEL_DIR, INPUT_DIR, OUTPUT_DIR
from tests import INPUT_DIR

"""Test the ability to instantiate a Study."""

"""Run as follows to get see test-time printouts:"""

"""python -m pytest -sv  tests/test_capitalization.py"""

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, "test_sample_info.yaml")


class TestStudyInstantation(unittest.TestCase):
    """biosample instantiation test."""

    def test_study_instantiation(self):

        # ammonium nitrogen -> ammonium_nitrogen
        tidied_dict = {
            "id": "nmdc:fk0vw3h76",
        }

        instantiated = None

        try:
            instantiated = Study(**tidied_dict)
            print("\n")
            print(f"instantiated:")
            print(f"{yaml_dumper.dumps(instantiated)}")
        except (ValueError, TypeError) as e:
            print("\n")
            print(f"Study instantiation error {e}")
            # print(f"tidied_dict:")
            print(f"{pprint.pformat(tidied_dict)}")

        assert type(instantiated) == Study
