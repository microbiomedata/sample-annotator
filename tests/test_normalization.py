# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator.ontology.package_checklist_normalizer import normalize_package
from sample_annotator.report_model import AnnotationReport

"""Test the module can be imported."""

import unittest


class TestMeasurements(unittest.TestCase):
    """annotation test."""

    def test_success(self):
        normalized = normalize_package("MIGS/MIMS/MIMARKS.host-associated")
        # print(normalized)
        assert normalized == "host-associated"

    def test_failure(self):
        normalized = normalize_package("made up package")
        assert normalized == ""
