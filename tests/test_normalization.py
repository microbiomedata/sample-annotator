# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator.ontology.package_checklist_normalizer import normalize_package
from sample_annotator.report_model import AnnotationReport
from sample_annotator.ontology.triad_normalizer import normalize_triad_slot

"""Test the module can be imported."""

import unittest


class TestNormalizastions(unittest.TestCase):
    """annotation test."""

    def test_env_package_success(self):
        normalized = normalize_package("MIGS/MIMS/MIMARKS.host-associated")
        # print(normalized)
        assert normalized == "host-associated"

    def test_env_package_failure(self):
        normalized = normalize_package("made up package")
        assert normalized == ""

    def test_ebs_failure(self):
        normalized = normalize_triad_slot("ENVO:00001998 pile of dirt")
        assert normalized == "soil [ENVO:00001998]"
