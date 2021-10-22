# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator.ontology.package_checklist_normalizer import normalize_package
from sample_annotator.report_model import AnnotationReport
from sample_annotator.ontology.triad_normalizer import normalize_triad_slot

"""Test the module can be imported."""

import unittest


class TestNormalizastions(unittest.TestCase):
    """Tests for normalization of various combinations of EnvO terms and label like strings"""

    def test_env_package_success(self):
        normalized = normalize_package("MIGS/MIMS/MIMARKS.host_associated")
        assert normalized == "host-associated"

    def test_env_package_failure(self):
        normalized = normalize_package("made up package")
        assert normalized is None

    def test_good_id_bad_label_for_triad(self):
        normalized = normalize_triad_slot("ENVO:00001998 pile of dirt")
        assert normalized == "soil [ENVO:00001998]"

    def test_good_id_only_for_triad(self):
        normalized = normalize_triad_slot("ENVO:00001998")
        assert normalized == "soil [ENVO:00001998]"

    def test_good_label_only_for_triad(self):
        normalized = normalize_triad_slot("soil")
        assert normalized == "soil [ENVO:00001998]"

    def test_bad_label_only_for_triad(self):
        normalized = normalize_triad_slot("pile of dirt")
        assert normalized is None

    def test_multi_good_label_only_for_triad(self):
        normalized = normalize_triad_slot("soil|water")
        assert normalized == "soil [ENVO:00001998]|water [CHEBI:15377]"
