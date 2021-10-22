# -*- coding: utf-8 -*-

import os
import unittest
import yaml
from sample_annotator.ontology.package_checklist_normalizer import normalize_package
# MODEL_DIR, INPUT_DIR, OUTPUT_DIR
from tests import INPUT_DIR

"""Test the ability to repair biosample env package, checklist and triad slots."""

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, 'test_sample_info.yaml')


# fixed TestNormalizastions typo
class TestNormalizations(unittest.TestCase):
    """Tests for normalization of various combinations of EnvO terms and label like strings"""

    def test_envpack_repair1(self):
        with open(TEST_DATA) as stream:
            test_obj = yaml.load(stream, Loader=yaml.FullLoader)
        for t in test_obj.get('tests'):
            # print(t)
            desc = t.get('description', None)
            # pdb.set_trace()
            if desc == 'package repair test 1':
                current_input = t['sample']['text']
                print(current_input)
                processed_input = normalize_package(current_input)
                # print(processed_input)
                # expected_output = t['output']['text']
                # assert processed_input == expected_output
                # assert False

    # def test_env_package_success(self):
    #     normalized = normalize_package("MIGS/MIMS/MIMARKS.host_associated")
    #     assert normalized == "host-associated"
    #
    # def test_env_package_failure(self):
    #     normalized = normalize_package("made up package")
    #     assert normalized is None
    #
    # def test_good_id_bad_label_for_triad(self):
    #     normalized = normalize_triad_slot("ENVO:00001998 pile of dirt")
    #     assert normalized == "soil [ENVO:00001998]"
    #
    # def test_good_id_only_for_triad(self):
    #     normalized = normalize_triad_slot("ENVO:00001998")
    #     assert normalized == "soil [ENVO:00001998]"
    #
    # def test_good_label_only_for_triad(self):
    #     normalized = normalize_triad_slot("soil")
    #     assert normalized == "soil [ENVO:00001998]"
    #
    # def test_bad_label_only_for_triad(self):
    #     normalized = normalize_triad_slot("pile of dirt")
    #     assert normalized is None
    #
    # def test_multi_good_label_only_for_triad(self):
    #     normalized = normalize_triad_slot("soil|water")
    #     assert normalized == "soil [ENVO:00001998]|water [CHEBI:15377]"
