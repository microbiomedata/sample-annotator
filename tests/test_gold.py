# -*- coding: utf-8 -*-
import logging
import os
import yaml
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator
from sample_annotator.clients.gold_client import GoldClient

"""Test the module can be imported."""

import unittest

KEYPATH = os.path.join(INPUT_DIR, 'gold-key.txt')
STUDY_LIST_PATH = os.path.join(INPUT_DIR, 'gold-studies-subset.tsv')

TEST_STUDY_ID = 'Gs0144570' ## NEON
TEST_BIOSAMPLE_IDS = ['Gb0255525', 'Gb0255899', 'Gb0255966',  ## in Gs0144570
                      'Gb0011929']

logging.basicConfig(level=logging.DEBUG)

class TestGoldClient(unittest.TestCase):
    """elevation test."""

    def test_get_studies(self):
        gc = GoldClient()
        if os.path.exists(KEYPATH):
            gc.load_key(KEYPATH)
            samples = gc.fetch_biosamples_by_study(TEST_STUDY_ID)
            print(samples[0])
            study = gc.fetch_study(TEST_STUDY_ID, include_biosamples=True)
            assert study['studyGoldId'] == TEST_STUDY_ID
            assert len(study['biosamples']) > 100
            studies = gc.fetch_studies_from_file(STUDY_LIST_PATH)
            assert len(studies) > 0
            print(studies[0])
        else:
            print(f'Skipping study tests')
            print(f'To enable these, add your apikey to {KEYPATH}')

    def test_get_biosamples(self):
        gc = GoldClient()
        if os.path.exists(KEYPATH):
            gc.load_key(KEYPATH)
            studies = gc.fetch_studies_by_biosample_ids(TEST_BIOSAMPLE_IDS)
            print(len(studies))
            print(studies[0])
            print(studies[1])

            assert len(studies) == 2
        else:
            print(f'Skipping sample tests')
            print(f'To enable these, add your apikey to {KEYPATH}')
