# -*- coding: utf-8 -*-
import logging
import os
import time

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
                      'Gb0011929',
                      'Gb0051032'  ## sample with no study
                      ]

#logging.basicConfig(level=logging.DEBUG)

class TestGoldClient(unittest.TestCase):
    """elevation test."""

    def test_get_studies(self):
        gc = GoldClient()
        gc.clear_cache()
        if os.path.exists(KEYPATH):
            gc.load_key(KEYPATH)
            samples = gc.fetch_biosamples_by_study(TEST_STUDY_ID)
            print(samples[0])
            print(f'Sample project = {samples[0]["projects"]}')
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
        gc.clear_cache()
        if os.path.exists(KEYPATH):
            gc.load_key(KEYPATH)
            studies = gc.fetch_studies_by_biosample_ids(TEST_BIOSAMPLE_IDS)
            print(len(studies))
            #print(studies[0])
            #print(studies[1])

            assert len(studies) == 3

            print('Doing again: this time should be cached')
            tic = time.perf_counter()
            studies = gc.fetch_studies_by_biosample_ids(TEST_BIOSAMPLE_IDS)
            toc = time.perf_counter()
            elapsed = toc - tic
            print(f'Time taken using cache = {elapsed}')
            assert len(studies) == 3
            assert elapsed < 0.1

            # Gb0011929 is in Gs0014886 which samples and projects that do not align;
            # e.g. Gp0011347 has no biosample
            biosamples = gc.fetch_biosamples_by_study('Gs0014886', include_project=True)
            for biosample in biosamples:
                print(f'Sample: f{biosample["biosampleGoldId"]} // f{biosample.get("projects", None)}')

            # unusual
            UNUSUAL_ID = 'Gb0096893'
            print(f'QUERYING STUDY BY SAMPLE: {UNUSUAL_ID}')
            study = gc.fetch_study_by_biosample_id(UNUSUAL_ID, include_biosamples=True)
            for biosample in study['biosamples']:
                print(f'Sample: f{biosample["biosampleGoldId"]} // f{biosample.get("projects", None)}')

            print(f'QUERYING SAMPLES BY STUDY: Gs0047444')
            biosamples = gc.fetch_biosamples_by_study('Gs0047444', include_project=True)
            for biosample in biosamples:
                print(f'Sample: f{biosample["biosampleGoldId"]} // f{biosample.get("projects", None)}')


        else:
            print(f'Skipping sample tests')
            print(f'To enable these, add your apikey to {KEYPATH}')
