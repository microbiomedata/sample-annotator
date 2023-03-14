# -*- coding: utf-8 -*-
import filecmp
import os
import tempfile

from tests import INPUT_DIR, OUTPUT_DIR

from sample_annotator.clients.gold_nmdc_pipeline import GoldNMDC

import unittest

KEYPATH = os.path.join(INPUT_DIR, "gold-key.txt")

OUTPUT_NMDC_JSON = os.path.join(OUTPUT_DIR, "gold_nmdc.json")

# GOLD study id for EMP500 study
TEST_STUDY_ID = "gold:Gs0154244"


class TestGoldNMDC(unittest.TestCase):
    """Test the NMDC wrapper for the Gold client."""

    def test_gold_nmdc_client(self):
        gc = GoldNMDC(study_id=TEST_STUDY_ID)

        _, file_path = tempfile.mkstemp()

        # JSON file path
        file_path = file_path + ".json"

        gc.clear_cache()

        if os.path.exists(KEYPATH):
            gc.load_key(KEYPATH)

            # unit test for transform_emp500_nmdc()
            _ = gc.transform_gold_nmdc(file_path=file_path)

            # assert that True is always returned when we compare
            # two JSON files
            self.assertTrue(filecmp.cmp(file_path, OUTPUT_NMDC_JSON))

            # assert that tempfile validates against NMDC schema
            self.assertTrue(gc.validate_nmdc(file_path=file_path))