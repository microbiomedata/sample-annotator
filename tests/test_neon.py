# -*- coding: utf-8 -*-
import json
import os
import pprint

import pandas as pd
import requests

from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR, EXAMPLE_DIR, EXAMPLE_OUTDIR
from os.path import isfile, join
import unittest
from sample_annotator.clients.neon.neon_client import NeonClient

TEST_DATA = os.path.join(INPUT_DIR, 'test_sample_info.yaml')
REPORT_OUT = os.path.join(OUTPUT_DIR, 'report.tsv')
SAMPLES_OUT = os.path.join(OUTPUT_DIR, 'samples.yaml')


class TestNeon(unittest.TestCase):
    """annotation test."""

    def test_neon_release_names(self):
        nc = NeonClient()
        rn = set(nc.get_release_names())
        self.assertEqual(rn, {'RELEASE-2021', 'RELEASE-2022', 'RELEASE-2023'})

    def test_neon_product_code_to_name(self):
        nc = NeonClient()
        pctpn_sorted = nc.get_product_code_to_product_name()
        self.assertEqual(pctpn_sorted['DP1.00036.001'], 'Atmospheric CO2 isotopes')

    def test_neon_pctpn_string_match(self):
        nc = NeonClient()
        neon_pctpn_string_match = nc.get_codes_names_by_string("metagenom")
        self.assertEqual(neon_pctpn_string_match['DP1.10107.001'], 'Soil microbe metagenome sequences')
        self.assertNotIn('DP1.00036.001', neon_pctpn_string_match)

    def test_neon_site_summaries(self):
        nc = NeonClient()
        nc.add_site_summary('BART')
        pprint.pprint(nc.site_summaries['BART'])
        # todo: add assertion, remove pprint

    def test_neon_product_summaries(self):
        nc = NeonClient()
        nc.add_product_summary('DP1.10107.001')
        pprint.pprint(nc.product_summaries['DP1.10107.001'])
        # todo: add assertion, remove pprint

    def test_integration_soil(self):
        nc = NeonClient()
        neon_pctpn_string_match = nc.get_codes_names_by_string("metagenom")
        pprint.pprint(neon_pctpn_string_match)
        # {'DP1.10107.001': 'Soil microbe metagenome sequences',
        #  'DP1.20279.001': 'Benthic microbe metagenome sequences',
        #  'DP1.20281.001': 'Surface water microbe metagenome sequences'}
        nc.add_product_summary('DP1.10107.001')
        prod_site_info = nc.product_summaries['DP1.10107.001']['data']['siteCodes']
        bart_soil_metagenome_info = next((i for i in prod_site_info if i["siteCode"] == "BART"), None)

        # NEON.[domain number].[site code].[data product ID].[file-specific name]. [year and month of data].[basic or expanded data package]. [date of file creation]
        # NEON.D01.BART.DP1.10107.001.mms_metagenomeDnaExtraction.2017-06.basic.20230113T224735Z.csv

        # todo: how to efficiently get all of those parameters?
        #  how to find data from other packages from the same site around the same time?

    def test_download_data_package(self):
        url = 'https://data.neonscience.org/api/v0/releases/RELEASE-2023/data/package/DP1.10107.001/BART/2017-06?package=basic'
        # result = requests.get(url=url)
        # todo not implemented yet

    def test_files_by_prod_site_year_month(self):
        url = 'https://data.neonscience.org/api/v0/data/DP1.10107.001/BART/2017-06?package=basic&release=RELEASE-2023'
        result = requests.get(url=url).json()
        print()
        pprint.pprint(result)
        # todo add assertions

    def test_frame_from_filename(self):
        url = 'https://data.neonscience.org/api/v0/data/DP1.10107.001/BART/2017-06/NEON.D01.BART.DP1.10107.001.mms_metagenomeDnaExtraction.2017-06.basic.20230113T224735Z.csv?package=basic&release=RELEASE-2023'
        result_frame = pd.read_csv(url)
        print()
        print(result_frame)
        result_frame.to_csv("test.csv", index=False)

    def test_report_sample_classes(self):
        url = 'https://data.neonscience.org/api/v0/samples/supportedClasses'
        result = requests.get(url=url).json()
        print()
        pprint.pprint(result)

    def test_classes_from_sample_tag(self):
        url = 'https://data.neonscience.org/api/v0/samples/classes?sampleTag=BART_001-O-20170612-COMP'
        result = requests.get(url=url).json()
        print()
        pprint.pprint(result)

    def test_info_from_sample_tag_class(self):
        url = 'https://data.neonscience.org/api/v0/samples/view?sampleTag=BART_001-O-20170612-COMP&sampleClass=sls_metagenomicsPooling_in.compositeSampleID'
        result = requests.get(url=url).json()
        print()
        pprint.pprint(result)


    def test_location_history(self):
        url = 'https://data.neonscience.org/api/v0/locations/BART_003.basePlot.bgc?history=true&hierarchy=false'
        result = requests.get(url=url).json()
        print()
        pprint.pprint(result)


    def test_location_hierarchy(self):
        url = 'https://data.neonscience.org/api/v0/locations/BART_003.basePlot.bgc?history=false&hierarchy=true'
        result = requests.get(url=url).json()
        print()
        pprint.pprint(result)
