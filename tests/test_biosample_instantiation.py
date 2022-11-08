# -*- coding: utf-8 -*-
import os
import pprint
import unittest

import yaml
from linkml_runtime.dumpers import yaml_dumper
from nmdc_schema.annotation import ControlledTermValue
from nmdc_schema.nmdc import (
    Biosample,
    QuantityValue,
    GeolocationValue,
    TextValue,
    TimestampValue,
    OntologyClass,
)

from sample_annotator import capitalizer

# MODEL_DIR, INPUT_DIR, OUTPUT_DIR
from tests import INPUT_DIR

"""Test the ability to instantiate a Biosample."""

"""Run as follows to get see test-time printouts:"""

"""python -m pytest -sv  tests/test_capitalization.py"""

# INPUT_DIR comes from __init__.py
PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, "test_sample_info.yaml")


class TestBiosampleInstantation(unittest.TestCase):
    """biosample instantiation test."""

    def test_biosample_instantiation(self):

        # ammonium nitrogen -> ammonium_nitrogen
        tidied_dict = {
            "ammonium_nitrogen": QuantityValue(
                has_raw_value="10.599 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=10.599,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "analysis_type": ["natural organic matter"],
            "calcium": QuantityValue(
                has_raw_value="2219.35 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=2219.35,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            # "collection_date": "2020-09-07",
            "depth": QuantityValue(
                has_raw_value="0.1",
                was_generated_by=None,
                type=None,
                has_unit="dimensionless",
                has_numeric_value=0.1,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "ecosystem": "Environmental",
            "ecosystem_category": "Terrestrial",
            "ecosystem_subtype": "Bulk soil",
            "ecosystem_type": "Soil",
            "elev": QuantityValue(
                has_raw_value="62 meter",
                was_generated_by=None,
                type=None,
                has_unit="metre",
                has_numeric_value=62.0,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "env_broad_scale": ControlledTermValue(
                has_raw_value="anthropogenic terrestrial biome [ENVO:01000219]",
                was_generated_by=None,
                type=None,
                term=OntologyClass(
                    id="ENVO:01000219",
                    name="anthropogenic terrestrial biome",
                    description=None,
                    alternative_identifiers=[],
                ),
            ),
            "env_local_scale": ControlledTermValue(
                has_raw_value="agricultural field [ENVO:00000114]",
                was_generated_by=None,
                type=None,
                term=OntologyClass(
                    id="ENVO:00000114",
                    name="agricultural field",
                    description=None,
                    alternative_identifiers=[],
                ),
            ),
            "env_medium": ControlledTermValue(
                has_raw_value="agricultural soil [ENVO:00002259]",
                was_generated_by=None,
                type=None,
                term=OntologyClass(
                    id="ENVO:00002259",
                    name="agricultural soil",
                    description=None,
                    alternative_identifiers=[],
                ),
            ),
            "env_package": TextValue(
                has_raw_value="soil", was_generated_by=None, type=None, language=None
            ),
            "geo_loc_name": TextValue(
                has_raw_value="USA: Oregon, Clatskanie",
                was_generated_by=None,
                type=None,
                language=None,
            ),
            "growth_facil": ControlledTermValue(
                has_raw_value="agricultural field [ENVO:00000114]",
                was_generated_by=None,
                type=None,
                term=OntologyClass(
                    id="ENVO:00000114",
                    name="agricultural field",
                    description=None,
                    alternative_identifiers=[],
                ),
            ),
            "id": "nmdc:fk0vw3h76",
            "lat_lon": GeolocationValue(
                has_raw_value="46.121307 -123.269236",
                was_generated_by=None,
                type=None,
                latitude=46.121307,
                longitude=-123.269236,
            ),
            "lbc_thirty": QuantityValue(
                has_raw_value="1252 ppm",
                was_generated_by=None,
                type=None,
                has_unit="parts-per-million",
                has_numeric_value=1252.0,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "lbceq": QuantityValue(
                has_raw_value="3630.8 ppm",
                was_generated_by=None,
                type=None,
                has_unit="parts-per-million",
                has_numeric_value=3630.8,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "magnesium": QuantityValue(
                has_raw_value="379.527 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=379.527,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "manganese": QuantityValue(
                has_raw_value="34.6609 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=34.6609,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            # nitrate nitrogen -> nitrate_nitrogen
            "nitrate_nitrogen": QuantityValue(
                has_raw_value="27.6905 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=27.6905,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            # nitrite nitrogen -> nitrite_nitrogen
            "nitrite_nitrogen": QuantityValue(
                has_raw_value="0 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=0.0,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "part_of": "bioscales",
            "ph": QuantityValue(
                has_raw_value="5.07",
                was_generated_by=None,
                type=None,
                has_unit="dimensionless",
                has_numeric_value=5.07,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "potassium": QuantityValue(
                has_raw_value="441.338 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=441.338,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            # "samp_name": "BESC-133-CL1_28_16_76",
            "samp_store_temp": QuantityValue(
                has_raw_value="4 C",
                was_generated_by=None,
                type=None,
                has_unit="coulomb",
                has_numeric_value=4.0,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
            "source_mat_id": TextValue(
                has_raw_value="Bio-Scales:3f8322b3-26cc-46eb-9078-238a12388147",
                was_generated_by=None,
                type=None,
                language=None,
            ),
            "specific_ecosystem": "Agricultural soil",
            "store_cond": TextValue(
                has_raw_value="fresh", was_generated_by=None, type=None, language=None
            ),
            # "tot_nitro": QuantityValue(
            #     has_raw_value="0.419 Percent",
            #     was_generated_by=None,
            #     type=None,
            #     has_unit="percentage",
            #     has_numeric_value=0.419,
            #     has_minimum_numeric_value=None,
            #     has_maximum_numeric_value=None,
            # ),
            "zinc": QuantityValue(
                has_raw_value="5.5536 mg/kg",
                was_generated_by=None,
                type=None,
                has_unit="milligram per kilogram",
                has_numeric_value=5.5536,
                has_minimum_numeric_value=None,
                has_maximum_numeric_value=None,
            ),
        }

        instantiated = None

        try:
            instantiated = Biosample(**tidied_dict)
            print("\n")
            print(f"instantiated:")
            print(f"{yaml_dumper.dumps(instantiated)}")
        except (ValueError, TypeError) as e:
            print("\n")
            print(f"Biosample instantiation error {e}")
            # print(f"tidied_dict:")
            # print(f"{pprint.pformat(tidied_dict)}")

        assert type(instantiated) == Biosample
