# SETUP

# get an ORCID if you don't have one: https://orcid.org/register
# log into https://data.microbiomedata.org or https://data.dev.microbiomedata.org with your ORCID
# get the NMDC session cookie value
#   firefox: open Firefox DevTools. On a Mac, you can use the command-option-c keystroke
#     click the storage tab at the top of the DevTools pane
#     click the "Cookies" button on the left
#     there should be an entry for https://data.microbiomedata.org or https://data.dev.microbiomedata.org, with a globe icon to the left
#     click the globe icon
#     find the session row
#     double click in the value column
#     copy the value with command-c
#     enter the session cookie value into the session_cookie row in local/.env
#       what are the other env variables for ?

# todo are we properly handling multivalued, non-enum slots?

# todo tests, more typing, docstrings, etc.

# todo: /Users/MAM/Library/Caches/pypoetry/virtualenvs/sample-annotator-G4hsqM_G-py3.9/lib/python3.9/site-packages/requests/__init__.py:109: RequestsDependencyWarning: urllib3 (1.26.9) or chardet (5.0.0)/charset_normalizer (2.0.12) doesn't match a supported version!
#   warnings.warn(
#  requests                      2.28.0

# todo switch from CSV to TSV or infer seperator

# todo return None in all fallback cases

# todo add ability to iterate over the dev and prod endpoints
#   how to assert which endpoint a study or biosample "came" from?

import csv
import logging
import os
import pprint
import re
import sqlite3
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any

import click
import click_log
import pendulum
import requests
import yaml
from dotenv import load_dotenv
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper
from nmdc_schema.nmdc import (
    Biosample,
    QuantityValue,
    GeolocationValue,
    OntologyClass,
    TextValue,
    ControlledTermValue,
    TimestampValue,
    Database,
    Study,
)
from quantulum3 import parser

import sample_annotator.clients.nmdc.nmdc_runtime_snippets as nrs

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

# todo dangerous to hardcode repairs like this

# #sample_type:
# #has_raw_value:soil
# technical_reps:'1'
# analysis_type:
# -naturalorganicmatter
# #samp_collec_device:russiancorer

# todo use SSSOM files for these overrides?

drop_cols_pending_research = [
    "horizon",
    "soil horizon",
    "soil_horizon",
    "prev_land_use_meth",
    "samp_collec_device",
    "samp_type",
    "sample_type",
]

biosample_sqlite_to_dh_field_name = {
    "title": {"dh": "samp_name", "format": "scalar"},
    "id": {"dh": "INSDC biosample identifiers", "format": "list"},
    # # todo the schema is giving mixed messages about whether INSDC secondary sample identifiers is multivalued or not
    # "sra_id": {"dh": "INSDC secondary sample identifiers", "format": "list"},
}

dh_field_name_overrides = {
    "samp_name": "name",
    # "samp_type": "sample_type",
    "ammonium nitrogen": "ammonium_nitrogen",
    "nitrate nitrogen": "nitrate_nitrogen",
    "nitrite nitrogen": "nitrite_nitrogen",
    "tot_nitro": "tot_nitro_content",
    "soil_horizon": "horizon",
}

unit_overrides = {
    "coulomb": "degree Celsius",
    "degree angle coulomb": "degree Celsius",
}


def load_vars_from_env_file(env_file) -> None:
    load_dotenv(env_file)


def get_session_cookie() -> str:
    return os.getenv("session_cookie")


def get_count_from_response(api_response):
    return api_response["count"]


def get_results_from_response(api_response):
    return api_response["results"]


def get_metadata_submission_from_result(result):
    if "metadata_submission" in result:
        return result["metadata_submission"]


def get_sample_data_from_metadata_submission(metadata_submission):
    if "sampleData" in metadata_submission:
        return metadata_submission["sampleData"]


def get_view(schema_url):
    logger.info(f"creating a view of {schema_url}")
    try:
        view = SchemaView(schema_url)
        logger.info(f"confirming load of schema '{view.schema.name}'")
        return view
    except Exception as e:
        logger.critical(f"failed to load schema: {e}")
        exit()


def process_qv(raw_value: str):
    # todo what if more than one qv gets parsed out?
    # todo units are expressed as words. convert to symbols?
    # todo units my be surprising, like Coulombs for degrees Celsius
    qv = QuantityValue(has_raw_value=raw_value)
    quants = parser.parse(raw_value)
    if len(quants) > 0:
        if len(quants) > 1:
            logger.error(f"quantulum3 found more than one quantity in: {quants}")
        quant = quants[0]
        if quant.uncertainty:
            qv.has_minimum_numeric_value = quant.value - quant.uncertainty
            qv.has_maximum_numeric_value = quant.value + quant.uncertainty
        else:
            qv.has_numeric_value = quant.value
        # todo unit is required ?
        if quant.unit and quant.unit.name != "dimensionless":
            if quant.unit.name in unit_overrides:
                qv.has_unit = unit_overrides[quant.unit.name]
            else:
                qv.has_unit = quant.unit.name
    else:
        logger.error(f"quantulum3 couldn't parse any quantities out of {raw_value}")
        # todo this will just return the has_raw_value portion of the QV
        #   better than nothing?
    return qv


def extract_lat_lon(raw_value: str):
    # if DH validation worked,
    # should be one decimal value, then a single whitespace, then another decimal value
    gv = GeolocationValue(has_raw_value=raw_value)
    if raw_value:
        logger.debug(f"extracting lat/lon from {raw_value}")
        strip_ed = raw_value.strip()
        logger.debug(f"{raw_value} stripped as {strip_ed}")
        pattern = r"\s+"
        split_ed = re.split(pattern, strip_ed)
        logger.debug(f"split {strip_ed} into {split_ed}")
        decimals = []
        for i in split_ed:
            try:
                decimals.append(float(i))
                logger.debug(f"successful parse of {i} as a geolocation float")
            except ValueError:
                logger.error(f"failed to parse {i} as a geolocation float")

        if len(decimals) != 2:
            logger.error(
                f"there should be two and exactly two space-separated chunks in {decimals}"
            )
            return None
        else:
            latv = decimals[0]
            longv = decimals[1]
            if (
                len(decimals) == 2
                and latv > -90
                and latv < 90
                and longv > -180
                and longv < 180
            ):
                gv.latitude = latv
                gv.longitude = longv
            else:
                logger.error(f"invalid lat/lon: {raw_value}")
    logger.debug(gv)
    return gv


def extract_ctv(raw_value: str, strip_initial_underscores=True):
    # ____mediterranean shrubland biome [ENVO:01000217]
    # todo could be more than one term id
    #  pipe or semicolon separated?
    #  or just one big mess?
    #  in any case, could be label only, id only, matching label and id, mismatch...
    #  check ontology owner to see if term is still active and label/id match?
    ctv = ControlledTermValue(has_raw_value=raw_value)
    if raw_value:
        if raw_value != "":
            if strip_initial_underscores:
                possibly_underscoreless = re.sub(
                    pattern=r"^_*\s*", repl="", string=raw_value
                )
                logger.debug(
                    f"would strip underscores in {raw_value} and process {possibly_underscoreless} as a controlled term value"
                )
            else:
                possibly_underscoreless = raw_value
                logger.debug(
                    f"would process {raw_value} as a controlled term value, keeping any initial underscores"
                )
            p = re.compile(r"\[(.*)]")
            term_id = p.findall(possibly_underscoreless)
            if len(term_id) == 0:
                logger.error(f"no [term id] found in {possibly_underscoreless}")
            elif len(term_id) == 1:
                logger.debug(f"term id {term_id[0]} found in {possibly_underscoreless}")
                label = possibly_underscoreless.replace(f"[{term_id[0]}]", "")
                label = label.strip()
                if label:
                    logger.debug(f"label {label} found in {possibly_underscoreless}")
                    # todo what is required for an OntologyClass?
                    oc = OntologyClass(id=term_id[0], name=label)
                    ctv["term"] = oc
                    logger.debug(yaml_dumper.dumps(ctv))
                else:
                    logger.error(f"no label found in {possibly_underscoreless}")
            else:
                logger.error(
                    f"multiple [term id]s {term_id} found in {possibly_underscoreless}"
                )
        else:
            logger.error(f"blank controlled term value")
    else:
        logger.error(f"None controlled term value")
    logger.debug(ctv)
    return ctv


# def set_to_list(set_input, do_sort=True):
#     temp = list(set_input)
#     if do_sort:
#         temp.sort()
#     return temp


# def is_number(s):
#     try:
#         float(s)
#         return True
#     except ValueError:
#         return False


@dataclass
class SubmissionsSandbox:
    url_suffix = "api/metadata_submission"

    study_metadata: Optional[Dict[str, Any]] = None
    sample_metadata_by_title: Optional[Dict[str, List[List[str]]]] = None
    sample_metadata_by_slotname: Optional[Dict[str, List[List[str]]]] = None
    updatable_monikers: Optional[List[str]] = None

    api_url: Optional[str] = None
    session_cookie: Optional[str] = None

    # todo switch to w3id URLs
    nmdc_url: Optional[
        str
    ] = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml"
    mixs_url: Optional[
        str
    ] = "https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/model/schema/mixs.yaml"
    submission_schema_url: Optional[
        str
    ] = "https://raw.githubusercontent.com/microbiomedata/sheets_and_friends/main/artifacts/nmdc_submission_schema.yaml"

    mixs_view: Optional[SchemaView] = None
    nmdc_view: Optional[SchemaView] = None
    submission_schema_view: Optional[SchemaView] = None

    submission_slot_alias_to_x: Optional[Dict[str, Dict[str, Any]]] = None
    submission_slot_key_to_x: Optional[Dict[str, Dict[str, Any]]] = None
    submission_slot_title_to_x: Optional[Dict[str, Dict[str, Any]]] = None

    minting_client: Optional[nrs.RuntimeApiSiteClient] = None

    env_pack_title_to_key = {}

    biosample_database: Optional[Database] = Database()
    study_database: Optional[Database] = Database()

    def view_setup(self):
        # self.mixs_view = get_view(self.mixs_url)

        self.nmdc_view = get_view(self.nmdc_url)

        self.submission_schema_view = get_view(self.submission_schema_url)

    def get_submission_titles_and_names(self):
        submission_slots = self.submission_schema_view.schema.slots
        self.submission_slot_title_to_x = {
            v.title: {"title": v.title, "key": k, "alias": v.alias}
            for k, v in submission_slots.items()
            if v.title
        }
        self.submission_slot_key_to_x = {
            k: {"title": v.title, "key": k, "alias": v.alias}
            for k, v in submission_slots.items()
        }
        self.submission_slot_alias_to_x = {
            v.alias: {"title": v.title, "key": k, "alias": v.alias}
            for k, v in submission_slots.items()
            if v.alias
        }

    def get_one_submission_page_from_api(self, start, stop):
        logger.debug(f"session_cookie: {self.session_cookie}")
        logger.info(f"api_url: {self.api_url}")
        metadata_api_url = f"{self.api_url}{self.url_suffix}"
        logger.debug(f"whole_url: {metadata_api_url}")
        params = {"offset": start, "limit": stop}
        cookies = {"session": self.session_cookie}

        submission_response = requests.get(
            metadata_api_url, cookies=cookies, params=params
        )

        submission_json = submission_response.json()
        submission_count = get_count_from_response(submission_json)
        logger.info(f"submission_count: {submission_count}")

        results = get_results_from_response(submission_json)
        results_pretty = pprint.pformat(results)
        logger.debug(f"results_pretty: {results_pretty}")

        (
            self.study_metadata,
            self.sample_metadata_by_title,
        ) = self.split_study_and_sample_metadata(results)

    def make_sample_data_dict(
        self, sample_data: List[List[str]], result_id: str
    ) -> List[Dict[str, str]]:

        logger.debug(f"study_metadata: {self.study_metadata}")
        # todo may need to add or mint study id
        #   reuse the UUID that Kitware assigns to id,
        #   or some study identifier from the metadata_submission.multiOmicsForm or metadata_submission.studyForm paths?
        #   NMDC sample to study relationship expressed with sample_link?
        sample_data_row_count = len(sample_data)
        if result_id:
            if sample_data_row_count > 2:
                # todo make this test more flexible
                provided = set(sample_data[1])
                expected = set(self.submission_slot_title_to_x.keys())
                intersection = provided.intersection(expected)
                purity = len(intersection) / len(provided)
                # purity will almost always be a little lower than 1
                #   due to re-titling of slots in environmental package slot usages
                # but won't we have to resolve that eventually?
                if purity < 0.9:  # todo make this a parameter
                    logger.error(
                        f"study {result_id} doesn't seem to have the expected header rows: {sample_data[0:3]}"
                    )
                    # todo try to infer columns from template? what about versioning?
                    return [{}]
                else:
                    sample_data_headers = sample_data[1]
                    sample_data_body = sample_data[2:sample_data_row_count]
                    # todo the sample_data_headers values are term titles, not names
                    #  lookup with the latest version for now,
                    #  but work on better recording and reporting of schema version
                    body_list = []
                    for body_row in sample_data_body:
                        row_dict = dict(zip(sample_data_headers, body_row))
                        row_dict["sample_link"] = result_id
                        # row_dict['canary'] = 'canary'
                        body_list.append(row_dict)

                    return body_list
            else:
                logger.error(
                    f"study {result_id} has less than three sample data rows (including the expected two headers)"
                )
                return [{}]
        else:
            logger.error(f"Empty result_id")
            return [{}]

    def del_sample_data_from_result(self, result):
        if "metadata_submission" in result:
            metadata_submission = result["metadata_submission"]
            if "sampleData" in metadata_submission:
                for_counting = metadata_submission["sampleData"]
                del metadata_submission["sampleData"]
                if len(for_counting) > 2:
                    result["sample_data_total_rows"] = len(for_counting)
                    result["metadata_submission"] = metadata_submission
                    return result
                else:
                    result["sample_data_total_rows"] = 0
                    return result
        else:
            logger.error(f"no metadata_submission for {result['id']}")
            result["sample_data_total_rows"] = 0
            return result

    def split_study_and_sample_metadata(self, results):
        study_metadata = {}
        sample_metadata_by_title = {}
        for result in results:
            result_id = result["id"]
            logger.info(f"{result_id} started")
            logger.debug(f"result_id: {result_id}")
            if "metadata_submission" in result:
                metadata_submission = result["metadata_submission"]
                if "sampleData" in metadata_submission:
                    sample_data_lol = metadata_submission["sampleData"]
                    sample_data_lod = self.make_sample_data_dict(
                        sample_data_lol, result_id
                    )
                    sample_metadata_by_title[result_id] = sample_data_lod
            without_sample_data = self.del_sample_data_from_result(result)
            if without_sample_data:
                study_metadata[result_id] = without_sample_data
        return study_metadata, sample_metadata_by_title

    def update_monikers(
        self, acceptable_statuses, suggested_initial_columns, source="raw"
    ):
        all_headers = set()
        logger.debug(f"updating monikers from {source} sample metadata")
        if source == "raw":
            source_dict = self.sample_metadata_by_title
        elif source == "tidied":
            source_dict = self.sample_metadata_by_slotname
        # todo AttributeError: 'NoneType' object has no attribute 'items' IF there weren't any acceptable studies
        for k, v in source_dict.items():
            current_status = None
            if k in self.study_metadata:
                current_status = self.study_metadata[k]["status"]
            if current_status in acceptable_statuses:
                lod = v
                for row in lod:
                    if row:
                        current_headers = list(row.keys())
                        current_headers = [x for x in current_headers if x]
                        if current_headers:
                            all_headers.update(current_headers)
            else:
                logger.info(f"{k} has status value: {current_status}")

        all_headers = list(all_headers)
        if source == "tidied":
            all_headers.sort()
            remainder_headers = set(all_headers) - set(suggested_initial_columns)
            remainder_headers = list(remainder_headers)
            remainder_headers.sort()
            self.updatable_monikers = (
                list(suggested_initial_columns) + remainder_headers
            )
        else:
            self.updatable_monikers = all_headers
        logger.debug(f"updatable_monikers: {self.updatable_monikers}")

    def get_ids_list(self, list_size):
        # todo parameterize this
        minting_params = {
            "populator": "",
            "naa": "nmdc",
            "shoulder": "fk0",
            "number": list_size,
        }
        minting_response = self.minting_client.request(
            "POST", "/ids/mint", params_or_json_data=minting_params
        )
        id_list = minting_response.json()
        return id_list

    def load_env_pack_title_to_key(self, current_template):
        env_pack_class = self.submission_schema_view.induced_class(current_template)
        env_pack_slot_usage = env_pack_class["slot_usage"]
        current_slot_dict = {}
        for ik, iv in env_pack_slot_usage.items():
            if iv["title"]:
                current_slot_dict[iv["title"]] = ik
            else:
                current_slot_dict[ik] = ik
        schema_slots = self.submission_schema_view.all_slots()
        for ik, iv in schema_slots.items():
            if iv["title"]:
                current_slot_dict[iv["title"]] = ik
            else:
                current_slot_dict[ik] = ik
        # todo pull this out to the top
        current_slot_dict["sample_link"] = "sample_link"
        # current_slot_dict['canary'] = 'canary'
        #
        logger.debug(
            f"current_slot_dict for {current_template}: {pprint.pformat(current_slot_dict)}"
        )
        self.env_pack_title_to_key[current_template] = current_slot_dict

    def tidy_flat_sample_metadata(self, acceptable_statuses):
        """
        Assigns ids from the nmdc-runtime's id minter
        Rey-keys sample metadata fields by slot name, not slot title
        Does not process sample metadata lacking corresponding study metadata
        Only processes sample metadata whose study status is in acceptable_statuses
        Puts the resulting dict of dicts, whose leaves are still flat stings, into sample_metadata_by_slotname
        """
        # todo do some logging with all of these pass statements
        # todo refactor
        tidied_dict = {}
        for k, raw_lod in self.sample_metadata_by_title.items():
            biosample_count = len(raw_lod)

            id_list = self.get_ids_list(biosample_count)

            tidied_lod = []
            logger.debug(f"k: {k}")
            current_status = None
            if k in self.study_metadata:
                current_creation_date = self.study_metadata[k]["created"]
                current_status = self.study_metadata[k]["status"]
                current_template = self.study_metadata[k]["metadata_submission"][
                    "template"
                ]
                logger.info(f"{k} was created on: {current_creation_date}")
                logger.info(f"{k} has status: {current_status}")
                logger.info(f"{k} uses environmental package: {current_template}")
                logger.info(f"{k} comes from server: {self.api_url}")

                if (
                    not self.env_pack_title_to_key
                    or current_template not in self.env_pack_title_to_key
                ):
                    self.load_env_pack_title_to_key(current_template)
            else:
                logger.error(
                    f"{k} can't determine status, because no study metadata available"
                )
            if current_status in acceptable_statuses:
                for row in raw_lod:
                    if row:
                        logger.debug(f"row: {row}")
                        filtered_row = {}
                        for fk, fv in row.items():
                            if fk in self.updatable_monikers:
                                fks_key = self.env_pack_title_to_key[current_template][
                                    fk
                                ]
                                filtered_row[fks_key] = fv
                        filtered_row["id"] = id_list.pop()
                        logger.debug(f"filtered_row: {filtered_row}")
                        tidied_lod.append(filtered_row)
                    else:
                        pass
            else:
                pass
            if tidied_lod:
                tidied_dict[k] = tidied_lod
            else:
                pass
            logger.debug(f"tidied_dict: {tidied_dict}")
        if tidied_dict:
            self.sample_metadata_by_slotname = tidied_dict
        else:
            pass

    def parse_any_range(self, sample_value, ssd_range, range_type):
        # todo branch on the types, not the type's pre-extracted class names
        if range_type == "type_definition":
            # todo i think sample_link is handled here, since it has the default range of string
            #   change to range named thing?
            return sample_value

        elif range_type == "enum_definition":
            # todo DH entries may be ; separated lists
            #   sure hope we don't have any PVs with ; in them
            #   should check
            logger.debug(f"{ssd_range} raw value: {sample_value}")
            raw_strip_ed = sample_value.strip()
            individual_values = raw_strip_ed.split(";")
            logger.debug(f"{ssd_range} spit value: {individual_values}")
            # todo trim?

            current_enum = self.nmdc_view.get_enum(ssd_range)
            current_pvs = current_enum.permissible_values
            current_pv_texts = [v.text for k, v in current_pvs.items()]
            passing_individuals = []
            for individual in individual_values:
                ind_strip_ed = individual.strip()
                logger.debug(
                    f"need to check {ind_strip_ed} against {ssd_range} with PVs {current_pv_texts}"
                )
                if ind_strip_ed not in current_pv_texts:
                    # todo flag the study and sample this came from
                    logger.error(
                        f"{ind_strip_ed} is not in {current_pv_texts}, so it's an invalid {ssd_range}"
                    )
                else:
                    passing_individuals.append(ind_strip_ed)
                    logger.debug(
                        f"{ind_strip_ed} is in {current_pv_texts}, so it's a valid {ssd_range}"
                    )
            logger.debug(f"{ssd_range} passing values: {passing_individuals}")
            return passing_individuals

        elif ssd_range == "text value":

            tv_obj = TextValue(has_raw_value=sample_value)
            return tv_obj

        elif ssd_range == "geolocation value":
            geolocation_value = extract_lat_lon(sample_value)
            return geolocation_value

        elif ssd_range == "quantity value":
            quantity_value = process_qv(sample_value)
            return quantity_value

        elif ssd_range == "timestamp value":
            timestamp_value = TimestampValue(has_raw_value=sample_value)
            try:
                # todo need to try more variations on legal partial date and illegal dates
                parsed_value = pendulum.parse(sample_value)
                logger.debug(
                    f"{sample_value} can be parsed as {parsed_value} so will be returned as {timestamp_value}"
                )
                return timestamp_value
            except ValueError:
                logger.error(f"{ValueError}: {sample_value} is not a valid timestamp")

        elif ssd_range == "controlled term value":
            ctv = extract_ctv(sample_value)
            return ctv

        elif ssd_range == "named thing":
            return sample_value

    def instantiate_biosample(self, deep_parse_dict, study_id):
        # todo do this based on introspection of the nmdc-schema and the submission portal schema
        for i in drop_cols_pending_research:
            if i in deep_parse_dict:
                logger.warning(f"dropping column {i}")
                del deep_parse_dict[i]

        for k, v in dh_field_name_overrides.items():
            if k in deep_parse_dict:
                logger.warning(f"replacing Biosample slot {k} with {v}")
                deep_parse_dict[v] = deep_parse_dict.pop(k)

        # todo determine the biosample_requireds by introspection
        biosample_requireds = [
            "id",
            "sample_link",
            "env_broad_scale",
            "env_local_scale",
            "env_medium",
        ]
        logger.debug(f"biosample_requireds: {biosample_requireds}")
        required_values = {
            key: value
            for key, value in deep_parse_dict.items()
            if key in biosample_requireds
        }
        logger.debug(f"required_values: {pprint.pformat(required_values)}")
        remaining_slots = list(set(deep_parse_dict.keys()) - set(biosample_requireds))
        logger.debug(f"remaining_slots: {remaining_slots}")

        try:
            instantiated_biosample = Biosample(**required_values)
            logger.debug(f"INSTANTIATED!")
            for current_remaining in remaining_slots:
                logger.debug(
                    f"still need to add: {current_remaining} of {deep_parse_dict[current_remaining]}"
                )
                try:
                    instantiated_biosample[current_remaining] = deep_parse_dict[
                        current_remaining
                    ]
                except (KeyError, ValueError, TypeError) as add_e:
                    logger.error(f"addition error: {add_e}")
            return instantiated_biosample

        except (ValueError, TypeError) as e:
            available_identifiers = ""
            if "name" in deep_parse_dict:
                available_identifiers = f"with name {deep_parse_dict['name']}"
            if "source_mat_id" in deep_parse_dict:
                if "has_raw_value" in deep_parse_dict["source_mat_id"]:
                    available_identifiers = (
                        available_identifiers
                        + f" with source_mat_id {deep_parse_dict['source_mat_id']['has_raw_value']}"
                    )
            logger.error(
                f"study {study_id} error instantiating Biosample {available_identifiers}: {e}"
            )
            logger.debug(traceback.format_exc())
            logger.debug(f"{pprint.pformat(deep_parse_dict)}")

    def deep_parse_sample_metadata(self, sample_database_file):
        for study_id, samples in self.sample_metadata_by_slotname.items():
            for sample in samples:
                logger.debug(f"current sample:\n{pprint.pformat(sample)}")
                dp_sample_dict = {}
                lists_for_appending = {}
                if sample:
                    for sample_slot, sample_value in sample.items():
                        if sample_value:
                            logger.debug(
                                f"study {study_id} has {sample_slot} value of {sample_value}"
                            )
                            sample_slot_definition = self.nmdc_view.get_slot(
                                sample_slot
                            )
                            logger.debug(yaml_dumper.dumps(sample_slot_definition))
                            # if not sample_slot_definition:
                            #     underscored = re.sub(" ", "_", sample_slot)
                            #     logger.warning(
                            #         f"attempting to lookup {sample_slot} as {underscored}"
                            #     )
                            #     sample_slot_definition = self.nmdc_view.get_slot(
                            #         underscored
                            #     )
                            logger.debug(yaml_dumper.dumps(sample_slot_definition))
                            if sample_slot_definition:
                                ssd_range = sample_slot_definition["range"]
                                if ssd_range:
                                    logger.debug(
                                        f"{sample_slot} has an explicit range of {ssd_range}"
                                    )
                                else:
                                    schema_default_range = (
                                        self.nmdc_view.schema.default_range
                                    )
                                    logger.info(
                                        f"{sample_slot} uses the default range: {schema_default_range}"
                                    )
                                    ssd_range = schema_default_range
                            else:
                                logger.warning(
                                    f"data still contains a slot {sample_slot}, which is not in the schema"
                                )
                            # todo check for special cases like enums
                            range_def = self.nmdc_view.get_element(ssd_range)
                            range_type = type(range_def).class_name
                            parse_result = self.parse_any_range(
                                sample_value, ssd_range, range_type
                            )
                            logger.debug(
                                f"{study_id}'s sample {sample['source_mat_id']}/{sample['samp_name']}'s {sample_slot} value of  _{sample_value}_ with range {ssd_range} and type {range_type} was converted to {parse_result}"
                            )
                            if type(parse_result) == list:
                                lists_for_appending[sample_slot] = parse_result
                            else:
                                dp_sample_dict[sample_slot] = parse_result
                        else:
                            logger.debug("saw an empty-ish value")
                else:
                    logger.debug(f"the whole sample was empty")

                biosample_instance = self.instantiate_biosample(
                    dp_sample_dict, study_id
                )

                if biosample_instance:
                    for ltk, ltv in lists_for_appending.items():
                        if ltk in drop_cols_pending_research:
                            pass
                        else:
                            # todo get this as the slot name or alias? may require some induction?
                            underscored = re.sub(" ", "_", ltk)
                            slot = self.nmdc_view.get_slot(ltk)
                            logger.debug(f"{ltk} has slot definition:")
                            logger.debug(yaml_dumper.dumps(slot))
                            multivalued = slot["multivalued"]
                            list_len = len(ltv)
                            logger.debug(
                                f"working on {ltv} from {ltk}. List len: {list_len}. Multivalued? {multivalued}"
                            )

                            if list_len == 0:
                                pass
                            elif list_len == 1:
                                if multivalued:
                                    for i in ltv:
                                        logger.debug(
                                            f"adding singleton {i} to {underscored}"
                                        )
                                        biosample_instance[underscored].append(i)
                                else:
                                    logger.debug(
                                        f"setting singleton {i} to {underscored}"
                                    )
                                    biosample_instance[underscored] = ltv[0]
                            else:
                                if multivalued:
                                    for i in ltv:
                                        logger.debug(
                                            f"adding list item {i} to {underscored}"
                                        )
                                        biosample_instance[underscored].append(i)
                                else:
                                    logger.warning(
                                        f"{ltk} is not multivalued, but multiple values were provided: {ltv}"
                                    )
                if biosample_instance:
                    self.biosample_database["biosample_set"].append(biosample_instance)

        yaml_dumper.dump(self.biosample_database, sample_database_file)

    def sample_metadata_to_csv(self, sample_metadata_csv_file):
        logger.debug(f"self.updatable_monikers: {self.updatable_monikers}")
        with open(sample_metadata_csv_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.updatable_monikers)
            writer.writeheader()
            if self.sample_metadata_by_slotname:
                for k, v in self.sample_metadata_by_slotname.items():
                    writer.writerows(v)

    def study_metadata_to_yaml(self, study_metadata_yaml_file):
        with open(study_metadata_yaml_file, "w") as outfile:
            yaml.dump(self.study_metadata, outfile, default_flow_style=False)

    # todo this has keys, so couldn't be directly ingested into LinkML
    #   also what's the difference between null values and ""?
    def sample_metadata_to_yaml(self, sample_metadata_yaml_file):
        with open(sample_metadata_yaml_file, "w") as outfile:
            yaml.dump(
                self.sample_metadata_by_slotname, outfile, default_flow_style=False
            )

    def get_final_slot_names(self):
        pass


@click.group()
def cli():
    pass


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env_file",
    default="local/.env",
)
@click.option(
    "--data_portal_url",
    type=click.Choice(
        ["https://data.dev.microbiomedata.org/", "https://data.microbiomedata.org/"]
    ),
    default="https://data.microbiomedata.org/",
    # required=True,
)
@click.option("--page_start", default=0)
@click.option("--page_stop", default=999)
@click.option(
    "--study_metadata_yaml_file", type=click.Path(), default="study_metadata.yaml"
)
@click.option(
    "--sample_metadata_csv_file", type=click.Path(), default="sample_metadata.csv"
)
@click.option(
    "--sample_metadata_yaml_file",
    type=click.Path(),
    default="sample_metadata.yaml",
    help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
)
@click.option(
    # todo need better handling when only complete is considered acceptable and there are no complete studies
    "--acceptable_statuses",
    type=click.Choice(["complete", "in-progress"]),
    default=["complete", "in-progress"],
    multiple=True,
)
@click.option(
    "--suggested_initial_columns",
    type=click.Choice(["samp_name", "source_mat_id", "id", "sample_link"]),
    default=["samp_name", "source_mat_id", "id", "sample_link"],
    multiple=True,
)
def from_submissions(
    env_file,
    data_portal_url,
    study_metadata_yaml_file,
    sample_metadata_csv_file,
    page_start,
    page_stop,
    acceptable_statuses,
    suggested_initial_columns,
    sample_metadata_yaml_file,
):
    """from submissions help"""

    load_vars_from_env_file(env_file)

    sandbox = SubmissionsSandbox(
        api_url=data_portal_url, session_cookie=get_session_cookie()
    )

    sandbox.minting_client = nrs.RuntimeApiSiteClient(
        base_url="https://api.dev.microbiomedata.org",
        site_id=os.getenv("site_id"),
        client_id=os.getenv("client_id"),
        client_secret=os.getenv("client_secret"),
    )

    sandbox.view_setup()

    sandbox.get_submission_titles_and_names()

    sandbox.get_one_submission_page_from_api(start=page_start, stop=page_stop)

    sandbox.update_monikers(
        acceptable_statuses=acceptable_statuses,
        suggested_initial_columns=[],
    )

    sandbox.tidy_flat_sample_metadata(acceptable_statuses=acceptable_statuses)

    #  todo need better handling when only complete is considered acceptable and there are no complete studies
    sandbox.update_monikers(
        acceptable_statuses=acceptable_statuses,
        suggested_initial_columns=suggested_initial_columns,
        source="tidied",
    )

    # todo separate out write step?
    sandbox.deep_parse_sample_metadata(sample_database_file=sample_metadata_yaml_file)

    sandbox.sample_metadata_to_csv(sample_metadata_csv_file=sample_metadata_csv_file)

    sandbox.study_metadata_to_yaml(study_metadata_yaml_file=study_metadata_yaml_file)

    # sandbox.sample_metadata_to_yaml(sample_metadata_yaml_file=sample_metadata_yaml_file)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env_file",
    default="local/.env",
)
@click.option(
    "--sample_metadata_csv_file", type=click.Path(), default="sample_metadata.csv"
)
@click.option(
    "--sample_metadata_yaml_file",
    type=click.Path(),
    default="sample_metadata.yaml",
    help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
)
@click.option("--data_csv", type=click.Path(exists=True))
@click.option("--static_project_id", required=True)
@click.option("--static_dh_template", required=True)
@click.option(
    "--suggested_initial_columns",
    type=click.Choice(["samp_name", "source_mat_id", "id", "sample_link"]),
    default=["samp_name", "source_mat_id", "id", "sample_link"],
    multiple=True,
)
def from_csv(
    env_file,
    sample_metadata_csv_file,
    suggested_initial_columns,
    data_csv,
    static_project_id,
    static_dh_template,
    sample_metadata_yaml_file,
):
    """from submissions help"""

    static_project_status = "placeholder"

    load_vars_from_env_file(env_file)

    with open(data_csv, newline="") as csvfile:
        rows = []
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row:
                row["sample_link"] = static_project_id
                rows.append(row)
    sandbox = SubmissionsSandbox()
    sandbox.sample_metadata_by_title = {static_project_id: rows}

    logger.debug(pprint.pformat(sandbox.sample_metadata_by_title))

    # todo use today's date as the created date?
    #  do we capture the study start or end date anywhere?
    sandbox.study_metadata = {
        static_project_id: {
            "created": datetime.today().strftime("%Y-%m-%d"),
            "metadata_submission": {"template": static_dh_template},
            "status": static_project_status,
        }
    }

    logger.debug(pprint.pformat(sandbox.study_metadata))

    sandbox.update_monikers(
        acceptable_statuses=[static_project_status],
        suggested_initial_columns=suggested_initial_columns,
    )

    sandbox.minting_client = nrs.RuntimeApiSiteClient(
        base_url="https://api.dev.microbiomedata.org",
        site_id=os.getenv("site_id"),
        client_id=os.getenv("client_id"),
        client_secret=os.getenv("client_secret"),
    )

    sandbox.view_setup()

    sandbox.tidy_flat_sample_metadata(acceptable_statuses=[static_project_status])

    sandbox.update_monikers(
        acceptable_statuses=static_project_status,
        suggested_initial_columns=suggested_initial_columns,
        source="tidied",
    )

    sandbox.sample_metadata_to_csv(sample_metadata_csv_file=sample_metadata_csv_file)

    # todo separate out write step?
    sandbox.deep_parse_sample_metadata(sample_database_file=sample_metadata_yaml_file)

    # sandbox.sample_metadata_to_yaml(sample_metadata_yaml_file=sample_metadata_yaml_file)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env_file",
    default="local/.env",
)
@click.option(
    "--biosample_sql_file",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "--biosample_id_file",
    type=click.Path(exists=True),
    required=True,
    help="Biosample IDs, like SAMN00000002, one per line with no header, no prefixes and no quotes",
)
@click.option(
    "--static_project_id",
    required=True,
)
@click.option(
    "--sample_metadata_yaml_file",
    type=click.Path(),
    default="sample_metadata.yaml",
    help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
)
def from_sqlite(
    env_file,
    biosample_sql_file,
    biosample_id_file,
    static_project_id,
    sample_metadata_yaml_file,
):
    """biosample_sql_file help"""

    # MIMS Environmental/Metagenome sample from soil metagenome (NAME)

    # Identifiers
    # BioSample: SAMN08902828; Sample name: 14_0903_02_20cm; SRA: SRS3199723
    # Organism
    # soil metagenome
    # unclassified entries; unclassified sequences; metagenomes; ecological metagenomes
    # Package
    # MIMS: metagenome/environmental, soil; version 5.0
    # Attributes
    # collection date	2014-09-03
    # depth	10-20 cm
    # elevation	1417 ft
    # broad-scale environmental context	temperate grassland biome
    # local-scale environmental context	meadow soil
    # environmental medium	soil
    # geographic location	USA: Angelo Coast Range Reserve, CA
    # latitude and longitude	39.74 N 123.63 W
    # isolation source	Plot 2
    # Description
    # Keywords: GSC:MIxS;MIMS:5.0
    #
    # BioProject
    # PRJNA449266 soil metagenome
    # Retrieve all samples from this project
    #
    # Submission
    # BanfieldLab; 2018-04-09
    # Accession: SAMN08902828 ID: 8902828
    # BioProject SRA

    logger.debug(biosample_sql_file)

    with open(biosample_id_file) as f:
        biosample_ids = f.readlines()

    accession_list = [x.strip() for x in biosample_ids]

    logger.debug(accession_list)

    sandbox = SubmissionsSandbox()

    # todo may not need submission portal view
    sandbox.view_setup()

    bs_attributes = sandbox.nmdc_view.induced_class("biosample").attributes

    bs_attribute_names = [k for k, v in bs_attributes.items()]

    bs_attribute_names.sort()

    logger.info(f"bs_attribute_names: {bs_attribute_names}")

    conn = None

    try:
        conn = sqlite3.connect(biosample_sql_file)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        logger.critical(e)
        exit()

    cursor = conn.cursor()

    accession_core = "', '".join(accession_list)

    accession_tidy = f"('{accession_core}')"

    query = f"SELECT * FROM harmonized_wide hw join non_attribute_metadata nam on hw.raw_id = nam.raw_id where accession in {accession_tidy}"

    cursor.execute(query)

    rows = cursor.fetchall()

    row_count = len(rows)

    # todo report column names
    logger.info(f"{row_count} SQLite rows retrieved")

    load_vars_from_env_file(env_file)

    sandbox.minting_client = nrs.RuntimeApiSiteClient(
        base_url="https://api.dev.microbiomedata.org",
        site_id=os.getenv("site_id"),
        client_id=os.getenv("client_id"),
        client_secret=os.getenv("client_secret"),
    )

    id_list = sandbox.get_ids_list(row_count)

    # biosample_sqlite_to_dh_field_name = {
    #     "title": {"dh": "samp_name", "format": "scalar"},
    #     "id": {"dh": "INSDC biosample identifiers", "format": "list"},
    #     "sra_id": {"dh": "INSDC secondary sample identifiers", "format": "list"},
    # }

    rows_list = []
    for row in rows:
        row_dict = {}
        logger.info(row["id"])
        sqlite_cols = list(row.keys())
        for k in sqlite_cols:
            if row[k] and k in biosample_sqlite_to_dh_field_name:
                mapping_dict = biosample_sqlite_to_dh_field_name[k]
                dh_name = mapping_dict["dh"]
                mapping_format = mapping_dict["format"]
                logger.info(f"mapping: {k} {row[k]} {dh_name} {mapping_format}")
                # todo why isn't INSDC_secondary_sample_identifiers being treated like a list?
                if mapping_format == "list":
                    row_dict[dh_name] = [row[k]]
                else:
                    row_dict[dh_name] = row[k]
            elif row[k] and k in bs_attribute_names:
                row_dict[k] = row[k]
            elif row[k]:
                logger.info(f"don't know what to do with {k}")

        row_dict["id"] = id_list.pop()
        row_dict["sample_link"] = static_project_id
        row_dict["source_mat_id"] = "requires new functionality in biosample-basex"
        rows_list.append(row_dict)

    # https://www.ncbi.nlm.nih.gov/biosample/8902828

    for i in rows_list:
        logger.debug(pprint.pformat(i))

    sandbox.sample_metadata_by_slotname = {static_project_id: rows_list}

    # todo monitor INSDC_biosample_identifiers
    #  None → 0..* ExternalIdentifier (which is a type with root URIorCURIE and representation str)
    #    currently passing a scalar, which is silently repaired to a list at instantiation time?
    sandbox.deep_parse_sample_metadata(sample_database_file=sample_metadata_yaml_file)


cli.add_command(from_submissions)
cli.add_command(from_csv)
cli.add_command(from_sqlite)

if __name__ == "__main__":
    cli()
