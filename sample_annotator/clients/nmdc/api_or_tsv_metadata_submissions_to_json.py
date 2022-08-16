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

import chardet
import click
import click_log
import pendulum
import requests
import yaml
from dotenv import load_dotenv
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper, json_dumper
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
from requests.auth import HTTPBasicAuth

import sample_annotator.clients.nmdc.nmdc_runtime_snippets as nrs

import LatLon23

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

# biosample_sqlite_col_name_overrides = {
#     # ideally: mention slots wiht underscores and write methods that try underscored or whitesapced
#     #   better: make a lookup between al schema slots and their induced aliases or keys
#     # "title": {"dh": "name", "format": "scalar"},  # 0..1
#     "id": {"dh": "INSDC_biosample_identifiers", "format": "list"},  # 0..*
#     #   what's the right biosample slot for SRAs
#     "sra_id": {"dh": "INSDC_secondary_sample_identifiers", "format": "scalar"},
#     # ...secondary... is 0..* according to the docs at https://microbiomedata.github.io/nmdc-schema/Biosample/#class-biosample
#     # but NOT multivalued according to the term definition at
#     #   https://github.com/microbiomedata/nmdc-schema/blob/b0775ffd354f6064124bef34bb9fc15eb6ae4084/src/schema/external_identifiers.yaml#L173-L184
#     # "samp_name": {"dh": "source_mat_id", "format": "scalar"},  # 0..1
#     "samp_name": {"dh": "identifier", "format": "scalar"},  # 0..1
#     "bp_acc": {"dh": "project_ID", "format": "scalar"},  # 0..1
#     # added join with the accession table, which required a new index
#     # CREATE INDEX bp_id_accession_bp_id_IDX ON bp_id_accession (bp_id);
#     "taxonomy_name": {"dh": "ncbi_taxonomy_name", "format": "scalar"},  # 0..1
#     "paragraph": {"dh": "description", "format": "scalar"},  # 0..1
# }

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
    logger.debug(f"processing qv: {raw_value}")
    qv = QuantityValue(has_raw_value=raw_value)
    quants = parser.parse(str(raw_value))
    if len(quants) > 0:
        if len(quants) > 1:
            logger.error(f"quantulum3 found more than one quantity in: {quants}")
        quant = quants[0]
        if quant.uncertainty:
            qv.has_minimum_numeric_value = round((quant.value - quant.uncertainty), ndigits=3)
            qv.has_maximum_numeric_value = round((quant.value + quant.uncertainty), ndigits=3)
            # qv.has_maximum_numeric_value = quant.value + quant.uncertainty
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
    # todo this is way to complicated
    gv = GeolocationValue(has_raw_value=raw_value)
    if raw_value:
        logger.debug(f"extracting lat/lon from {raw_value}")
        strip_ed = raw_value.strip()
        logger.debug(f"{raw_value} stripped as {strip_ed}")
        pattern = r"\s+"
        split_ed = re.split(pattern, strip_ed)
        logger.debug(f"split {strip_ed} into {split_ed}")

        if len(split_ed) == 2:
            try:
                latv = float(split_ed[0])
                longv = float(split_ed[1])
            except Exception as e:
                logger.error(f"failed to parse lat/lon from {raw_value}: {e}")
            if (
                    len(split_ed) == 2
                    and -90 <= latv <= 90
                    and -180 <= longv <= 180
            ):
                gv.latitude = latv
                gv.longitude = longv
            else:
                logger.error(f"{raw_value} has out-of-range values")
        # todo this will error out if
        elif len(split_ed) == 4:
            try:
                latv = float(split_ed[0])
                lath = split_ed[1].upper()
                longv = float(split_ed[2])
                longh = split_ed[3].upper()
            except Exception as e:
                logger.error(f"failed to parse lat/lon and hemisphere values from {raw_value}: {e}")
            if (
                    0 <= latv <= 90
                    and 0 <= longv <= 180
                    and lath in ['N', 'S']
                    and longh in ['E', 'W']
            ):
                if lath.upper() == 'N':
                    gv.latitude = abs(latv)
                else:
                    gv.latitude = -abs(latv)
                if longh.upper() == 'E':
                    gv.longitude = abs(longv)
                else:
                    gv.longitude = -abs(longv)
            else:
                logger.error(
                    f"{split_ed} does not follow the expected with-hemispheres format"
                )
        else:
            logger.error(
                f"{split_ed} should contain exactly two in-range numbers, or exactly two in-range number/hemisphere pairs"
            )

    logger.debug(f"gv: {gv}")
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

def underscored_to_coloned(string):
    return string.replace("_", ":")


@dataclass
class SubmissionsSandbox:
    url_suffix = "api/metadata_submission"

    study_metadata: Optional[Dict[str, Any]] = None
    sample_metadata_by_title: Optional[Dict[str, List[List[str]]]] = None
    sample_metadata_by_slotname: Optional[Dict[str, List[List[str]]]] = None
    updatable_monikers: Optional[List[str]] = None

    api_url: Optional[str] = None
    session_cookie: Optional[str] = None

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

    assess_sqlite_mappings: bool = True

    # todo switch to w3id URLs
    schemas_dict = {
        "mixs": "https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/model/schema/mixs.yaml",
        "nmdc": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml",
        "submission": "https://raw.githubusercontent.com/microbiomedata/sheets_and_friends/main/artifacts/nmdc_submission_schema.yaml",
    }

    # todo allow user to specify which schemas to load
    def view_setup(self, schemas_to_load: List[str] = None) -> None:
        if schemas_to_load is None:
            schemas_to_load = ["nmdc", "submission"]
        for schema_key in schemas_to_load:
            self.nmdc_view = get_view(self.schemas_dict[schema_key])

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
                        # row_dict["part_of"] = result_id
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

    def get_ids_list(self, list_size, naa: str = "nmdc", shoulder: str = "fk0"):
        # todo parameterize this
        minting_params = {
            "populator": "",
            "naa": naa,
            "shoulder": shoulder,
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
        # current_slot_dict["sample_link"] = "sample_link"
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

    def deep_parse_sample_metadata(self):
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

                            sample_slot_definition = self.slot_def_from_underscored_or_whitespaced(
                                slot_moniker=sample_slot)

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
                                    logger.debug(
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
                            # todo need for flexible sample identification
                            # logger.debug(
                            #     f"{study_id}'s sample {sample['source_mat_id']}/{sample['samp_name']}'s {sample_slot} value of  _{sample_value}_ with range {ssd_range} and type {range_type} was converted to {parse_result}"
                            # )
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

                            slot = self.slot_def_from_underscored_or_whitespaced(ltk)

                            slot_name = re.sub(" ", "_", slot["name"])

                            logger.debug(f"{slot_name} has slot definition:")
                            logger.debug(yaml_dumper.dumps(slot))
                            multivalued = slot["multivalued"]
                            list_len = len(ltv)
                            logger.debug(
                                f"working on {ltv} from {slot_name}. List len: {list_len}. Multivalued? {multivalued}"
                            )

                            if list_len == 0:
                                logger.warning(f"{slot_name} has no values")
                            elif list_len == 1:
                                if multivalued:
                                    for i in ltv:
                                        logger.debug(
                                            f"adding singleton {i} to {slot_name}"
                                        )
                                        biosample_instance[slot_name].append(i)
                                        logger.debug(f"multivalued singleton {biosample_instance[slot_name]}")
                                        logger.debug(f"biosample instance {biosample_instance}")
                                else:
                                    logger.debug(
                                        f"setting singleton {i} to {slot_name}"
                                    )
                                    biosample_instance[slot_name] = ltv[0]
                            else:
                                if multivalued:
                                    for i in ltv:
                                        logger.debug(
                                            f"adding list item {i} to {slot_name}"
                                        )
                                        biosample_instance[slot_name].append(i)
                                else:
                                    logger.warning(
                                        f"{ltk} is not multivalued, but multiple values were provided: {ltv}"
                                    )
                logger.debug(f"biosample instance outside of lists_for_appending: {biosample_instance}")
                if biosample_instance:
                    self.biosample_database["biosample_set"].append(biosample_instance)

        # yaml_dumper.dump(self.biosample_database, sample_database_file)

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

    # # todo this has keys, so couldn't be directly ingested into LinkML
    # #   also what's the difference between null values and ""?
    # def sample_metadata_to_yaml(self, sample_metadata_yaml_file):
    #     with open(sample_metadata_yaml_file, "w") as outfile:
    #         yaml.dump(
    #             self.sample_metadata_by_slotname, outfile, default_flow_style=False
    #         )

    def sample_metadata_to_yaml(self, sample_metadata_yaml_file):
        yaml_dumper.dump(self.biosample_database, sample_metadata_yaml_file)

    def sample_metadata_to_json(self, sample_metadata_json_file):
        json_dumper.dump(self.biosample_database, sample_metadata_json_file)

    # def get_final_slot_names(self):
    #     pass

    def get_biosamples_from_sqlite_by_accession(self,
                                                biosample_id_file,
                                                biosample_sql_file,
                                                sqlite_to_biosample_file,
                                                static_project_id
                                                ):
        logger.info(f"will read sqlite_to_biosample mapping from {sqlite_to_biosample_file}")

        sqlite_to_biosample_dict = {}
        with open(sqlite_to_biosample_file, newline='') as file:
            reader = csv.DictReader(file, delimiter="\t")
            for row in reader:
                sqlite_to_biosample_dict[row["sqlite"]] = row

        logger.debug(f"sqlite_to_biosample_dict: {pprint.pformat(sqlite_to_biosample_dict)}")

        logger.info(f"will query {biosample_sql_file} with accessions from {biosample_id_file}")

        with open(biosample_id_file) as f:
            # todo error handling
            #   also strip off prefixes
            biosample_accessions = f.readlines()

        biosample_accessions = [x.strip() for x in biosample_accessions]

        biosample_accessions = [re.sub(
            pattern=r"^BIOSAMPLE:", repl="", string=x
        ) for x in biosample_accessions]

        biosample_accessions.sort()

        logger.info(pprint.pformat(biosample_accessions))

        sandbox = SubmissionsSandbox()

        sandbox.view_setup(schemas_to_load=["nmdc"])

        bs_attributes = sandbox.nmdc_view.induced_class("biosample").attributes
        bs_attribute_names = [k for k, v in bs_attributes.items()]
        bs_attribute_names.sort()
        logger.info(f"bs_attribute_names: {pprint.pformat(bs_attribute_names)}")

        conn = None

        try:
            conn = sqlite3.connect(biosample_sql_file)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            logger.critical(e)
            exit()

        cursor = conn.cursor()

        accession_core = "', '".join(biosample_accessions)

        accession_tidy = f"('{accession_core}')"

        query = f"""
        SELECT * FROM harmonized_wide hw 
        join non_attribute_metadata nam on hw.raw_id = nam.raw_id 
        join bp_id_accession bia on nam.bp_id = bia.bp_id
        where accession in {accession_tidy}"""

        cursor.execute(query)

        rows = cursor.fetchall()

        row_count = len(rows)

        # todo report column names? see below
        logger.info(f"{row_count} SQLite rows retrieved")

        # todo this only gets the harmonized values
        rows_list = []
        for row in rows:
            row_dict = {}
            logger.info(f"processing SQLite row for {row['id']}")
            sqlite_cols = list(row.keys())
            if sandbox.assess_sqlite_mappings:
                # todo report column names? see above
                for mk in sqlite_to_biosample_dict.keys():
                    if mk not in sqlite_cols:
                        logger.warning(
                            f"{mk} has a mapping assigned but is not in the retrieved sqlite columns: {sqlite_cols}")
                    else:
                        logger.debug(f"{mk} has a valid mapping assigned")
                sandbox.assess_sqlite_mappings = False
            for k in sqlite_cols:
                if row[k]:
                    if k in sqlite_to_biosample_dict:
                        current_mapping = sqlite_to_biosample_dict[k]
                        if current_mapping["action"] == "ignore/drop":
                            logger.debug(f"would drop sqlite column from Biosample modeling: {current_mapping}")
                            logger.warning(f"dropping sqlite column {current_mapping['sqlite']}")
                        elif current_mapping["action"] == "replace":
                            logger.debug(f"would replace sqlite column for Biosample modeling: {current_mapping}")
                            logger.warning(
                                f"replacing sqlite column {current_mapping['sqlite']} with Biosample slot {current_mapping['biosample-slot']} {row[k]}")
                            if current_mapping["format"] == "list":
                                row_dict[current_mapping["biosample-slot"]] = [row[k]]
                            else:
                                row_dict[current_mapping["biosample-slot"]] = row[k]
                        else:
                            logger.error(f"don't know how to process sqlite to Biosample mapping: {current_mapping}")
                    else:
                        if k in bs_attribute_names:
                            row_dict[k] = row[k]
                        else:
                            logger.warning(f"don't know what to do with sqlite column {k}")
            row_dict["sample_link"] = static_project_id
            row_dict["part_of"] = [static_project_id]
            rows_list.append(row_dict)

        logger.debug(f"{pprint.pformat(rows_list)}")

        for i in rows_list:
            logger.debug(pprint.pformat(i))

        return rows_list

    def get_biosamples_from_gold_by_seq_proj(self, gold_study_id, gold_mapping_file):
        logger.debug(f"will query {gold_study_id}")

        gold_mapping_dict = {}
        with open(gold_mapping_file, newline='') as file:
            reader = csv.DictReader(file, delimiter="\t")
            for row in reader:
                gold_mapping_dict[row["GOLD field"]] = row

        # todo analysis of gold mapped fields vs gold retrieved fields

        logger.debug(f"gold_mapping_dict: {gold_mapping_dict}")

        user = os.getenv('nmdc_gold_api_user')
        logger.debug(user)
        password = os.getenv('nmdc_gold_api_password')
        logger.debug(password)
        endpoint_url = 'https://gold.jgi.doe.gov/rest/nmdc/projects'

        params = {"studyGoldId": re.sub(pattern="^gold:", repl="", string=gold_study_id)}

        logger.debug(f"params: {pprint.pformat(params)}")

        results = requests.get(
            endpoint_url, params=params, auth=HTTPBasicAuth(user, password)
        )

        projects_list = results.json()

        logger.debug(f"{pprint.pformat(projects_list[0])}")

        identifiable_projects = {}
        for current_project in projects_list:
            if "biosampleGoldId" in current_project and current_project["biosampleGoldId"]:
                if "ncbiBioSampleAccession" in current_project and current_project["ncbiBioSampleAccession"]:
                    logger.debug(
                        f"biosampleGoldId: {current_project['projectGoldId']} from project {current_project['projectGoldId']} from study {current_project['studyGoldId']}; ncbiBioSampleAccession: {current_project['ncbiBioSampleAccession']}")
                    identifiable_projects[current_project["projectGoldId"]] = current_project
                else:
                    logger.warning(
                        f"{current_project['projectGoldId']} from study {current_project['studyGoldId']} has no ncbiBioSampleAccession")
            else:
                logger.warning(
                    f"{current_project['projectGoldId']} from study {current_project['studyGoldId']} has no biosampleGoldId")

        logger.warning(f"retained {len(identifiable_projects)} of {len(projects_list)} projects")

        outer_biosample_list = []
        project_ids = [v['projectGoldId'] for k, v in identifiable_projects.items()]
        project_ids.sort()
        last_project = project_ids[-1]
        # todo remove constraint on last project
        for current_identifiable in project_ids:

            biosample_dict = {}

            civ = identifiable_projects[current_identifiable]
            logger.debug(f"working on {current_identifiable} of {last_project}")

            logger.debug(f"{pprint.pformat(civ)}")

            endpoint_url = 'https://gold.jgi.doe.gov/rest/nmdc/biosamples'

            params = {"biosampleGoldId": civ['biosampleGoldId']}

            logger.debug(f"params: {pprint.pformat(params)}")

            logger.debug(f"{civ['biosampleGoldId']}")

            results = requests.get(
                endpoint_url, params=params, auth=HTTPBasicAuth(user, password)
            )

            biosample_list = results.json()

            if len(biosample_list) != 1:
                logger.warning(f"results mention {len(biosample_list)} biosamples")

            biosample_obj = biosample_list[0]

            for bk, bv in biosample_obj.items():
                if bv:
                    logger.debug(f"{bk}: {bv}")
                    if bk in gold_mapping_dict:
                        if "NMDC field" in gold_mapping_dict[bk]:
                            if gold_mapping_dict[bk]["NMDC field"]:
                                mapped_col_name = gold_mapping_dict[bk]['NMDC field']
                                logger.debug(f"{bk}: {bv} -> {mapped_col_name}")
                                current_is_list = bool(gold_mapping_dict[bk]['NMDC field is a list'])
                                current_is_path = bool(gold_mapping_dict[bk]['NMDC field is a path'])
                                if not current_is_list and not current_is_path:
                                    biosample_dict[mapped_col_name] = bv
                                elif current_is_list and not current_is_path:
                                    biosample_dict[mapped_col_name] = [bv]
                                # elif current_is_path and not current_is_list:
                                #     logger.warning(f"will process {bk} with {bv} as a non-list path")
                                # else:
                                #     logger.warning(
                                #         f"don't know how to process {bk} yet because it is both a path and a list")
                            else:
                                logger.warning(f"{bk} is in gold_mapping_dict but is not mapped")
                    else:
                        logger.warning(f"{bk} not in gold_mapping_dict")
                else:
                    logger.debug(f"skipping null {bk}")

            # todo switch to declarative mapping document
            if "envoBroadScale" in biosample_obj:
                if "id" in biosample_obj["envoBroadScale"] and "label" in biosample_obj["envoBroadScale"]:
                    biosample_dict[
                        'env_broad_scale'] = f'{biosample_obj["envoBroadScale"]["label"]} [{underscored_to_coloned(biosample_obj["envoBroadScale"]["id"])}]'
            if "envoLocalScale" in biosample_obj:
                if "id" in biosample_obj["envoLocalScale"] and "label" in biosample_obj["envoLocalScale"]:
                    biosample_dict[
                        'env_local_scale'] = f'{biosample_obj["envoLocalScale"]["label"]} [{underscored_to_coloned(biosample_obj["envoLocalScale"]["id"])}]'
            if "envoMedium" in biosample_obj:
                if "id" in biosample_obj["envoMedium"] and "label" in biosample_obj["envoMedium"]:
                    biosample_dict[
                        'env_medium'] = f'{biosample_obj["envoMedium"]["label"]} [{underscored_to_coloned(biosample_obj["envoMedium"]["id"])}]'

            if "latitude" in biosample_obj and "longitude":
                biosample_dict['lat_lon'] = f'{biosample_obj["latitude"]} {biosample_obj["longitude"]}'
            else:
                logger.warning(f"{civ['biosampleGoldId']} has no lat/lon")

            if "depthInMeters" in biosample_obj:
                if biosample_obj["depthInMeters"]:
                    if "depthInMeters2" in biosample_obj:
                        if biosample_obj["depthInMeters2"]:
                            biosample_dict[
                                "depth"] = f'{biosample_obj["depthInMeters"]} to {biosample_obj["depthInMeters2"]} meters'
                        else:
                            logger.info(
                                f"depthInMeters is present, but no depthInMeters2 for {civ['biosampleGoldId']}")
                            biosample_dict["depth"] = f'{biosample_obj["depthInMeters"]} meters'
                else:
                    logger.warning(f"no depthInMeters for {civ['biosampleGoldId']}")

            logger.debug(f"GOLD biosample ID  = {civ['biosampleGoldId']}")
            logger.debug(f"GOLD project ID  = {civ['projectGoldId']}")
            logger.debug(f"GOLD study ID  = {civ['studyGoldId']}")
            logger.debug(f"NCBI biosample ID from GOLD {civ['ncbiBioSampleAccession']}")
            logger.debug(f"civ keys {civ.keys()}")
            # biosample_dict['id'] = f"gold:{civ['biosampleGoldId']}"
            biosample_dict['id'] = f"BIOSAMPLE:{civ['ncbiBioSampleAccession']}"

            biosample_dict['GOLD_sample_identifiers'] = [f"gold:{civ['biosampleGoldId']}"]

            logger.debug(f"will use gold:{civ['studyGoldId']} as the study")

            biosample_dict["sample_link"] = f"gold:{civ['studyGoldId']}"

            biosample_dict["part_of"] = [f"gold:{civ['studyGoldId']}"]

            logger.debug(f"biosample_dict: {pprint.pformat(biosample_dict)}")
            outer_biosample_list.append(biosample_dict)

        logger.debug(f"outer_biosample_list: {pprint.pformat(outer_biosample_list)}")

        return outer_biosample_list

        # # todo useful form a study perspective, but not for NMDC Biosamples
        # #   unmapped anyway
        # biosample_obj['contacts'] = None

    def slot_def_from_underscored_or_whitespaced(self, slot_moniker):
        underscored = slot_moniker.replace(" ", "_")
        whitespaced = slot_moniker.replace("_", " ")
        logger.debug(f"looking up information about slot {underscored}")
        sample_slot_definition = None
        try:
            sample_slot_definition = self.nmdc_view.induced_slot(class_name='biosample',
                                                                 slot_name=underscored)
            logger.debug(f"Found underscored slot model for {underscored}")
        except Exception as e:
            logger.debug(f"Unsuccessful underscored slot lookup: {e}")
            try:
                sample_slot_definition = self.nmdc_view.induced_slot(class_name='biosample',
                                                                     slot_name=whitespaced)
                logger.debug(f"Found whitespaced slot model for {whitespaced}")
            except Exception as e:
                logger.error(f"Couldn't find a definition of either {underscored} or {whitespaced}: {e}")
                exit()
        return sample_slot_definition


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

    # sandbox.study_metadata_to_yaml(study_metadata_yaml_file=study_metadata_yaml_file)


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


# @click.command()
# @click_log.simple_verbosity_option(logger)
# @click.option(
#     "--env_file",
#     default="local/.env",
# )
# @click.option(
#     "--biosample_sql_file",
#     type=click.Path(exists=True),
#     required=True,
# )
# @click.option(
#     "--biosample_id_file",
#     type=click.Path(exists=True),
#     required=True,
#     help="Biosample IDs, like SAMN00000002, one per line with no header, no prefixes and no quotes",
# )
# @click.option(
#     "--static_project_id",
#     required=True,
# )
# @click.option(
#     "--sample_metadata_yaml_file",
#     type=click.Path(),
#     default="sample_metadata.yaml",
#     help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
# )
# @click.option(
#     "--lookup_file",
#     type=click.Path(exists=True),
#     help="Provide a id lookup file in a known format if necessary. See also --lookup_style.",
# )
# @click.option(
#     "--lookup_style",
#     type=click.Choice(["mcafes_gold_lookup"]),
#     help="Choose a pre-defined id lookup format. See also --lookup_file.", )
# def from_sqlite(
#         env_file,
#         biosample_sql_file,
#         biosample_id_file,
#         static_project_id,
#         sample_metadata_yaml_file,
#         lookup_file,
#         lookup_style,
# ):
#     """biosample_sql_file help"""
#
#     load_dotenv(env_file)
#
#     lookup_dict = {}
#     if lookup_file and lookup_style:
#         if lookup_style == "mcafes_gold_lookup":
#
#             # for subsequent GOLD API steps
#             user = os.getenv('nmdc_gold_api_user')
#             logger.debug(user)
#             password = os.getenv('nmdc_gold_api_password')
#             logger.debug(password)
#             endpoint_url = 'https://gold.jgi.doe.gov/rest/nmdc/biosamples'
#
#             # bootstrap sample details lookup dict from lookup file
#             with open(lookup_file, encoding='utf16') as csvfile:
#                 reader = csv.DictReader(csvfile, delimiter='\t')
#                 for row in reader:
#                     lookup_dict[row['Duplicated_NCBI_Biosample']] = row
#
#             # now enrich the lookup dict with the GOLD API
#             for luk, luv in lookup_dict.items():
#                 # todo this is from an XLSX -> MS Excel utf 16 TSV conversion
#                 #   looks like there was some garbage in the spreadsheet
#                 gold_proj = luv['GOLD Sequencing Project ID'].strip()
#                 logger.info(
#                     f"richly_annotated: {luv['Duplicated_NCBI_Biosample']}; GOLD's NCBI BS ID: {luv['NBCI_Biosample']}; GOLD's seq proj id: {gold_proj}")
#
#                 params = {"projectGoldId": gold_proj}
#                 results = requests.get(
#                     endpoint_url, params=params, auth=HTTPBasicAuth(user, password)
#                 )
#                 # todo under what circumstances would there be more than one response in the list?
#                 rj = results.json()
#                 biosample_response_size = len(rj)
#                 if biosample_response_size != 1:
#                     # todo report more GOLD identifiers from lookup table
#                     #   warning: found 0 biosample, so skipping merge for SAMN08902876
#                     logger.warning(f"found {biosample_response_size} biosample, so skipping merge for {luk}")
#                 else:
#                     logger.debug(f"Will merge in GOLD's one biosample record for {luk}")
#                     biosample_response = rj[0]
#                     logger.debug(f"{luk} {biosample_response}")
#                     lookup_file_keys = set(luv.keys())
#                     gold_api_keys = set(biosample_response.keys())
#                     intersection = lookup_file_keys.intersection(gold_api_keys)
#                     logger.debug(f"lookup file keys: {lookup_file_keys}")
#                     logger.debug(f"gold api keys: {gold_api_keys}")
#                     if len(intersection) > 0:
#                         logger.warning(f"lookup file/gold api key intersection: {intersection}")
#                     luv = {**luv, **biosample_response}
#                     lookup_dict[luk] = luv
#
#         logger.debug(pprint.pformat(lookup_dict))
#
#     logger.debug(f"will query {biosample_sql_file}")
#
#     lookup_file_accession_list = [k for k, v in lookup_dict.items()]
#
#     with open(biosample_id_file) as f:
#         biosample_ids = f.readlines()
#
#     mcafes_accession_list = [x.strip() for x in biosample_ids]
#
#     lookup_only_accessions = set(lookup_file_accession_list) - set(mcafes_accession_list)
#     if len(lookup_only_accessions) > 0:
#         logger.warning(f"lookup_only_accessions: {lookup_only_accessions}")
#     mcafes_only_accessions = set(mcafes_accession_list) - set(lookup_file_accession_list)
#     if len(mcafes_only_accessions) > 0:
#         logger.warning(f"mcafes_only_accessions: {mcafes_only_accessions}")
#     intersection_accessions = set(mcafes_accession_list).intersection(set(lookup_file_accession_list))
#     intersection_accessions = list(intersection_accessions)
#     intersection_accessions.sort()
#     ia_len = len(intersection_accessions)
#     logger.debug(f"{ia_len} intersection_accessions: {intersection_accessions}")
#
#     accession_list = mcafes_accession_list
#
#     logger.debug(accession_list)
#
#     sandbox = SubmissionsSandbox()
#
#     # todo may not need submission portal view
#     sandbox.view_setup()
#
#     bs_attributes = sandbox.nmdc_view.induced_class("biosample").attributes
#
#     bs_attribute_names = [k for k, v in bs_attributes.items()]
#
#     bs_attribute_names.sort()
#
#     logger.info(f"bs_attribute_names: {bs_attribute_names}")
#
#     conn = None
#
#     try:
#         conn = sqlite3.connect(biosample_sql_file)
#         conn.row_factory = sqlite3.Row
#     except Exception as e:
#         logger.critical(e)
#         exit()
#
#     cursor = conn.cursor()
#
#     accession_core = "', '".join(accession_list)
#
#     accession_tidy = f"('{accession_core}')"
#
#     query = f"""
#     SELECT * FROM harmonized_wide hw
#     join non_attribute_metadata nam on hw.raw_id = nam.raw_id
#     join bp_id_accession bia on nam.bp_id = bia.bp_id
#     where accession in {accession_tidy}"""
#
#     cursor.execute(query)
#
#     rows = cursor.fetchall()
#
#     row_count = len(rows)
#
#     # todo report column names
#     logger.info(f"{row_count} SQLite rows retrieved")
#
#     load_vars_from_env_file(env_file)
#
#     sandbox.minting_client = nrs.RuntimeApiSiteClient(
#         base_url="https://api.dev.microbiomedata.org",
#         site_id=os.getenv("site_id"),
#         client_id=os.getenv("client_id"),
#         client_secret=os.getenv("client_secret"),
#     )
#
#     id_list = sandbox.get_ids_list(row_count)
#
#     rows_list = []
#     for row in rows:
#         row_dict = {}
#         logger.info(f"processing SQLite row for {row['id']}")
#         sqlite_cols = list(row.keys())
#         for k in sqlite_cols:
#             if row[k] and k in biosample_sqlite_col_name_overrides:
#                 mapping_dict = biosample_sqlite_col_name_overrides[k]
#                 dh_name = mapping_dict["dh"]
#                 mapping_format = mapping_dict["format"]
#                 logger.info(
#                     f"mapping SQLite column {k} with value {row[k]} to DH column {dh_name}. Format =  {mapping_format}")
#                 # todo why isn't INSDC_secondary_sample_identifiers being treated like a list?
#                 if mapping_format == "list":
#                     row_dict[dh_name] = [row[k]]
#                 else:
#                     row_dict[dh_name] = row[k]
#             elif row[k] and k in bs_attribute_names:
#                 row_dict[k] = row[k]
#             elif row[k]:
#                 logger.info(f"don't know what to do with SQLite column {k}")
#
#         if lookup_file and lookup_style and lookup_style == "mcafes_gold_lookup" and row[
#             'accession'] in lookup_dict and 'biosampleGoldId' in lookup_dict[row['accession']]:
#             logger.info(f"found lookup record for {row['accession']}")
#             current_lookup = lookup_dict[row['accession']]
#
#             row_dict["description"] = current_lookup['description']
#             row_dict["id"] = f"gold:{current_lookup['biosampleGoldId']}"
#             row_dict["name"] = current_lookup['biosampleName']
#             row_dict["sample_collection_site"] = current_lookup['sampleCollectionSite']
#             row_dict["habitat"] = current_lookup['habitat']
#
#             # todo or check lookup?
#             # row_dict["part_of"] = static_project_id
#             row_dict["sample_link"] = static_project_id
#
#             # todo
#             # row_dict["depth2"] = static_project_id
#
#             row_dict["ecosystem"] = current_lookup['ecosystem']
#             row_dict["ecosystem_category"] = current_lookup['ecosystemCategory']
#             row_dict["ecosystem_subtype"] = current_lookup['ecosystemSubtype']
#             row_dict["ecosystem_type"] = current_lookup['ecosystemType']
#             row_dict["specific_ecosystem"] = current_lookup['specificEcosystem']
#
#             # todo check this
#             triad_terms = ["envoBroadScale", "envoLocalScale", "envoMedium"]
#             for current_from_triad in triad_terms:
#                 current_term = current_lookup[current_from_triad]['id']
#                 current_label = current_lookup[current_from_triad]['label']
#                 coloned_term = underscored_to_coloned(current_term)
#                 row_dict[current_from_triad] = f"{current_label} [{coloned_term}]"
#
#             row_dict["GOLD sample identifiers"] = [f"gold:{current_lookup['biosampleGoldId']}"]
#
#             # todo it doesn't look like IMG taxon identifiers are available for all of the GOLD biosample entries
#             alternative_identifiers = [id_list.pop()]
#             img_genome_aka_taxon_key = 'IMG Genome ID '
#             img_genome_aka_taxon_value = current_lookup[img_genome_aka_taxon_key]
#             if img_genome_aka_taxon_value:
#                 alternative_identifiers.append(f"img.taxon:{img_genome_aka_taxon_value}")
#             row_dict["alternative identifiers"] = alternative_identifiers
#
#             rows_list.append(row_dict)
#         else:
#             row_dict["id"] = id_list.pop()
#             row_dict["sample_link"] = static_project_id
#             rows_list.append(row_dict)
#
#         # todo advantages and disadvantages?
#         row_dict["type"] = "nmdc:Biosample"
#
#     # https://www.ncbi.nlm.nih.gov/biosample/8902828
#
#     for i in rows_list:
#         logger.debug(pprint.pformat(i))
#
#     sandbox.sample_metadata_by_slotname = {static_project_id: rows_list}
#
#     # todo monitor INSDC_biosample_identifiers
#     #  None → 0..* ExternalIdentifier (which is a type with root URIorCURIE and representation str)
#     #    currently passing a scalar, which is silently repaired to a list at instantiation time?
#     sandbox.deep_parse_sample_metadata(sample_database_file=sample_metadata_yaml_file)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env-file",
    default="local/.env",
)
@click.option(
    "--biosample-sql-file",
    type=click.Path(exists=True),
    required=True,
)
# todo this should relly be called biosample accession file
@click.option(
    "--biosample-id-file",
    type=click.Path(exists=True),
    required=True,
    help="Biosample IDs, like SAMN00000002, one per line with no header, no prefixes and no quotes",
)
@click.option(
    "--sqlite-to-biosample-file",
    type=click.Path(exists=True),
    required=True,
    help="""mappings between BBOP's SQLite version of the NCBI biosmaple data and NMDC Biosample slots
    sqlite	action	nmdc-schema-v3-plus-v6	format	notes""",
)
@click.option(
    "--static-project-id",
    required=True,
)
@click.option(
    "--sample-metadata-yaml-file",
    type=click.Path(),
    default="sample_metadata.yaml",
    # todo make a separate dumper methods for database of biosamples
    # help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
)
def pure_from_sqlite(
        biosample_id_file,
        biosample_sql_file,
        env_file,
        sample_metadata_yaml_file,
        sqlite_to_biosample_file,
        static_project_id,
):
    """Make biosample dicts by querying a biosmaple SQLite file with a list of biosample accessions."""

    # todo is this wrapper really necessary?
    load_vars_from_env_file(env_file)

    sandbox = SubmissionsSandbox()

    sandbox.view_setup(schemas_to_load=["nmdc"])

    rows_list = sandbox.get_biosamples_from_sqlite_by_accession(biosample_id_file=biosample_id_file,
                                                                biosample_sql_file=biosample_sql_file,
                                                                sqlite_to_biosample_file=sqlite_to_biosample_file,
                                                                static_project_id=static_project_id)

    sandbox.sample_metadata_by_slotname = {static_project_id: rows_list}

    # # todo monitor INSDC_biosample_identifiers
    # #  None → 0..* ExternalIdentifier (which is a type with root URIorCURIE and representation str)
    # #    currently passing a scalar, which is silently repaired to a list at instantiation time?

    logger.debug(f"""pre deep parse dict
    {pprint.pformat(sandbox.sample_metadata_by_slotname)}""")

    sandbox.deep_parse_sample_metadata()

    sandbox.sample_metadata_to_yaml(sample_metadata_yaml_file=sample_metadata_yaml_file)

    sample_metadata_json_file = sample_metadata_yaml_file.replace(".yaml", ".json")

    sandbox.sample_metadata_to_json(sample_metadata_json_file=sample_metadata_json_file)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env-file",
    default="local/.env",
    type=click.Path(exists=True),
)
@click.option(
    "--gold-mapping-file",
    required=True,
    type=click.Path(exists=True),
    help="TSV mapping form GOLD biosample fields to NMDC Biosample slots"
)
@click.option(
    "--gold-study-id",
    required=True,
    help="Prefixed GOLD study id, like..."
)
@click.option(
    "--sample-metadata-yaml-file",
    type=click.Path(),
    default="sample_metadata.yaml",
    # todo make a separate dumper methods for database of biosamples
    # help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
)
def pure_from_gold_study(
        env_file,
        gold_mapping_file,
        gold_study_id,
        sample_metadata_yaml_file,
):
    """"""
    load_dotenv(dotenv_path=env_file)
    sandbox = SubmissionsSandbox()
    sandbox.view_setup(schemas_to_load=["nmdc"])

    rows_list = sandbox.get_biosamples_from_gold_by_seq_proj(gold_study_id=gold_study_id,
                                                             gold_mapping_file=gold_mapping_file)

    sandbox.sample_metadata_by_slotname = {gold_study_id: rows_list}

    logger.debug(f"""pre deep parse dict
        {pprint.pformat(sandbox.sample_metadata_by_slotname)}""")

    sandbox.deep_parse_sample_metadata()

    sandbox.sample_metadata_to_yaml(sample_metadata_yaml_file=sample_metadata_yaml_file)

    sample_metadata_json_file = sample_metadata_yaml_file.replace(".yaml", ".json")

    sandbox.sample_metadata_to_json(sample_metadata_json_file=sample_metadata_json_file)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env-file",
    default="local/.env",
    type=click.Path(exists=True),
)
@click.option(
    "--gold-mapping-file",
    required=True,
    type=click.Path(exists=True),
    help="TSV mapping form GOLD biosample fields to NMDC Biosample slots"
)
@click.option(
    "--gold-study-id",
    required=True,
    help="Prefixed GOLD study id, like..."
)
@click.option(
    "--sample-metadata-yaml-file",
    type=click.Path(),
    default="sample_metadata.yaml",
    # todo make a separate dumper methods for database of biosamples
    # help="This is a relatively flat and study-indexed file, not directly suitable for the NMDC schema.",
)
@click.option(
    "--biosample-sql-file",
    type=click.Path(exists=True),
    required=True,
)
# todo this should really be called biosample accession file
@click.option(
    "--biosample-id-file",
    type=click.Path(exists=True),
    required=True,
    help="Biosample IDs, like SAMN00000002, one per line with no header, no prefixes and no quotes",
)
@click.option(
    "--sqlite-to-biosample-file",
    type=click.Path(exists=True),
    required=True,
    help="""mappings between BBOP's SQLite version of the NCBI biosmaple data and NMDC Biosample slots
    sqlite	action	nmdc-schema-v3-plus-v6	format	notes""",
)
@click.option(
    "--lookup_file",
    type=click.Path(exists=True),
    help="Provide a id lookup file in a known format if necessary. See also --lookup_style.",
)
@click.option(
    "--lookup_style",
    type=click.Choice(["mcafes_gold_lookup"]),
    help="Choose a pre-defined id lookup format. See also --lookup_file.", )
def sqlite_gold_hybrid(
        biosample_id_file,
        biosample_sql_file,
        env_file,
        gold_mapping_file,
        gold_study_id,
        lookup_file,
        lookup_style,
        sample_metadata_yaml_file,
        sqlite_to_biosample_file,

):
    load_dotenv(dotenv_path=env_file)

    lookup_dict = {}
    if lookup_file and lookup_style:
        if lookup_style == "mcafes_gold_lookup":

            # bootstrap sample details lookup dict from lookup file
            with open(lookup_file, encoding='utf16') as csvfile:
                reader = csv.DictReader(csvfile, delimiter='\t')
                for row in reader:
                    lookup_dict[row['Duplicated_NCBI_Biosample']] = row

    logger.debug(f"lookup_dict: {pprint.pformat(lookup_dict)}")

    sandbox = SubmissionsSandbox()
    sandbox.view_setup(schemas_to_load=["nmdc"])

    gold_rows_list = sandbox.get_biosamples_from_gold_by_seq_proj(gold_study_id=gold_study_id,
                                                                  gold_mapping_file=gold_mapping_file)

    gold_rows_dict = {row['id']: row for row in gold_rows_list}

    logger.debug(f"gold_rows_dict: {pprint.pformat(gold_rows_dict)}")

    sqlite_rows_list = sandbox.get_biosamples_from_sqlite_by_accession(biosample_id_file=biosample_id_file,
                                                                       biosample_sql_file=biosample_sql_file,
                                                                       sqlite_to_biosample_file=sqlite_to_biosample_file,
                                                                       static_project_id=gold_study_id)

    # todo
    #   gold vs ncbi id
    #   check units and scale on depth, elev, etc.
    #   remove env_package
    #   add nmdc identifier?
    #   sra identifier slot and prefix
    #   project prefix
    #   no slot for isolation source?
    #   community and location don't appear in GOLD Biosample API results

    hybrid_rows_list = []
    for sr in sqlite_rows_list:
        hybrid_row = sr.copy()
        direct_submission_prefixed = sr['id']
        direct_submission_bare = re.sub("BIOSAMPLE:", "", direct_submission_prefixed)
        logger.debug(f"sqlite biosample prefixed {direct_submission_prefixed} bare {direct_submission_bare}")
        if direct_submission_bare in lookup_dict:
            gold_submission_biosample_id_bare = lookup_dict[direct_submission_bare]['NBCI_Biosample']
            gold_submission_biosample_id_prefixed = f"BIOSAMPLE:{gold_submission_biosample_id_bare}"
            logger.debug(
                f"found lookup {gold_submission_biosample_id_bare} aka {gold_submission_biosample_id_prefixed} for {direct_submission_bare}")
            if gold_submission_biosample_id_prefixed in gold_rows_dict:
                gold_biosample = gold_rows_dict[gold_submission_biosample_id_prefixed]
                logger.debug(
                    f"from gold_rows_dict: {pprint.pformat(gold_biosample)}")
                for grk, grv in gold_biosample.items():
                    if grv:
                        hybrid_row[grk] = grv
                # todo this might be too project specific
                if lookup_file and lookup_style and lookup_style == "mcafes_gold_lookup":
                    hybrid_row['INSDC_biosample_identifiers'] = [sr['id']]
                    if "INSDC_secondary_sample_identifiers" in hybrid_row:
                        del hybrid_row['INSDC_secondary_sample_identifiers']
                    if "env_package" in hybrid_row:
                        del hybrid_row['env_package']
                    if "elev" in gold_biosample and gold_biosample["elev"]:
                        hybrid_row['elev'] = f"{gold_biosample['elev']} meters"
                    if "depth2" in gold_biosample and gold_biosample["depth2"]:
                        hybrid_row['depth2'] = f"{gold_biosample['depth2']} meters"
                    if 'GOLD_sample_identifiers' in gold_biosample:
                        logger.info(f"GOLD_sample_identifiers {gold_biosample['GOLD_sample_identifiers'][0]}")
                        hybrid_row['id'] = gold_biosample['GOLD_sample_identifiers'][0]
                    else:
                        logger.warning(f"no GOLD_sample_identifiers for {gold_submission_biosample_id_prefixed}")
            else:
                logger.warning(f"{gold_submission_biosample_id_prefixed} not found in gold_rows_dict")
        else:
            logger.warning(f"no lookup record for {direct_submission_bare}")
        hybrid_rows_list.append(hybrid_row)

    logger.debug(f"sqlite_rows_list[0] {pprint.pformat(sqlite_rows_list[0])}")

    logger.debug(f"hybrid_rows_list[0] {pprint.pformat(hybrid_rows_list[0])}")

    sandbox.sample_metadata_by_slotname = {gold_study_id: hybrid_rows_list}

    sandbox.deep_parse_sample_metadata()

    sandbox.sample_metadata_to_yaml(sample_metadata_yaml_file=sample_metadata_yaml_file)

    sample_metadata_json_file = sample_metadata_yaml_file.replace(".yaml", ".json")

    sandbox.sample_metadata_to_json(sample_metadata_json_file=sample_metadata_json_file)


cli.add_command(from_csv)
# cli.add_command(from_sqlite)
cli.add_command(from_submissions)
cli.add_command(pure_from_sqlite)
cli.add_command(pure_from_gold_study)
cli.add_command(sqlite_gold_hybrid)

if __name__ == "__main__":
    cli()
