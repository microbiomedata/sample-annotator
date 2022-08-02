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

# todo more tests, typing, docstrings, etc.

# todo: /Users/MAM/Library/Caches/pypoetry/virtualenvs/sample-annotator-G4hsqM_G-py3.9/lib/python3.9/site-packages/requests/__init__.py:109: RequestsDependencyWarning: urllib3 (1.26.9) or chardet (5.0.0)/charset_normalizer (2.0.12) doesn't match a supported version!
#   warnings.warn(
#  requests                      2.28.0

# todo switch from CSV to TSV

import csv
import logging
import os
import pprint
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

import click
import click_log
import yaml
from dotenv import load_dotenv
from linkml_runtime import SchemaView
from sphinx.util import requests

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


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


@dataclass
class SubmissionsToJson:
    url_suffix = "api/metadata_submission"
    study_metadata: Optional[Dict[str, Any]] = None
    sample_metadata: Optional[Dict[str, List[List[str]]]] = None
    tidied_sample_metadata: Optional[Dict[str, List[List[str]]]] = None
    final_headers: Optional[List[str]] = None
    api_url: Optional[str] = None
    session_cookie: Optional[str] = None
    # todo switch to w3id URLs
    nmdc_url: Optional[str] = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml"
    mixs_url: Optional[
        str] = "https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/model/schema/mixs.yaml"
    submission_schema_url: Optional[
        str] = "https://raw.githubusercontent.com/microbiomedata/sheets_and_friends/main/artifacts/nmdc_submission_schema.yaml"
    mixs_view: Optional[SchemaView] = None
    nmdc_view: Optional[SchemaView] = None
    submission_schema_view: Optional[SchemaView] = None
    submission_slot_alias_to_x: Optional[Dict[str, Dict[str, Any]]] = None
    submission_slot_key_to_x: Optional[Dict[str, Dict[str, Any]]] = None
    submission_slot_title_to_x: Optional[Dict[str, Dict[str, Any]]] = None

    def del_sample_data_from_result(self, result):
        if "metadata_submission" in result:
            metadata_submission = result["metadata_submission"]
            if "sampleData" in metadata_submission:
                for_counting = metadata_submission["sampleData"]
                del metadata_submission["sampleData"]
                if len(for_counting) > 2:
                    # significant_row_count = len(for_counting) - 2
                    result["sample_data_total_rows"] = len(for_counting)
                    result["metadata_submission"] = metadata_submission
                    return result
                else:
                    logger.info(f"{result['id']} has no sample data rows")
                    result["sample_data_total_rows"] = 0
                    return result
        else:
            logger.info(f"no metadata_submission for {result['id']}")
            result["sample_data_total_rows"] = 0
            return result

    def split_study_and_sample_metadata(self, results):
        study_metadata = {}
        sample_metadata = {}
        for result in results:
            result_id = result["id"]
            logger.debug(f"result_id: {result_id}")
            if "metadata_submission" in result:
                metadata_submission = result["metadata_submission"]
                if "sampleData" in metadata_submission:
                    sample_data_lol = metadata_submission["sampleData"]
                    sample_data_lod = self.make_sample_data_dict(sample_data_lol, result_id)
                    sample_metadata[result_id] = sample_data_lod
            without_sample_data = self.del_sample_data_from_result(result)
            if without_sample_data:
                study_metadata[result_id] = without_sample_data
        return study_metadata, sample_metadata

    def make_sample_data_dict(self, sample_data: List[List[str]], result_id: str
                              ) -> List[Dict[str, str]]:
        # todo may need to add study id
        #   reuse the UUID that Kitware assigns to id,
        #   or some study identifier from the metadata_submission.multiOmicsForm or metadata_submission.studyForm paths?
        #   NMDC sample to study relationship expressed with part_of?
        # todo may need to mint sample id
        sample_data_row_count = len(sample_data)
        if result_id and sample_data_row_count > 2:
            # todo make this test more flexible
            # if sample_data[1][0] != "globally unique ID":
            if sample_data[1][0] != "globally unique ID":
                logger.info(f"{result_id} doesn't seem to have the expected header rows: {sample_data[0:3]}")
                # todo try to infer columns from template?
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
                    row_dict["part_of"] = result_id
                    body_list.append(row_dict)

                return body_list
        else:
            return [{}]

    def view_setup(self):
        logger.info(f"Trying to load a view from {self.mixs_url}")
        self.mixs_view = SchemaView(self.mixs_url)
        logger.info(f"Loaded {self.mixs_view.schema.name}")
        logger.info(f"Trying to load a view from {self.nmdc_url}")
        self.nmdc_view = SchemaView(self.nmdc_url)
        logger.info(f"Loaded {self.nmdc_view.schema.name}")
        logger.info(f"Trying to load a view from {self.submission_schema_url}")
        self.submission_schema_view = SchemaView(self.submission_schema_url)
        logger.info(f"Loaded {self.submission_schema_view.schema.name}")

    def get_submission_titles_and_names(self):
        submission_slots = self.submission_schema_view.schema.slots
        self.submission_slot_title_to_x = {v.title: {"title": v.title, "key": k, "alias": v.alias} for k, v in
                                           submission_slots.items() if v.title}
        self.submission_slot_key_to_x = {k: {"title": v.title, "key": k, "alias": v.alias} for k, v in
                                         submission_slots.items()}
        self.submission_slot_alias_to_x = {v.alias: {"title": v.title, "key": k, "alias": v.alias} for k, v in
                                           submission_slots.items() if v.alias}
        # logger.info(pprint.pformat(submission_slot_title_to_x))

    def get_api_url(self) -> str:
        if self.api_url:
            return self.api_url
        else:
            return os.getenv("api_url")

    def get_final_headers(self, acceptable_statuses, suggested_initial_columns):
        all_headers = set()
        for k, v in self.sample_metadata.items():
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
        all_headers.sort()
        remainder_headers = set(all_headers) - set(suggested_initial_columns)
        remainder_headers = list(remainder_headers)
        remainder_headers.sort()
        self.final_headers = list(suggested_initial_columns) + remainder_headers
        logger.debug(f"final_headers: {self.final_headers}")

    def tidy_sample_metadata(self, acceptable_statuses):
        tidied_dict = {}
        for k, raw_lod in self.sample_metadata.items():
            tidied_lod = []
            logger.debug(f"k: {k}")
            current_status = None
            if k in self.study_metadata:
                current_status = self.study_metadata[k]["status"]
            if current_status in acceptable_statuses:
                for row in raw_lod:
                    if row:
                        logger.debug(f"row: {row}")
                        filtered_row = {
                            k: v for k, v in row.items() if k in self.final_headers
                        }
                        logger.debug(f"filtered_row: {filtered_row}")
                        tidied_lod.append(filtered_row)
            if tidied_lod:
                tidied_dict[k] = tidied_lod
            logger.debug(f"tidied_dict: {tidied_dict}")
        if tidied_dict:
            self.tidied_sample_metadata = tidied_dict

    def sample_metadata_to_csv(self, sample_metadata_csv_file):

        with open(sample_metadata_csv_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.final_headers)
            writer.writeheader()
            if self.tidied_sample_metadata:
                for k, v in self.tidied_sample_metadata.items():
                    writer.writerows(v)

    def study_metadata_to_yaml(self, study_metadata_yaml_file):
        with open(study_metadata_yaml_file, "w") as outfile:
            yaml.dump(self.study_metadata, outfile, default_flow_style=False)

    def get_one_submission_page_from_api(self, start, stop):
        logger.debug(f"session_cookie: {self.session_cookie}")
        logger.info(f"api_url: {self.api_url}")
        metadata_api_url = f"{self.api_url}{self.url_suffix}"
        logger.info(f"whole_url: {metadata_api_url}")
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

        self.study_metadata, self.sample_metadata = self.split_study_and_sample_metadata(
            results
        )


# todo try @click.group()
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
)
@click.option("--page_start", default=0)
@click.option("--page_stop", default=999)
@click.option(
    "--study_metadata_yaml_file", type=click.Path(), default="study_metadata.yaml"
)
@click.option(
    "--sample_metadata_csv_file", type=click.Path(), default="sample_metadata.csv"
)
@click.option("--data_csv", type=click.Path(exists=True))
@click.option("--csv_proj_id")
@click.option(
    "--acceptable_statuses",
    type=click.Choice(["complete"]),
    default=["complete"],
    multiple=True,
)
@click.option(
    "--suggested_initial_columns",
    type=click.Choice(["sample name", "globally unique ID", "part_of"]),
    default=["sample name", "globally unique ID", "part_of"],
    multiple=True,
)
def cli(
        env_file,
        data_portal_url,
        study_metadata_yaml_file,
        sample_metadata_csv_file,
        page_start,
        page_stop,
        acceptable_statuses,
        suggested_initial_columns,
        data_csv,
        csv_proj_id,
):
    """
    CLI for converting a csv file or the response from the NMDC metadata_submission API to a JSON file,
    validatable by the NMDC schema.
    """

    load_vars_from_env_file(env_file)

    if data_portal_url:
        if data_csv:
            raise Exception("don't provide a data_csv when providing a data_portal_url")
        else:
            s2j = SubmissionsToJson(
                api_url=data_portal_url, session_cookie=get_session_cookie()
            )

            s2j.view_setup()

            s2j.get_submission_titles_and_names()

            s2j.get_one_submission_page_from_api(start=page_start, stop=page_stop)

            s2j.get_final_headers(
                acceptable_statuses=acceptable_statuses,
                suggested_initial_columns=suggested_initial_columns,
            )

            s2j.tidy_sample_metadata(acceptable_statuses=acceptable_statuses)

            s2j.sample_metadata_to_csv(
                sample_metadata_csv_file=sample_metadata_csv_file
            )

            s2j.study_metadata_to_yaml(
                study_metadata_yaml_file=study_metadata_yaml_file
            )
    elif data_csv:
        if data_portal_url:
            raise Exception("don't provide a data_portal_url when providing a data_csv")
        if csv_proj_id:
            with open(data_csv, newline="") as csvfile:
                rows = []
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row:
                        row["part_of"] = csv_proj_id
                        rows.append(row)
            s2j = SubmissionsToJson()
            s2j.sample_metadata = {csv_proj_id: rows}
            s2j.study_metadata = {csv_proj_id: {"status": "complete"}}
            s2j.get_final_headers(
                acceptable_statuses=acceptable_statuses,
                suggested_initial_columns=suggested_initial_columns,
            )
            s2j.tidy_sample_metadata(acceptable_statuses=acceptable_statuses)
            s2j.sample_metadata_to_csv(
                sample_metadata_csv_file=sample_metadata_csv_file
            )

        else:
            raise Exception("csv_proj_id is required with data_csv")
    else:
        raise Exception("must specify either data_portal_url or data_csv")


if __name__ == "__main__":
    cli()
