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

# todo more typing, docstrings, etc.

# todo: /Users/MAM/Library/Caches/pypoetry/virtualenvs/sample-annotator-G4hsqM_G-py3.9/lib/python3.9/site-packages/requests/__init__.py:109: RequestsDependencyWarning: urllib3 (1.26.9) or chardet (5.0.0)/charset_normalizer (2.0.12) doesn't match a supported version!
#   warnings.warn(
#  requests                      2.28.0

import os
import pprint
from typing import Dict, List

import click
from dataclasses import dataclass
import logging

import click_log

from dotenv import load_dotenv
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


# this could be for making the data more readable during development,
#   but maybe we shouldn't use it during production?
def del_sample_data_from_result(result):
    if "metadata_submission" in result:
        metadata_submission = result["metadata_submission"]
        if "sampleData" in metadata_submission:
            for_counting = metadata_submission["sampleData"]
            del metadata_submission["sampleData"]
            if len(for_counting) > 2:
                # significant_row_count = len(for_counting) - 2
                metadata_submission["sample_data_data_rows"] = len(for_counting) - 2
                result["metadata_submission"] = metadata_submission
                return result


def make_sample_data_dict(sample_data: List[List[str]]) -> List[Dict[str, str]]:
    # todo may need to add study id
    #   reuse the UUID that kitware assigns to id,
    #   or some study identifier from the metadata_submission.multiOmicsForm or metadata_submission.studyForm paths?
    #   NMDC sample to study relationship expressed with part_of?
    # todo may need to mint sample id
    sample_data_row_count = len(sample_data)
    sample_data_headers = sample_data[1]
    sample_data_body = sample_data[2:sample_data_row_count]
    # # sample_data_pretty = pprint.pformat(sample_data)
    logger.debug(f"sample_data_row_count: {sample_data_row_count}")
    logger.debug(f"discarding sample_data_row_count row 0: {sample_data[0]}")
    logger.debug(f"sample_data_headers: {sample_data_headers}")
    # todo the sample_data_headers values are term titles, not names
    #  lookup with the latest version for now,
    #  but work on better recording and reporting of schema version
    body_list = []
    for body_row in sample_data_body:
        row_dict = dict(zip(sample_data_headers, body_row))
        body_list.append(row_dict)

    return body_list


@dataclass
class SubmissionsToJson:
    api_url: str
    session_cookie: str
    url_suffix = "api/metadata_submission"

    def get_one_submission_from_api(self):
        logger.debug(f"session_cookie: {self.session_cookie}")
        logger.info(f"api_url: {self.api_url}")
        metadata_api_url = f"{self.api_url}{self.url_suffix}"
        logger.info(f"whole_url: {metadata_api_url}")

        # hardcoded for getting one interesting submission
        params = {"offset": 1, "limit": 1}
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

        meta_data_submission_bodies = []
        # we know the list is going to have a length of one
        for local_res_count, result in enumerate(results):
            logger.debug(f"local_res_count: {local_res_count}, result: {result}")
            metadata_submission = get_metadata_submission_from_result(result)
            logger.debug(f"metadata_submission: {metadata_submission}")
            sample_data = get_sample_data_from_metadata_submission(metadata_submission)
            logger.debug(f"sample_data: {sample_data}")

            body_list = make_sample_data_dict(sample_data)
            meta_data_submission_bodies.append(body_list)

            result_no_sample_data = del_sample_data_from_result(result)
            logger.info(
                f"result_no_sample_data: {pprint.pformat(result_no_sample_data)}"
            )

        return meta_data_submission_bodies


@click.command()
# todo make this an either-or between
#  https://data.dev.microbiomedata.org/
#  OR
#  https://data.microbiomedata.org/
@click_log.simple_verbosity_option(logger)
@click.option(
    "--env_file",
    default="local/.env",
)
@click.option(
    "--data_portal_url",
    default="https://data.microbiomedata.org/",
)
def cli(env_file, data_portal_url):
    """
    CLI for converting a TSV file or the response from the NMDC metadata_submission API to a JSON file,
    validatable by the NMDC schema.
    """

    load_vars_from_env_file(env_file)

    s2j = SubmissionsToJson(
        api_url=data_portal_url, session_cookie=get_session_cookie()
    )

    meta_data_submission_bodies = s2j.get_one_submission_from_api()

    for body in meta_data_submission_bodies:
        body_pretty = pprint.pformat(body)
        logger.info(f"body_pretty: {body_pretty}")


if __name__ == "__main__":
    cli()
