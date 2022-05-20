import requests
import pandas as pd

import logging

import click
import click_log

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option("--session_cookie", required=True)
def cli(session_cookie: str):
    """
    :param session_cookie:
    :return:
    """

    url = "https://data.dev.microbiomedata.org/api/metadata_submission"

    cookies = {"session": session_cookie}
    params = {"offset": 0, "limit": 3}

    response = requests.get(url, cookies=cookies)
    rj = response.json()

    # print(rj.keys())
    # # dict_keys(['count', 'results'])

    total_submissions = rj["count"]
    submissions_list = rj["results"]

    # print(submissions_list[0].keys())
    # # dict_keys(['metadata_submission', 'status', 'id', 'author_orcid', 'created'])

    external_keys = ["status", "id", "author_orcid", "created"]
    inner_key = "metadata_submission"

    # print(submissions_list[0][inner_key].keys())
    # # ['template', 'studyForm', 'sampleData', 'multiOmicsForm'

    # print(submissions_list[0][inner_key]['sampleData'])
    # # list of lists

    df = pd.DataFrame(submissions_list[0][inner_key]["sampleData"])

    print(df)

    # for i in


if __name__ == '__main__':
    cli()
