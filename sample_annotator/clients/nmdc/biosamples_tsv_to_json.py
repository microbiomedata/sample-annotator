import click
import pandas as pd
from linkml_runtime.dumpers import yaml_dumper

from sample_annotator.clients.nmdc.submissions_as_studies import (
    just_metadata_rows,
    get_view,
    get_template_titles_names,
    RuntimeApiSiteClient,
)


@click.command()
@click.option("--csv_in", required=True)
@click.option("--yaml_out", required=True)
@click.option("--asserted_template", required=True)
@click.option("--asserted_study", required=True)
def cli(csv_in: str, yaml_out: str, asserted_template: str, asserted_study: str):
    """
    :param csv_in:
    :param yaml_out:
    :param asserted_template:
    :param asserted_study:
    :return:
    """
    sample_data_frame = pd.read_csv(csv_in, sep=",")

    portal_view = get_view()

    current_title_to_name_frame = get_template_titles_names(
        asserted_template, portal_view
    )

    expected = list(current_title_to_name_frame["title"])

    provided = sample_data_frame.columns.tolist()

    if expected != provided:
        print("NOT OK")
        print(expected)
        print(provided)
        exit()
    else:
        sample_data_frame["part_of"] = asserted_study

    mintingClient = RuntimeApiSiteClient(
        base_url="https://api.dev.microbiomedata.org",
        site_id="mam_lbl_2019mbp_nobs",
        client_id="sys0acx2cb96",
        client_secret="w@sk23X?Ea7.",
    )

    minting_params = {
        "populator": "",
        "naa": "nmdc",
        "shoulder": "fk0",
        "number": len(sample_data_frame.index),
    }

    minting_response = mintingClient.request(
        "POST", "/ids/mint", params_or_json_data=minting_params
    )

    sample_data_frame["id"] = minting_response.json()

    print(sample_data_frame)


if __name__ == "__main__":
    cli()
