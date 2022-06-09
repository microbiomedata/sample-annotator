import pprint

import requests
import pandas as pd

import logging

import click
import click_log
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper, json_dumper

from nmdc_schema.nmdc import (
    Biosample,
    ControlledTermValue,
    QuantityValue,
    TextValue,
    TimestampValue,
    GeolocationValue,
    Database,
)

import yaml

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

pd.set_option("display.max_columns", None)


# todo: lots of hardcoded file names etc
submission_frame_filename = ""


# todo where does this warning come from?
#  mixs namespace is already mapped to https://w3id.org/mixs/terms/ - Mapping to https://w3id.org/gensc/ ignored


# todo add click help and better docstrings
#  turn the requests params into click options (with defaults)
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
    params = {"offset": 0, "limit": 99}

    # todo document
    #  login to the NMDC website (https://data.microbiomedata.org/ or https://data.dev.microbiomedata.org/)
    #  (requires an ORCID)
    #  get the content of the session cookie
    #  on chrome, chrome://settings/cookies/detail?site=data.dev.microbiomedata.org
    #  save that into local/SESSION_COOKIE.txt
    #  look out for a rj of {'detail': 'Login required'}
    response = requests.get(url, cookies=cookies, params=params)

    rj = response.json()

    # print(rj.keys())
    # # dict_keys(['count', 'results'])

    # total_submissions = rj["count"]
    # print(f"submission count: {total_submissions}")

    submissions_list = rj["results"]

    # print(submissions_list[0].keys())
    # # dict_keys(['metadata_submission', 'status', 'id', 'author_orcid', 'created'])

    # external_keys = ["status", "id", "author_orcid", "created"]
    inner_key = "metadata_submission"

    submission_lol = []
    metadata_dict = {}
    for i in submissions_list:
        col_count = None
        if len(i[inner_key]["sampleData"]) > 0:
            col_count = len(i[inner_key]["sampleData"][0])
        submission_dict = {
            "id": i["id"],
            "author_orcid": i["author_orcid"],
            "created": i["created"],
            "status": i["status"],
            # "template": i[inner_key]["template"],
            "rows": len(i[inner_key]["sampleData"]),
            "cols": col_count,
        }
        submission_dict.update(i[inner_key]["studyForm"])
        submission_dict.update(i[inner_key]["multiOmicsForm"])
        submission_lol.append(submission_dict)
        # metadata_dict[i["id"]] = {
        #     "template": i[inner_key]["template"],
        #     "lol": i[inner_key]["sampleData"],
        # }
    df = pd.DataFrame(submission_lol)

    df.to_csv("submission_frame.tsv", sep="\t", index=False)

    exit()

    # # print(submissions_list[0][inner_key].keys())
    # # # ['template', 'studyForm', 'sampleData', 'multiOmicsForm'
    #
    # # print(submissions_list[0][inner_key]['sampleData'])
    # # # list of lists

    nmdc_dh_view = get_schema_view(
        schema_source="https://microbiomedata.github.io/sheets_and_friends/template/nmdc_dh/source/nmdc_dh.yaml"
    )

    nmdc_view = get_schema_view(
        schema_source="https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml"
    )

    mixs_view = get_schema_view(
        schema_source="https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/model/schema/mixs.yaml"
    )

    known_templates = get_known_templates()

    # david sparse 33d31996-171a-4fdf-b2ea-d3936b649529
    # pajau 822e290d-6837-4956-abb9-996dd5f6d8b9

    bs_db, instantiation_log = lol_to_validatable(
        metadata_dict=metadata_dict,
        study_id="822e290d-6837-4956-abb9-996dd5f6d8b9",
        dh_view=nmdc_dh_view,
        mixs_view=mixs_view,
        nmdc_view=nmdc_view,
        known_templates=known_templates,
    )

    with open("instantiation_log.yml", "w") as outfile:
        yaml.dump(instantiation_log, outfile, default_flow_style=False)

    # print(yaml_dumper.dumps(bs_db))
    json_dumper.dump(element=bs_db, to_file="bs_db.json")


def get_schema_view(schema_source: str):
    schema_view = SchemaView(schema_source)
    return schema_view


def get_col_order(view: SchemaView, selected_class_name: str):
    # are induced slots too much here? in what way?
    cis = view.class_induced_slots(selected_class_name)
    lod = []
    for i in cis:
        lod.append({"slot": i.alias, "column_rank": i.rank, "slot_group": i.slot_group})
    slot_df = pd.DataFrame(lod)
    sg_set = set(slot_df["slot_group"])
    lod = []
    for i in sg_set:
        slot_obj = view.get_slot(i)
        lod.append({"slot_group": i, "section_rank": slot_obj["rank"]})
    sg_df = pd.DataFrame(lod)
    final_frame = slot_df.merge(right=sg_df)
    final_frame.sort_values(by=["section_rank", "column_rank"], inplace=True)
    return list(final_frame["slot"])


def lol_to_validatable(
    metadata_dict,
    study_id: str,
    dh_view: SchemaView,
    mixs_view: SchemaView,
    nmdc_view: SchemaView,
    known_templates,
):
    # -> Database
    re_mappings = {
        "samp_name": "name",
        "soil_horizon": "horizon",
        "prev_land_use_meth": "previous_land_use_meth",
        "samp_collec_device": "samp_collect_device",
    }
    claimed_template = metadata_dict[study_id]["template"]
    lol = metadata_dict[study_id]["lol"]
    df = pd.DataFrame(lol)
    # todo "template" is really just the environment
    # todo also need to include omicsProcessingTypes
    #  which is a list and will require some additional logic
    #  maybe all of that should be modeled in teh DH schema?
    #
    # todo for testing with
    #  study_id="33d31996-171a-4fdf-b2ea-d3936b649529" only
    #
    # todo may not even be able to trust this is the environment or omicsProcessingTypes
    #  were changed after data was started?
    #  study_id="33d31996-171a-4fdf-b2ea-d3936b649529" says omicsProcessingTypes = []
    #  but it sure looks like soil_emsl_jgi_mg
    if study_id in known_templates:
        bis = nmdc_view.class_induced_slots("biosample")
        bis_names = [i.alias for i in bis]
        bis_dict = dict(zip(bis_names, bis))
        known_template = known_templates[study_id]
        col_order = get_col_order(view=dh_view, selected_class_name=known_template)
        df.columns = col_order
        lod = df.to_dict(orient="records")
        unmapped = set()
        string_slots = set()
        other_ranges = {}
        bs_db = Database()
        for i in lod:
            # # doesn't require any particular id prefix?
            # # part of is supposed to take a named thing
            # # ControlledTermValue can be instantiated empty
            # # ControlledTermValue -> term is supposed to take an ontology class
            # todo strip leading underscores from MIxS env triad EnvO terms and parse label from ID
            # gold_path_field... generalize
            # ecosystem
            # ecosystem_category
            # ecosystem_subtype
            # ecosystem_type
            # specific_ecosystem

            # requireds
            #     # not doing anything special for multivalued slots yet
            #     # id vs source_mat_id
            #     # what value would be best for part_of?
            bs_attempt = Biosample(
                id=i["source_mat_id"],
                part_of=[study_id],
                env_broad_scale=ControlledTermValue(has_raw_value=i["env_broad_scale"]),
                env_local_scale=ControlledTermValue(has_raw_value=i["env_local_scale"]),
                env_medium=ControlledTermValue(has_raw_value=i["env_medium"]),
            )
            for k, v in i.items():
                # expected_key = None
                if k in re_mappings:
                    expected_key = re_mappings[k]
                else:
                    expected_key = k
                if expected_key in bis_dict:
                    current_range = bis_dict[expected_key].range
                    if current_range == "controlled term value":
                        bs_attempt[expected_key] = ControlledTermValue(has_raw_value=v)
                    elif current_range == "geolocation value":
                        bs_attempt[expected_key] = GeolocationValue(has_raw_value=v)
                    elif current_range == "quantity value":
                        bs_attempt[expected_key] = QuantityValue(has_raw_value=v)
                    elif current_range == "text value":
                        bs_attempt[expected_key] = TextValue(has_raw_value=v)
                    elif current_range == "timestamp value":
                        bs_attempt[expected_key] = TimestampValue(has_raw_value=v)
                    elif current_range == "string":
                        bs_attempt[expected_key] = v
                        string_slots.add(expected_key)
                    else:
                        other_ranges[expected_key] = current_range
                else:
                    unmapped.add(k)
            bs_db.biosample_set.append(bs_attempt)

        string_slots = set_to_list(string_slots)

        mixs_slots = mixs_view.all_slots()
        mixs_slot_names = list(mixs_slots.keys())
        mixs_defines = unmapped.intersection(set(mixs_slot_names))
        dh_defines = unmapped - mixs_defines

        dh_defines = set_to_list(dh_defines)
        mixs_defines = set_to_list(mixs_defines)

        instantiation_log = {
            "template": known_template,
            "string_slots": string_slots,
            "other_ranges": other_ranges,
            "mixs_defines": mixs_defines,
            "dh_defines": dh_defines,
        }

        return bs_db, instantiation_log


def set_to_list(set_input, do_sort=True):
    temp = list(set_input)
    if do_sort:
        temp.sort()
    return temp


def get_known_orcids():
    # todo there's probably a better place for this
    #  even an inline dict?
    known_orcids = pd.read_csv("../../../known_orcids.tsv", sep="\t")
    # return a dict instead?
    return known_orcids


def get_known_templates():
    # todo there's probably a better place for this
    #  even an inline dict?
    known_templates = pd.read_csv("known_templates.tsv", sep="\t")
    # return a dict instead?
    temp = dict(zip(known_templates.iloc[:, 0], known_templates.iloc[:, 1]))
    return temp


if __name__ == "__main__":
    cli()
