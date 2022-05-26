import pprint
import re

import requests
import pandas as pd

import logging

import click
import click_log
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper, json_dumper
from linkml_runtime.linkml_model import EnumDefinition

from nmdc_schema.nmdc import (
    Biosample,
    ControlledTermValue,
    QuantityValue,
    TextValue,
    TimestampValue,
    GeolocationValue,
    Database,
    OntologyClass,
)

import yaml
from quantulum3 import parser

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

pd.set_option("display.max_columns", None)

dh_to_nmdc_name_mappings = {
    "samp_name": "name",
    "soil_horizon": "horizon",
    "prev_land_use_meth": "previous_land_use_meth",
    "samp_collec_device": "samp_collect_device",
}


# todo repairs
#  TextValue
#    none? leave as has raw value?
#  TimestampValue
#    doesn't actually have any structured slots!

# todo could include sample data sparseness in submission frame

# todo requesting hardcoded 99 pages of submissions
#   1) at some point there will be more and pages will need to be joined
#   2) could there be so many that they shouldn't be joined into one huge in memory object?

# todo: lots of hardcoded file names etc

# todo where does this warning come from?
#  mixs namespace is already mapped to https://w3id.org/mixs/terms/ - Mapping to https://w3id.org/gensc/ ignored

# todo add click help and better docstrings
#  turn the requests params into click options (with defaults)
@click.command()
@click_log.simple_verbosity_option(logger)
@click.option("--session_cookie", required=True)
@click.option("--study_id", required=True)
@click.option("--data_out", default="assets/out/biosample_collection.json")
@click.option("--known_template_tsv", default="assets/in/known_templates.tsv")
@click.option("--known_orcids_tsv", default="assets/in/known_orcids.tsv")
@click.option("--instantiation_log_yaml", default="assets/out/instantiation_log.yaml")
@click.option("--submission_frame_tsv", default="assets/out/submission_frame.tsv")
@click.option(
    "--api_url", default="https://data.dev.microbiomedata.org/api/metadata_submission"
)
@click.option("--api_offset", default=0)
@click.option("--api_limit", default=99)
@click.option(
    "--single_study_sample_metadata_tsv",
    default="assets/out/single_study_sample_metadata.tsv",
)
def cli(
    session_cookie: str,
    study_id: str,
    data_out: str,
    known_template_tsv: str,
    known_orcids_tsv: str,
    instantiation_log_yaml: str,
    submission_frame_tsv: str,
    api_url: str,
    api_offset: str,
    api_limit: str,
    single_study_sample_metadata_tsv: str,
):
    """
    :param session_cookie:
    :param study_id:
    :param data_out:
    :param known_template_tsv:
    :param known_orcids_tsv:
    :param instantiation_log_yaml:
    :param submission_frame_tsv:
    :param api_url:
    :param api_offset:
    :param api_limit:
    :param single_study_sample_metadata_tsv:
    :return:
    """

    nmdc_submissions_view = get_schema_view(
        schema_source="https://microbiomedata.github.io/sheets_and_friends/template/nmdc_dh/source/nmdc_dh.yaml"
    )

    # https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml
    # https://raw.githubusercontent.com/microbiomedata/nmdc-schema/issue-317-dh-non-mixs/src/schema/nmdc.yaml
    nmdc_view = get_schema_view(
        schema_source="https://raw.githubusercontent.com/microbiomedata/nmdc-schema/issue-317-dh-non-mixs/src/schema/nmdc.yaml"
    )

    mixs_view = get_schema_view(
        schema_source="https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/model/schema/mixs.yaml"
    )

    submissions_frame, metadata_dict = get_study_and_samples(
        url=api_url,
        params={"offset": api_offset, "limit": api_limit},
        cookies={"session": session_cookie},
        known_orcids_tsv=known_orcids_tsv,
    )

    submissions_frame.to_csv(submission_frame_tsv, sep="\t", index=False)

    bs_db, instantiation_log, single_study_sample_metadata = lol_to_validatable(
        metadata_dict=metadata_dict,
        study_id=study_id,
        dh_view=nmdc_submissions_view,
        mixs_view=mixs_view,
        nmdc_view=nmdc_view,
        known_template_tsv=known_template_tsv,
    )

    with open(instantiation_log_yaml, "w") as outfile:
        yaml.dump(instantiation_log, outfile, default_flow_style=False)

    json_dumper.dump(element=bs_db, to_file=data_out)

    bsm_sparsity = single_study_sample_metadata.isna().mean().mul(100).mean().round()

    logger.info(f"Study {study_id}'s biosample metadata is {bsm_sparsity}% sparse")

    single_study_sample_metadata.to_csv(
        single_study_sample_metadata_tsv, sep="\t", index=False
    )


def get_study_and_samples(url, params, cookies, known_orcids_tsv):
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

    # # print(submissions_list[0][inner_key].keys())
    # # # ['template', 'studyForm', 'sampleData', 'multiOmicsForm'
    #
    # # print(submissions_list[0][inner_key]['sampleData'])
    # # # list of lists

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
            "template": i[inner_key]["template"],
            "rows": len(i[inner_key]["sampleData"]),
            "cols": col_count,
        }
        submission_dict.update(i[inner_key]["studyForm"])
        submission_dict.update(i[inner_key]["multiOmicsForm"])
        submission_lol.append(submission_dict)
        metadata_dict[i["id"]] = {
            "template": i[inner_key]["template"],
            "lol": i[inner_key]["sampleData"],
        }
    df = pd.DataFrame(submission_lol)

    known_orcids_frame = get_known_orcids(known_orcids_tsv=known_orcids_tsv)

    df = df.merge(
        right=known_orcids_frame, how="left", left_on="author_orcid", right_on="orcid"
    )

    return df, metadata_dict


def process_qv(raw_value: str):
    # todo more than one qv get parsed out?
    # todo units are expressed as words. convert to symbols?
    # todo units my be surprising, like Coulombs for degrees Celsius
    qv = QuantityValue(has_raw_value=raw_value)
    quants = parser.parse(raw_value)
    if len(quants) > 0:
        quant = quants[0]
        if quant.uncertainty:
            qv.has_minimum_numeric_value = quant.value - quant.uncertainty
            qv.has_maximum_numeric_value = quant.value + quant.uncertainty
        else:
            qv.has_numeric_value = quant.value
        if quant.unit and quant.unit.name != "dimensionless":
            qv.has_unit = quant.unit.name
    return qv


def extract_lat_lon(raw_value: str):
    # if DH validation worked,
    # should be one decimal value, then a single whitespace, then another decimal value
    splitted = raw_value.split(" ")
    decimals = [float(i) for i in splitted]
    if (
        len(decimals) == 2
        and decimals[0] > -90
        and decimals[0] < 90
        and decimals[1] > -180
        and decimals[1] < 180
    ):
        gv = GeolocationValue(
            has_raw_value=raw_value, latitude=decimals[0], longitude=decimals[1]
        )
        return gv


def extract_ctv(raw_value: str):
    # ____mediterranean shrubland biome [ENVO:01000217]
    # todo could be more than one term id
    #  pipe or semicolon separated?
    #  or just one big mess?
    #  in any case, could be label only, id only, matching label and id, mismatch...
    #  check ontology owner to see if term is still active and label/id match?
    underscoreless = re.sub(pattern=r"^_*\s*", repl="", string=raw_value)
    p = re.compile(r"\[(.*)\]")
    term_id = p.findall(underscoreless)
    if term_id:
        label = underscoreless.replace(f"[{term_id[0]}]", "")
        label = label.strip()
        oc = OntologyClass(id=term_id[0], name=label)
        return oc


def get_schema_view(schema_source: str):
    schema_view = SchemaView(schema_source)
    return schema_view


def get_known_col_names(view: SchemaView, selected_class_name: str):
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
    known_template_tsv,
):
    known_templates = get_known_templates(known_template_tsv)
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
        bs_induced_slots = nmdc_view.class_induced_slots("biosample")
        bs_induced_slot_names = [i.alias for i in bs_induced_slots]
        # could have been a dict comprehension one-liner
        bs_induced_slot_dict = dict(zip(bs_induced_slot_names, bs_induced_slots))
        known_template = known_templates[study_id]
        col_order = get_known_col_names(view=dh_view, selected_class_name=known_template)
        df.columns = col_order
        biosample_list = df.to_dict(orient="records")
        unmapped = set()
        string_slots = set()
        other_ranges = {}
        bs_db = Database()
        for current_biosample in biosample_list:
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
            # todo the MIxS env triad terms are probably getting overwritten in the loop below
            instantiated_bs = Biosample(
                id=current_biosample["source_mat_id"],
                part_of=[study_id],
                env_broad_scale=ControlledTermValue(
                    has_raw_value=current_biosample["env_broad_scale"],
                    term=extract_ctv(current_biosample["env_broad_scale"]),
                ),
                env_local_scale=ControlledTermValue(
                    has_raw_value=current_biosample["env_local_scale"],
                    term=extract_ctv(current_biosample["env_local_scale"]),
                ),
                env_medium=ControlledTermValue(
                    has_raw_value=current_biosample["env_medium"],
                    term=extract_ctv(current_biosample["env_medium"]),
                ),
            )
            for k, v in current_biosample.items():
                # expected_key = None
                if k in dh_to_nmdc_name_mappings:
                    expected_key = dh_to_nmdc_name_mappings[k]
                else:
                    expected_key = k
                if expected_key in bs_induced_slot_dict:
                    current_range = bs_induced_slot_dict[expected_key].range
                    if current_range == ControlledTermValue.class_name:
                        if v:
                            oc = extract_ctv(v)
                        else:
                            oc = None
                        instantiated_bs[expected_key] = ControlledTermValue(
                            has_raw_value=v, term=oc
                        )
                    elif current_range == GeolocationValue.class_name:
                        gv = extract_lat_lon(v)
                        if gv:
                            instantiated_bs[expected_key] = gv
                        else:
                            instantiated_bs[expected_key] = GeolocationValue(
                                has_raw_value=v
                            )
                    elif current_range == QuantityValue.class_name:
                        if v:
                            qv = process_qv(v)
                            instantiated_bs[expected_key] = qv
                    elif current_range == TextValue.class_name:
                        instantiated_bs[expected_key] = TextValue(has_raw_value=v)
                    elif current_range == TimestampValue.class_name:
                        instantiated_bs[expected_key] = TimestampValue(has_raw_value=v)
                    elif (
                        type(dh_view.get_element(current_range)).class_name
                        == EnumDefinition.class_name
                    ):
                        instantiated_bs[expected_key] = v
                    elif current_range == "string":
                        # todo note if the string slot comes from EMSL or JGI... those are expected
                        instantiated_bs[expected_key] = v
                        string_slots.add(expected_key)
                    else:
                        range_element = dh_view.get_element(current_range)
                        other_ranges[expected_key] = type(range_element).class_name
                else:
                    unmapped.add(k)
            bs_db.biosample_set.append(instantiated_bs)

        string_slots = set_to_list(string_slots)

        nmdc_slots = nmdc_view.all_slots()
        nmdc_slot_names = list(nmdc_slots.keys())

        mixs_slots = mixs_view.all_slots()
        mixs_slot_names = list(mixs_slots.keys())

        mixs_defines = unmapped.intersection(set(mixs_slot_names))
        nmdc_includes = unmapped.intersection(set(nmdc_slot_names))
        dh_defines = unmapped - mixs_defines

        dh_defines = set_to_list(dh_defines)
        mixs_defines = set_to_list(mixs_defines)
        nmdc_includes = set_to_list(nmdc_includes)

        instantiation_log = {
            "template": known_template,
            "string_slots": string_slots,
            "other_ranges": other_ranges,
            "nmdc_includes": nmdc_includes,
            "mixs_defines": mixs_defines,
            "dh_defines": dh_defines,
        }

        return bs_db, instantiation_log, df


def set_to_list(set_input, do_sort=True):
    temp = list(set_input)
    if do_sort:
        temp.sort()
    return temp


def get_known_orcids(known_orcids_tsv: str):
    # todo there's probably a better place for this
    #  even an inline dict?
    known_orcids = pd.read_csv(known_orcids_tsv, sep="\t")
    # return a dict instead?
    return known_orcids


def get_known_templates(known_template_tsv: str):
    # todo there's probably a better place for this
    #  even an inline dict?
    known_templates = pd.read_csv(known_template_tsv, sep="\t")
    temp = dict(zip(known_templates.iloc[:, 0], known_templates.iloc[:, 1]))
    return temp


if __name__ == "__main__":
    cli()
