import pprint
import re
from datetime import datetime, timezone, timedelta
from typing import Dict

import pandas as pd
import requests
import validators
from linkml_runtime import SchemaView
from linkml_runtime.dumpers import yaml_dumper
from linkml_runtime.linkml_model import EnumDefinition
from nmdc_schema.nmdc import (
    AttributeValue,
    Biosample,
    ControlledTermValue,
    CreditAssociation,
    Database,
    GeolocationValue,
    OntologyClass,
    PersonValue,
    QuantityValue,
    Study,
    TextValue,
    TimestampValue,
)
from pydantic import (
    BaseModel,
)
from quantulum3 import parser

pd.set_option("display.max_columns", None)

# todo want entry-time validation and enrichment of study data

api_offset = 0
api_limit = 999
session_cookie = "eyJ0b2tlbiI6IHsiYWNjZXNzX3Rva2VuIjogIjc5YzY2ZGMzLTk5OWMtNGMxMS1hY2I5LWI1NDkyZjgwMmNlNSIsICJ0b2tlbl90eXBlIjogImJlYXJlciIsICJyZWZyZXNoX3Rva2VuIjogIjI1YmFhZDRjLWVjZTQtNDRiNC04ZWYyLTJiNzIxNDFlOTA5NSIsICJleHBpcmVzX2luIjogNjMxMTM4NTE4LCAic2NvcGUiOiAiL2F1dGhlbnRpY2F0ZSIsICJuYW1lIjogIk1hcmsgQW5kcmV3IE1pbGxlciIsICJvcmNpZCI6ICIwMDAwLTAwMDEtOTA3Ni02MDY2IiwgImV4cGlyZXNfYXQiOiAyMjg1Njc0MzcwfX0=.YqH7Jg.9LWzGVPqArKtptP8CGjokyEOBvY"

url = "https://data.dev.microbiomedata.org/api/metadata_submission"
# url = "https://data.microbiomedata.org/api/metadata_submission"

params = {"offset": api_offset, "limit": api_limit}
cookies = {"session": session_cookie}

submission_frame_tsv_out = "../../../assets/out/submissions_as_studies.tsv"

studies_as_submissions_yaml = "../../../assets/out/submissions_as_studies.yaml"

biosample_metadata_tsv = "../../../assets/out/biosample_metadata.tsv"

known_orcids_file = "../../../assets/in/known_orcids.tsv"

biosample_metdata_yaml = "../../../assets/out/biosample_metadata.yaml"

dh_to_nmdc_name_mappings = {
    "prev_land_use_meth": "previous_land_use_meth",
    "samp_collec_device": "samp_collect_device",
    "samp_name": "name",
    "soil_horizon": "horizon",
}

final_submission_columns = [
    "id",
    "author_orcid",
    "GOLDStudyId",
    "JGIStudyId",
    "created",
    "status",
    "packageName",
    "template",
    "omicsProcessingTypes",
    "data_rows",
    "studyName",
    "studyDate",
    "studyNumber",
    "alternativeNames",
    "linkOutWebpage",
    "datasetDoi",
    "description",
    "notes",
    "NCBIBioProjectId",
    "NCBIBioProjectName",
    "piEmail",
    "piName",
    "piOrcid",
]


# ---

# todo from nmdc-runtime
def now(as_str=False):
    dt = datetime.now(timezone.utc)
    return dt.isoformat() if as_str else dt


def expiry_dt_from_now(days=0, hours=0, minutes=0, seconds=0):
    return now() + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def has_passed(dt):
    return now() > dt


class RuntimeApiSiteClient:
    def __init__(self, base_url: str, site_id: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.site_id = site_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.headers = {}
        self.token_response = None
        self.refresh_token_after = None
        self.get_token()

    def request(self, method, url_path, params_or_json_data=None):
        self.ensure_token()
        kwargs = {"url": self.base_url + url_path, "headers": self.headers}
        if isinstance(params_or_json_data, BaseModel):
            params_or_json_data = params_or_json_data.dict(exclude_unset=True)
        if method.upper() == "GET":
            kwargs["params"] = params_or_json_data
        else:
            kwargs["json"] = params_or_json_data
        return requests.request(method, **kwargs)

    def get_token(self):
        rv = requests.post(
            self.base_url + "/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        self.token_response = rv.json()
        if "access_token" not in self.token_response:
            raise Exception(f"Getting token failed: {self.token_response}")

        self.headers["Authorization"] = f'Bearer {self.token_response["access_token"]}'
        self.refresh_token_after = expiry_dt_from_now(
            **self.token_response["expires"]
        ) - timedelta(seconds=5)

    def ensure_token(self):
        if has_passed(self.refresh_token_after):
            self.get_token()


# todo end nmdc-runtime copy/paste


def get_view(
        schema_url="https://raw.githubusercontent.com/microbiomedata/sheets_and_friends/main/artifacts/nmdc_submission_schema.yaml",
):
    print(f"creating a view of {schema_url}")
    view = SchemaView(schema_url)
    # todo error handling
    print(f"confirming load of schema '{view.schema.name}'")
    return view


def get_template_titles_names(
        template,
        view,
):
    template_slots = view.class_induced_slots(template)
    title_to_name_dict = {}

    row_list = []
    for i in template_slots:
        # todo when to use name and when to use alias
        current_dict = {
            "name": i.alias,
            "section": i.slot_group,
            "section_rank": view.get_slot(i.slot_group).rank,
            "slot_rank": i.rank,
        }
        if i.title:
            title_to_name_dict[i.title] = i.alias
            current_dict["title"] = i.title
        else:
            title_to_name_dict[i.alias] = i.alias
            current_dict["title"] = i.alias
        row_list.append(current_dict)
    df = pd.DataFrame(row_list)
    df.sort_values(by=["section_rank", "slot_rank"], inplace=True)

    return df


def just_submission_row(current_submission):
    data_rows = len(current_submission["metadata_submission"]["sampleData"])
    preliminaries_dict = {
        "status": current_submission["status"],
        "id": current_submission["id"],
        "author_orcid": current_submission["author_orcid"],
        "created": current_submission["created"],
        "data_rows": data_rows,
    }
    current_metadata_submission = current_submission["metadata_submission"]

    metadata_preliminaries = {}

    # todo are there any other structural variations between the received submissions?
    if "template" in current_metadata_submission:
        metadata_preliminaries["template"] = current_metadata_submission["template"]
    else:
        metadata_preliminaries["template"] = "<no metadata_submission.template>"

    if "packageName" in current_metadata_submission:
        metadata_preliminaries["packageName"] = current_metadata_submission[
            "packageName"
        ]
    else:
        metadata_preliminaries["packageName"] = "<no metadata_submission.packageName>"

    sfd = current_metadata_submission["studyForm"]

    study_links_protected = sfd["linkOutWebpage"]
    sfd["linkOutWebpage"] = "|".join(sfd["linkOutWebpage"])
    # todo need to collapse some objects
    #  contributors may be a list of objects?
    #  assume the others are lists of strings?
    contributors_protected = sfd["contributors"]
    sfd["contributors"] = "<TODO>"
    mofd = current_metadata_submission["multiOmicsForm"]
    alternative_names_protected = mofd["alternativeNames"]
    mofd["alternativeNames"] = "|".join(mofd["alternativeNames"])
    mofd["omicsProcessingTypes"] = "|".join(mofd["omicsProcessingTypes"])
    # todo don't collect NCBIBioProjectName any more
    #  look up from NCBIBioProjectId
    #  which should be validated at collection time
    mofd["NCBIBioProjectName"] = f"{mofd['NCBIBioProjectName']}".strip()
    metadata_submission_dict = {**metadata_preliminaries, **sfd, **mofd}
    row_dict = {**preliminaries_dict, **metadata_submission_dict}

    # author_orcid	0000-0001-9076-6066
    # last	Miller
    # first	Mark
    # other
    # created	2022-06-09T20:52:10.294593
    # status	complete
    # packageName	soil
    # template	soil_emsl_jgi_mg
    # omicsProcessingTypes	mg-jgi|mp-emsl
    # data_rows	6
    # studyDate

    submission_as_study = Study(
        id=f"nmdc:submission_{row_dict['id']}",
        type="nmdc:Study",
        INSDC_SRA_ENA_study_identifiers=[],
        MGnify_project_identifiers=[],
        ess_dive_datasets=[],
        funding_sources=[],
        publications=[],
        relevant_protocols=[],
        abstract=None,
        objective=None,
        study_image=[],
        title=None,
        ecosystem=None,
        ecosystem_category=None,
        ecosystem_subtype=None,
        ecosystem_type=None,
        specific_ecosystem=None,
    )

    if row_dict["NCBIBioProjectId"] != "" and validators.url(
            row_dict["NCBIBioProjectId"]
    ):
        submission_as_study.INSDC_bioproject_identifiers.append(
            row_dict["NCBIBioProjectId"]
        )

    if row_dict["studyName"] != "":
        submission_as_study.name = (f"{row_dict['studyName']}",)

    if row_dict["description"] != "":
        submission_as_study.description = (f"{row_dict['description']}",)

    if "author" in current_submission and "orcid" in current_submission["author"]:
        submitter_person = PersonValue(orcid=current_submission["author"]["orcid"])
        if current_submission["author"]["name"] and current_submission["author"]["name"] != "":
            submitter_person.has_raw_value = current_submission["author"]["name"]
        # todo what role to use? Project administration?
        submitter_ca = CreditAssociation(applies_to_person=submitter_person, applied_roles=['Project administration'])
        submission_as_study.has_credit_associations.append(submitter_ca)

    if row_dict["piOrcid"] or row_dict["piName"] or row_dict["piEmail"]:
        pi_person = PersonValue()
        if row_dict["piOrcid"] and row_dict["piOrcid"] != "":
            pi_person.orcid = row_dict["piOrcid"]
        if row_dict["piName"] and row_dict["piName"] != "":
            pi_person.has_raw_value = row_dict["piName"]
        if row_dict["piEmail"] and row_dict["piEmail"] != "":
            pi_person.email = row_dict["piEmail"]
        pi_ca = CreditAssociation(
            applies_to_person=pi_person, applied_roles=["Principal Investigator"]
        )
        submission_as_study.principal_investigator = pi_person
        submission_as_study.has_credit_associations.append(pi_ca)

    already_associated = {}
    for i in submission_as_study.has_credit_associations:
        if i.applies_to_person.orcid:
            already_associated[i.applies_to_person.orcid] = i

    for i in contributors_protected:
        io = i['orcid']
        if io in already_associated:
            aa = already_associated[io]
            for j in i["roles"]:
                aa.applied_roles.append(j)
            already_associated[io] = aa
        else:
            temp_person = PersonValue(
                orcid=i["orcid"])
            if i["name"] and i["name"] != "":
                temp_person.has_raw_value = i["name"]
            temp_ca = CreditAssociation(
                applies_to_person=temp_person, applied_roles=i["roles"]
            )
            already_associated[io] = temp_ca

    submission_as_study.has_credit_associations = []

    for k, v in already_associated.items():
        submission_as_study.has_credit_associations.append(v)

    if row_dict["datasetDoi"] != "":
        submission_as_study.doi = AttributeValue(has_raw_value=row_dict["datasetDoi"])

    for i in study_links_protected:
        submission_as_study.websites.append(i)

    if row_dict["notes"] != "":
        submission_as_study.alternative_descriptions.append(row_dict["notes"])

    # todo don't save or modify if empty
    if row_dict['NCBIBioProjectName'] and row_dict['NCBIBioProjectName'] != "":
        submission_as_study.alternative_titles.append(
            f"{row_dict['NCBIBioProjectName']}"
        )

    # supposed to be a list of external identifiers, which are string representations of CURIEs
    if row_dict["GOLDStudyId"] != "" and validators.url(row_dict["GOLDStudyId"]):
        submission_as_study.GOLD_study_identifiers = row_dict["GOLDStudyId"]
    # else:
    #     # print(f"invalid URL {row_dict['GOLDStudyId']}")
    #     submission_as_study.GOLD_study_identifiers = row_dict["GOLDStudyId"]

    # alternate ID?
    # todo check with validators.url() ?
    if row_dict["JGIStudyId"] != "":
        submission_as_study.alternative_identifiers.append(row_dict["JGIStudyId"])
    # else:
    #     # print(f"invalid URL {row_dict['JGIStudyId']}")
    #     submission_as_study.alternative_identifiers.append(row_dict["JGIStudyId"])

    # emsl studyNumber
    if row_dict["studyNumber"] != "":
        submission_as_study.alternative_identifiers.append(row_dict["studyNumber"])
    # else:
    #     # print(f"invalid URL {row_dict['studyNumber']}")
    #     submission_as_study.alternative_identifiers.append(row_dict["studyNumber"])

    for i in alternative_names_protected:
        submission_as_study.alternative_names.append(i)

    return row_dict, submission_as_study


def assemble_studies_frame(submission_dict):
    row_list = []
    study_obj_list = []
    db_obj = Database()
    for k, v in submission_dict.items():
        # # todo would it be more efficient to strip out the metadata_submission.sampleData?
        # # submission = v.copy()
        # # submission.pop('key', None)
        row_dict, submission_as_study = just_submission_row(v)
        study_obj_list.append(submission_as_study)
        row_list.append(row_dict)
    row_frame = pd.DataFrame(row_list)

    db_obj.study_set = study_obj_list
    yaml_dumper.dump(db_obj, studies_as_submissions_yaml)

    return row_frame


def get_known_orcids(known_orcids_tsv: str):
    # todo there's probably a better place for this
    #  even an inline dict?
    known_orcids = pd.read_csv(known_orcids_tsv, sep="\t")
    # return a dict instead?
    return known_orcids


# ---

# todo refactor flattering, with explicit paths
def just_metadata_rows(submissions_dict: Dict, view: SchemaView):
    frame_list = []
    for k, v in submissions_dict.items():
        study_rhs = k

        sample_data_frame = pd.DataFrame(v["metadata_submission"]["sampleData"])
        if len(sample_data_frame.index) > 2 and v["status"] == "complete":
            # print(f"{study_rhs}: {v['status']}")
            asserted_template = v["metadata_submission"]["template"]

            current_title_to_name_frame = get_template_titles_names(
                asserted_template, view
            )

            expected = list(current_title_to_name_frame["title"])

            provided = list(sample_data_frame.iloc[1])

            if provided != expected:
                print(
                    f"column headings for {study_rhs} do not match columns from claimed template {asserted_template}"
                )
                exit()
            else:
                print(
                    f"column headings for {study_rhs} match columns from claimed template {asserted_template}"
                )
                sample_data_frame.drop(index=[0, 1], inplace=True)
                sample_data_frame.columns = list(current_title_to_name_frame["name"])
                sample_data_frame["part_of"] = f"nmdc:submission_{k}"

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

                frame_list.append(sample_data_frame)

    all_sample_frame = pd.concat(frame_list)
    return all_sample_frame


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
    if raw_value:
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

    # with open("instantiation_log.yml", "w") as outfile:
    #     yaml.dump(instantiation_log, outfile, default_flow_style=False)


def extract_ctv(raw_value: str):
    # ____mediterranean shrubland biome [ENVO:01000217]
    # todo could be more than one term id
    #  pipe or semicolon separated?
    #  or just one big mess?
    #  in any case, could be label only, id only, matching label and id, mismatch...
    #  check ontology owner to see if term is still active and label/id match?
    if raw_value and raw_value != "":
        underscoreless = re.sub(pattern=r"^_*\s*", repl="", string=raw_value)
        p = re.compile(r"\[(.*)\]")
        term_id = p.findall(underscoreless)
        if term_id:
            label = underscoreless.replace(f"[{term_id[0]}]", "")
            label = label.strip()
            oc = OntologyClass(id=term_id[0], name=label)
            return oc


def set_to_list(set_input, do_sort=True):
    temp = list(set_input)
    if do_sort:
        temp.sort()
    return temp


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def sample_df_to_sample_db(sample_df, dh_view):
    # todo parameterize
    nmdc_view = get_view(
        schema_url="https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/src/schema/nmdc.yaml",
    )

    mixs_view = get_view(
        schema_url="https://raw.githubusercontent.com/GenomicsStandardsConsortium/mixs/main/model/schema/mixs.yaml",
    )

    bs_induced_slots = nmdc_view.class_induced_slots("biosample")
    bs_induced_slot_names = [i.alias for i in bs_induced_slots]
    # could have been a dict comprehension one-liner
    bs_induced_slot_dict = dict(zip(bs_induced_slot_names, bs_induced_slots))

    biosample_list = sample_df.to_dict(orient="records")

    biosample_set_list = []

    biosample_db = Database()

    string_slots = set()

    other_ranges = {}

    unmapped = set()

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
            id=current_biosample["id"],
            part_of=current_biosample["part_of"],
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

        instantiated_bs.alternative_identifiers.append(current_biosample['source_mat_id'])

        for k, v in current_biosample.items():
            # expected_key = None
            if k in dh_to_nmdc_name_mappings:
                expected_key = dh_to_nmdc_name_mappings[k]
            else:
                expected_key = k
            if expected_key in bs_induced_slot_dict:
                current_range = bs_induced_slot_dict[expected_key].range
                if current_range == ControlledTermValue.class_name:
                    if v and v != "":
                        oc = extract_ctv(v)
                        instantiated_bs[expected_key] = ControlledTermValue(
                            has_raw_value=v, term=oc
                        )
                    # else:
                    #     oc = None
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
                elif current_range == TextValue.class_name and v and v != "":
                    instantiated_bs[expected_key] = TextValue(has_raw_value=v)
                elif current_range == TimestampValue.class_name and v and v != "":
                    instantiated_bs[expected_key] = TimestampValue(has_raw_value=v)
                elif (
                        (
                                type(nmdc_view.get_element(current_range)).class_name
                                == EnumDefinition.class_name
                        )
                        and v
                        and v != ""
                ):
                    instantiated_bs[expected_key] = v
                elif current_range == "string" and v and v != "":
                    # todo note if the string slot comes from EMSL or JGI... those are expected
                    # if type(v) == str and v.isnumeric():
                    if is_number(v):
                        instantiated_bs[expected_key] = float(v)
                    else:
                        instantiated_bs[expected_key] = v
                    string_slots.add(expected_key)
                else:
                    range_element = nmdc_view.get_element(current_range)
                    other_ranges[expected_key] = type(range_element).class_name
            else:
                unmapped.add(k)

        biosample_set_list.append(instantiated_bs)

    biosample_db.biosample_set = biosample_set_list

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
        "string_slots": string_slots,
        "other_ranges": other_ranges,
        "nmdc_includes": nmdc_includes,
        "mixs_defines": mixs_defines,
        "dh_defines": dh_defines,
    }

    pprint.pprint(instantiation_log)

    return biosample_db


# ---

if __name__ == "__main__":
    portal_view = get_view()

    # todo parameterize
    mintingClient = RuntimeApiSiteClient(
        base_url="https://api.dev.microbiomedata.org",
        site_id="mam_lbl_2019mbp_nobs",
        client_id="sys0acx2cb96",
        client_secret="w@sk23X?Ea7.",
    )

    known_orcids_frame = get_known_orcids(known_orcids_tsv=known_orcids_file)

    submission_response = requests.get(url, cookies=cookies, params=params)

    rj = submission_response.json()

    submission_count = rj["count"]

    print(f"submission_count = {submission_count}")

    submission_results = rj["results"]

    submission_result_ids = [i["id"] for i in submission_results]

    submission_results_dict = dict(zip(submission_result_ids, submission_results))

    submission_frame = assemble_studies_frame(submission_results_dict)

    submission_frame = submission_frame.merge(
        right=known_orcids_frame, how="left", left_on="author_orcid", right_on="orcid"
    )

    submission_frame = submission_frame[final_submission_columns]

    submission_frame.sort_values(
        by=["created", "data_rows"], inplace=True, ascending=[False, False]
    )

    submission_frame.to_csv(submission_frame_tsv_out, sep="\t", index=False)

    jmf = just_metadata_rows(submission_results_dict, portal_view)

    jmf.to_csv(biosample_metadata_tsv, sep="\t", index=False)

    # as_db = sample_df_to_sample_db(jmf, dh_view=portal_view)
    #
    # yaml_dumper.dump(as_db, biosample_metdata_yaml)
