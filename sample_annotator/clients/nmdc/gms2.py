import pprint

import pandas as pd
import requests
from typing import Dict

from linkml_runtime import SchemaView

import requests

from datetime import datetime, timezone, timedelta

from nmdc_schema.nmdc import Study
from pydantic import (
    BaseModel,
    root_validator,
    conint,
    PositiveInt,
    NonNegativeInt,
)

pd.set_option("display.max_columns", None)

# todo don't forget to mint IDs for studies and samples
#  want entry-time validation and enrichment of study data

# todo make Study and Biosample instances!

api_offset = 0
api_limit = 999
session_cookie = "eyJ0b2tlbiI6IHsiYWNjZXNzX3Rva2VuIjogIjc5YzY2ZGMzLTk5OWMtNGMxMS1hY2I5LWI1NDkyZjgwMmNlNSIsICJ0b2tlbl90eXBlIjogImJlYXJlciIsICJyZWZyZXNoX3Rva2VuIjogIjI1YmFhZDRjLWVjZTQtNDRiNC04ZWYyLTJiNzIxNDFlOTA5NSIsICJleHBpcmVzX2luIjogNjMxMTM4NTE4LCAic2NvcGUiOiAiL2F1dGhlbnRpY2F0ZSIsICJuYW1lIjogIk1hcmsgQW5kcmV3IE1pbGxlciIsICJvcmNpZCI6ICIwMDAwLTAwMDEtOTA3Ni02MDY2IiwgImV4cGlyZXNfYXQiOiAyMjg1Njc0MzcwfX0=.YqH7Jg.9LWzGVPqArKtptP8CGjokyEOBvY"

url = "https://data.dev.microbiomedata.org/api/metadata_submission"
# url = "https://data.microbiomedata.org/api/metadata_submission"

params = {"offset": api_offset, "limit": api_limit}
cookies = {"session": session_cookie}

current_submission_id = "3e919e97-0437-4ca7-a758-ac36c45b908d"

submission_frame_tsv_out = "../../../assets/out/sf2.tsv"

known_orcids_file = "../../../assets/in/known_orcids.tsv"

final_submission_columns = [
    "id",
    "author_orcid",
    "last",
    "first",
    "other",
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
    "contributors",
]


# ---

# todo from nmdc run time


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


# todo end nmdc runtime copy/paste


def get_view(
        schema_url="https://raw.githubusercontent.com/microbiomedata/sheets_and_friends/main/artifacts/nmdc_dh.yaml",
):
    print(f"creating a view of {schema_url}")
    view = SchemaView(schema_url)
    # todo error handling
    print(f"confirming load of schema '{view.schema.name}'")
    return view


portal_view = get_view()


def get_template_titles_names(
        template,
        view,
):
    template_slots = view.class_induced_slots(template)
    title_to_name_dict = {}
    # todo I had some hard-coded fixes for this in sample_annotator/clients/nmdc/get_metadata_submissions.py
    #  they need some revision esp regarding ids and names
    # dh_to_nmdc_name_mappings = {
    #     "samp_name": "name",
    #     "soil_horizon": "horizon",
    #     "prev_land_use_meth": "previous_land_use_meth",
    #     "samp_collec_device": "samp_collect_device",
    # }
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


# @click.option("--known_orcids_tsv", default="assets/in/known_orcids.tsv")
# known_orcids_frame = get_known_orcids(known_orcids_tsv=known_orcids_tsv)


# # todo refactor flattering, with explicit paths
# def get_submission_row(
#         submissions_dict: Dict, submission_id: str, view: SchemaView, known_orcids_tsv: str
# ):
#     current_submission = submissions_dict[submission_id]
#     # known_orcids_frame = get_known_orcids(known_orcids_tsv=known_orcids_tsv)
#     # todo refactor?
#     #  also, look up orcids?
#     #  btw, no more looking up study id -> template name
#     #  just make sure the second row  = the [titles of terms] from the corresponding template
#     preliminaries_dict = {
#         "status": current_submission["status"],
#         "id": current_submission["id"],
#         "author_orcid": current_submission["author_orcid"],
#         "created": current_submission["created"],
#     }
#     current_metadata_submission = current_submission["metadata_submission"]
#
#     metadata_preliminaries = {
#         "template": current_metadata_submission["template"],
#         "packageName": current_metadata_submission["packageName"],
#     }
#
#     # ---
#
#     current_title_to_name_frame = get_template_titles_names(
#         metadata_preliminaries["template"], view
#     )
#     # pprint.pprint(current_title_to_name_frame)
#
#     # ---
#
#     sfd = current_metadata_submission["studyForm"]
#     sfd["linkOutWebpage"] = "|".join(sfd["linkOutWebpage"])
#     # todo need to collapse some objects
#     #  contributors may be a list of objects?
#     #  assume the others are lists of strings?
#     sfd["contributors"] = "<TODO>"
#     mofd = current_metadata_submission["multiOmicsForm"]
#     mofd["alternativeNames"] = "|".join(mofd["alternativeNames"])
#     mofd["omicsProcessingTypes"] = "|".join(mofd["omicsProcessingTypes"])
#     # todo don't collect NCBIBioProjectName any more
#     #  look up from NCBIBioProjectId
#     #  which should be validated at collection time
#     mofd["NCBIBioProjectName"] = f"<DEPRECATED> {mofd['NCBIBioProjectName']}".strip()
#     metadata_submission_dict = {**metadata_preliminaries, **sfd, **mofd}
#     row_dict = {**preliminaries_dict, **metadata_submission_dict}
#     # pprint.pprint(row_dict)
#
#     # ---
#
#     sample_data_frame = pd.DataFrame(current_metadata_submission["sampleData"])
#     # sample_data_frame.columns = list(sample_data_frame.iloc[1])
#
#     provided = list(sample_data_frame.iloc[1])
#     # print(provided)
#
#     sample_data_frame.drop(index=[0, 1], inplace=True)
#     expected = list(current_title_to_name_frame["title"])
#     # print(expected)
#
#     if provided != expected:
#         print(
#             f'column headings for {current_submission["id"]} do not match columns from claimed template {current_metadata_submission["template"]}'
#         )
#         exit()
#     else:
#         print(
#             f'column headings for {current_submission["id"]} match columns from claimed template {current_metadata_submission["template"]}'
#         )
#
#     sample_data_frame.columns = list(current_title_to_name_frame["name"])
#     # print(sample_data_frame)
#
#
# # get_submission_row(submission_results_dict, current_submission_id, portal_view, known_orcids_tsv=None)


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

    metadata_preliminaries = {
        "packageName": current_metadata_submission["packageName"],
    }

    # todo are there any other variations between submissions?
    if "template" in current_metadata_submission:
        # print(current_submission["id"])
        metadata_preliminaries["template"] = current_metadata_submission["template"]
    else:
        metadata_preliminaries["template"] = "<no metadata_submission.template>"

    sfd = current_metadata_submission["studyForm"]
    sfd["linkOutWebpage"] = "|".join(sfd["linkOutWebpage"])
    # todo need to collapse some objects
    #  contributors may be a list of objects?
    #  assume the others are lists of strings?
    sfd["contributors"] = "<TODO>"
    mofd = current_metadata_submission["multiOmicsForm"]
    mofd["alternativeNames"] = "|".join(mofd["alternativeNames"])
    mofd["omicsProcessingTypes"] = "|".join(mofd["omicsProcessingTypes"])
    # todo don't collect NCBIBioProjectName any more
    #  look up from NCBIBioProjectId
    #  which should be validated at collection time
    mofd["NCBIBioProjectName"] = f"<DEPRECATED> {mofd['NCBIBioProjectName']}".strip()
    metadata_submission_dict = {**metadata_preliminaries, **sfd, **mofd}
    row_dict = {**preliminaries_dict, **metadata_submission_dict}

    return row_dict


def assemble_studies_frame(submission_dict):
    row_list = []
    for k, v in submission_dict.items():
        # # todo would it be more efficient to strip out the metadata_submission.sampleData?
        # # submission = v.copy()
        # # submission.pop('key', None)
        row_dict = just_submission_row(v)
        row_list.append(row_dict)
    row_frame = pd.DataFrame(row_list)
    return row_frame


def get_known_orcids(known_orcids_tsv: str):
    # todo there's probably a better place for this
    #  even an inline dict?
    known_orcids = pd.read_csv(known_orcids_tsv, sep="\t")
    # return a dict instead?
    return known_orcids


# ---

mintingClient = RuntimeApiSiteClient(
    base_url="https://api.dev.microbiomedata.org",
    site_id="mam_lbl_2019mbp_nobs",
    client_id="sys0acx2cb96",
    client_secret="w@sk23X?Ea7.",
)

porjda = {"populator": "", "naa": "nmdc", "shoulder": "fk0", "number": 1}

result = mintingClient.request("POST", "/ids/mint", params_or_json_data=porjda)

print(result.json())

x = Study(result.json()[0])

print(x)

# ---

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


# todo duplicates just_submission_row
def make_study_object(current_submission):
    preliminaries_dict = {
        "status": current_submission["status"],
        "id": current_submission["id"],
        "author_orcid": current_submission["author_orcid"],
        "created": current_submission["created"],
        # "data_rows": data_rows,
    }
    current_metadata_submission = current_submission["metadata_submission"]

    metadata_preliminaries = {
        "packageName": current_metadata_submission["packageName"],
    }

    # todo are there any other variations between submissions?
    if "template" in current_metadata_submission:
        # print(current_submission["id"])
        metadata_preliminaries["template"] = current_metadata_submission["template"]
    else:
        metadata_preliminaries["template"] = "<no metadata_submission.template>"

    sfd = current_metadata_submission["studyForm"]
    sfd["linkOutWebpage"] = "|".join(sfd["linkOutWebpage"])
    # todo need to collapse some objects
    #  contributors may be a list of objects?
    #  assume the others are lists of strings?
    sfd["contributors"] = "<TODO>"
    mofd = current_metadata_submission["multiOmicsForm"]
    mofd["alternativeNames"] = "|".join(mofd["alternativeNames"])
    mofd["omicsProcessingTypes"] = "|".join(mofd["omicsProcessingTypes"])
    # todo don't collect NCBIBioProjectName any more
    #  look up from NCBIBioProjectId
    #  which should be validated at collection time
    mofd["NCBIBioProjectName"] = f"<DEPRECATED> {mofd['NCBIBioProjectName']}".strip()
    metadata_submission_dict = {**metadata_preliminaries, **sfd, **mofd}
    row_dict = {**preliminaries_dict, **metadata_submission_dict}

    # author_orcid	0000-0001-9076-6066
    # last	Miller
    # first	Mark
    # other
    # JGIStudyId	31415
    # created	2022-06-09T20:52:10.294593
    # status	complete
    # packageName	soil
    # template	soil_emsl_jgi_mg
    # omicsProcessingTypes	mg-jgi|mp-emsl
    # data_rows	6
    # studyDate
    # studyNumber	31415
    # alternativeNames
    # linkOutWebpage	https://orcid.org/0000-0001-9076-6066
    # notes	I did an optional
    # NCBIBioProjectName	<DEPRECATED>
    # piEmail	MAM@lbl.gov
    # piName	MAM
    # piOrcid	0000-0001-9076-6066
    # contributors	<TODO>

    submission_as_study = Study(
        id=f"nmdc:submission_{row_dict['id']}",
        name=f"{row_dict['studyName']}",
        description=f"{row_dict['description']}",
        # # alternative_identifiers=list(None),
        ecosystem=None,
        ecosystem_category=None,
        ecosystem_type=None,
        ecosystem_subtype=None,
        specific_ecosystem=None,
        # principal_investigator=None,
        # doi=f"{row_dict['datasetDoi']}",
        title=None,
        alternative_titles=[],
        alternative_descriptions=[],
        alternative_names=[],
        abstract=None,
        objective=None,
        # websites=[],
        publications=[],
        ess_dive_datasets=[],
        # type=None,
        relevant_protocols=[],
        funding_sources=[],
        # INSDC_bioproject_identifiers=[f"{row_dict['NCBIBioProjectId']}"],
        INSDC_SRA_ENA_study_identifiers=[],
        # GOLD_study_identifiers=[f"{row_dict['GOLDStudyId']}"],
        MGnify_project_identifiers=[],
        # has_credit_associations={},
        # study_image=[],
    )

    return submission_as_study


# temp_submission = submission_results_dict['6b1e4498-529d-4813-bf8b-b69572183330']
# temp_study = make_study_object(temp_submission)
# print(temp_study)


# todo duplicates assemble_studies_frame
def make_study_collection(submission_dict):
    # row_list = []
    for k, v in submission_dict.items():
        temp_study = make_study_object(v)
        print(temp_study)
        # # todo would it be more efficient to strip out the metadata_submission.sampleData?
        # # submission = v.copy()
        # # submission.pop('key', None)
        # row_dict = just_submission_row(v)
    #     row_list.append(row_dict)
    # row_frame = pd.DataFrame(row_list)
    # return row_frame


make_study_collection(submission_results_dict)

# Study(
# id='nmdc:fk0t221'
# name=None
# description=None
# alternative_identifiers=[]
# ecosystem=None
# ecosystem_category=None
# ecosystem_type=None
# ecosystem_subtype=None
# specific_ecosystem=None
# principal_investigator=None
# doi=None
# title=None
# alternative_titles=[]
# alternative_descriptions=[]
# alternative_names=[]
# abstract=None
# objective=None
# websites=[]
# publications=[]
# ess_dive_datasets=[]
# type=None
# relevant_protocols=[]
# funding_sources=[]
# INSDC_bioproject_identifiers=[]
# INSDC_SRA_ENA_study_identifiers=[]
# GOLD_study_identifiers=[]
# MGnify_project_identifiers=[]
# has_credit_associations={}
# study_image=[]
# )
