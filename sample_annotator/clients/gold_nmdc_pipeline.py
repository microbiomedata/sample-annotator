import io
import os
import re
import json
import pkgutil
import logging

from typing import Dict, List, Union

import jsonschema
import pandas as pd
import nmdc_schema.nmdc as nmdc

from linkml_runtime.dumpers import json_dumper
from linkml_runtime.linkml_model.types import XSDDateTime
from sample_annotator.clients.gold_client import (
    GoldClient,
    StudyDict,
    ProjectDict,
    ApDict,
    SampleDict,
)


logger = logging.getLogger(__name__)  # module level logger


class GoldNMDC(GoldClient):
    def __init__(self, study_id: str) -> None:

        # construct MongoDB with study_set, biosample_set, omics_processing_set
        self.nmdc_db = nmdc.Database()

        # set the GOLD study id
        self.study_id = study_id

    def project_ids_subset(self, path_to_subset_ids: Union[str, bytes, os.PathLike]) -> List[str]:
        """List of GOLD project ids to subset the retreived dataset on.

        :return: list of sample project ids in the subset
        """
        df = pd.read_csv(path_to_subset_ids)

        # this assumes that at all times there is only one
        # column in the project_ids_subset.txt file
        return df[df.columns.values[0]].to_list()

    def validate_nmdc(
        self, file_path: Union[str, bytes, os.PathLike], database_set: str = None
    ) -> bool:
        """Validate JSON files against the NMDC Schema using the
        jsonschema library.

        :param file_path: path to input JSON file
        :param database_set: optional top level database set
            (e.g, study_set, biosample_set) that contains the data,
            defaults to None
        :return: True if no validation errors are raised, else False
        """
        nmdc_json_schema_bytes = io.BytesIO(
            pkgutil.get_data("nmdc_schema", "nmdc.schema.json")
        )
        nmdc_json_schema = json.loads(nmdc_json_schema_bytes.getvalue())

        with open(file_path, "r") as fh:
            json_data = json.load(fh)

            if database_set:
                if type(json_data) == list:
                    json_data = {f"{database_set}": json_data}
                else:
                    json_data = {f"{database_set}": [json_data]}
        try:
            jsonschema.validate(instance=json_data, schema=nmdc_json_schema)
        except jsonschema.exceptions.ValidationError as err:
            logger.error(err.message)

            return False

        return True

    def project_has_output_dict(self) -> List[Dict[str, str]]:
        """Get list of dictionaries as {"projectGoldId": ["has_input_ids"]}

        :return: List of dicts
        """
        read_qc_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "nmdc", "input", "EMP_soil_readQC.json"
        )

        with open(read_qc_path) as f:
            read_qc_array = json.load(f)

        read_qc_list = []

        for item in read_qc_array:
            has_output_dict = {}
            has_output_dict[item["was_informed_by"].replace("gold:", "")] = item[
                "has_input"
            ][0]
            read_qc_list.append(has_output_dict)

        project_has_output_list = {
            k: [d.get(k) for d in read_qc_list if k in d]
            for k in set().union(*read_qc_list)
        }

        return project_has_output_list

    def get_pi_dict(self, nmdc_entry: Dict[str, str]) -> Dict[str, str]:
        """Get dictionary with PI information like name, email, etc.

        :param nmdc_entry: An NMDC record like study, project, etc.
        :return: dictionary with PI information.
        """
        pi_dict = next(
            (contact for contact in nmdc_entry["contacts"] if "PI" in contact["roles"])
        )

        return pi_dict

    def mod_date_handler(self, nmdc_entry: Dict[str, str]) -> XSDDateTime:
        """Compute modDate in GOLD if it has not been populated in the database.

        :param nmdc_entry: An NMDC record like study, project, etc.
        :return: XSDDateTime formatted datetime
        """
        # use below logic to determine modDate if it is not
        # populated in GOLD
        mod_date = (
            XSDDateTime(nmdc_entry["addDate"])
            if nmdc_entry["modDate"] is None
            else XSDDateTime(nmdc_entry["modDate"])
        )

        return mod_date

    def _processing_institute_handler(self, sequencing_centers: List[str]) -> List[str]:
        """GOLD NMDC transformation pipeline specific term handler.
        Specific to the ProcessingInstitutionEnum term.

        :param sequencing_centers: List of sequencing centers as stored in GOLD
        :return: NMDC Schema compliant processing institute names
        """
        nmdc_compliant_seq_ctrs = []

        for seq_ctr in sequencing_centers:
            if re.findall(
                r"University of California[,]? San Diego", seq_ctr, flags=re.IGNORECASE
            ):
                nmdc_compliant_seq_ctrs.append("UCSD")

            if re.findall(
                r"Environmental Molecular Sciences Laboratory",
                seq_ctr,
                flags=re.IGNORECASE,
            ):
                nmdc_compliant_seq_ctrs.append("EMSL")

            if re.findall(r"Joint Genome Institute", seq_ctr, flags=re.IGNORECASE):
                nmdc_compliant_seq_ctrs.append("JGI")

        return nmdc_compliant_seq_ctrs

    def compute_study_set(self, study_data: StudyDict):
        """Compute study_set parameters to be populated from the dataset."""
        pi_dict = self.get_pi_dict(study_data)

        self.nmdc_db.study_set.append(
            nmdc.Study(
                id="gold:" + study_data["studyGoldId"],
                description=study_data["description"],
                title=study_data["studyName"],
                GOLD_study_identifiers="gold:" + study_data["studyGoldId"],
                principal_investigator=nmdc.PersonValue(
                    has_raw_value=pi_dict["name"],
                    name=pi_dict["name"],
                    email=pi_dict["email"],
                ),
                type="nmdc:Study",
            )
        )

    def compute_biosample_set(
        self, study_data: StudyDict, biosamples: List[str], projects: List[str]
    ) -> SampleDict:
        """Compute biosample parameters to be populated from the dataset."""
        for biosample in biosamples:
            try:
                mod_date = self.mod_date_handler(biosample)

                # use the logic in if conditional to populate value for
                # depth, when depth can be retreived from GOLD API
                if biosample["depthInMeters"] is not None:
                    depth = nmdc.QuantityValue(
                        has_raw_value=biosample["depthInMeters"],
                        has_numeric_value=biosample["depthInMeters"],
                        has_unit="meter",
                    )
                else:
                    depth = {}

                if biosample["depthInMeters2"] is not None:
                    depth2 = nmdc.QuantityValue(
                        has_raw_value=biosample["depthInMeters2"],
                        has_numeric_value=biosample["depthInMeters2"],
                        has_unit="meter",
                    )
                else:
                    depth2 = {}

                # retrieve INSDC identifier information using both projects and biosamples
                insdc_biosample_identifiers = [
                    "biosample:" + proj["ncbiBioSampleAccession"]
                    for proj in projects
                    if proj["biosampleGoldId"] == biosample["biosampleGoldId"]
                ]

                self.nmdc_db.biosample_set.append(
                    nmdc.Biosample(
                        # biosample identifiers
                        id="gold:" + biosample["biosampleGoldId"],
                        GOLD_sample_identifiers="gold:" + biosample["biosampleGoldId"],
                        INSDC_biosample_identifiers=insdc_biosample_identifiers,
                        
                        # metadata fields
                        description=biosample["description"],
                        name=biosample["biosampleName"],
                        part_of=self.study_id,
                        ncbi_taxonomy_name=biosample["ncbiTaxName"],
                        type="nmdc:Biosample",
                        
                        # biosample date information
                        add_date=XSDDateTime(biosample["addDate"]),
                        collection_date=nmdc.TimestampValue(
                            has_raw_value=biosample["dateCollected"]
                        ),
                        mod_date=mod_date,
                        
                        # Earth fields
                        depth=depth,
                        
                        # TODO: this is temporary non MIxS that can
                        # hopefully be eliminated sooner rather than later
                        depth2=depth2,
                        temp=nmdc.QuantityValue(
                            has_numeric_value=biosample["sampleCollectionTemperature"]
                        ),
                        
                        # ecosystem collected from fields
                        ecosystem=biosample["ecosystem"],
                        ecosystem_category=biosample["ecosystemCategory"],
                        ecosystem_subtype=biosample["ecosystemSubtype"],
                        ecosystem_type=biosample["ecosystemType"],
                        specific_ecosystem=biosample["specificEcosystem"],
                        
                        # collection site metadata
                        geo_loc_name=nmdc.TextValue(has_raw_value=biosample["geoLocation"]),
                        lat_lon=nmdc.GeolocationValue(
                            has_raw_value=str(biosample["latitude"])
                            + " "
                            + str(biosample["longitude"]),
                            latitude=biosample["latitude"],
                            longitude=biosample["longitude"],
                        ),
                        habitat=biosample["habitat"],
                        location=biosample["isoCountry"],
                        
                        # collection metadata fields
                        host_name=biosample["hostName"],
                        sample_collection_site=biosample["sampleBodySite"],
                        
                        # chemical metadata fields
                        nitrate=nmdc.QuantityValue(
                            has_numeric_value=biosample["nitrateConcentration"]
                        ),
                        salinity=nmdc.QuantityValue(
                            has_numeric_value=biosample["salinityConcentration"]
                        ),
                        
                        # environment metadata fields
                        env_broad_scale=nmdc.ControlledTermValue(
                            has_raw_value=biosample["envoBroadScale"]["id"].replace(
                                "_", ":"
                            )
                        ),
                        env_local_scale=nmdc.ControlledTermValue(
                            has_raw_value=biosample["envoLocalScale"]["id"].replace(
                                "_", ":"
                            )
                        ),
                        env_medium=nmdc.ControlledTermValue(
                            has_raw_value=biosample["envoMedium"]["id"].replace("_", ":")
                        ),
                        sample_link="gold:" + study_data["studyGoldId"],
                    )
                )
            except:
                logger.error(
                    f"Biosample not properly annotated: {biosample['biosampleGoldId']}"
                )

    def compute_project_set(self, projects: List[str]) -> ProjectDict:
        """Compute sequencing project parameters to be populated from the dataset."""
        for project in projects:
            try:
                pi_dict = self.get_pi_dict(project)

                mod_date = self.mod_date_handler(project)

                project_has_output_dict = self.project_has_output_dict()

                # create value for has_output attribute
                if project["projectGoldId"] in self.project_has_output_dict():
                    has_output = project_has_output_dict[project["projectGoldId"]]

                self.nmdc_db.omics_processing_set.append(
                    nmdc.OmicsProcessing(
                        # omics processing metadata
                        id="gold:" + project["projectGoldId"],
                        name=project["projectName"],
                        GOLD_sequencing_project_identifiers="gold:"
                        + project["projectGoldId"],
                        ncbi_project_name=project["projectName"],
                        type="nmdc:OmicsProcessing",
                        has_input="gold:" + project["biosampleGoldId"],
                        has_output=has_output,
                        part_of=self.study_id,
                        
                        # omics processing date fields
                        add_date=XSDDateTime(project["addDate"]),
                        mod_date=mod_date,
                        principal_investigator=nmdc.PersonValue(
                            has_raw_value=pi_dict["name"],
                            name=pi_dict["name"],
                            email=pi_dict["email"],
                        ),
                        
                        # sequencing details fields
                        omics_type=nmdc.ControlledTermValue(
                            has_raw_value=project["sequencingStrategy"]
                        ),
                        instrument_name=project["itsSequencingProductName"],
                        processing_institution=self._processing_institute_handler(
                            project["sequencingCenters"]
                        )[0],
                    )
                )
            except:
                logger.error(
                    f"Omics processing set not properly annotated: {project['projectGoldId']}"
                )

    def compute_analysis_project_set(self, analysis_projects: List[str]) -> ApDict:
        """Compute analysis project parameters to be populated from the dataset."""
        # TODO: AP handling code to be revised after
        # determining which slots are actually required
        for ap in analysis_projects:
            mod_date = self.mod_date_handler(ap)

            if re.search("Metagenome Analysis", ap["apType"], re.IGNORECASE):
                self.nmdc_db.metagenome_annotation_activity_set.append(
                    nmdc.MetagenomeAnnotationActivity(
                        id="gold:" + ap["apGoldId"],
                        name=ap["apName"],
                        part_of="gold:" + self.study_id,
                        execution_resource="",
                        git_url="",
                        has_input=["gold:" + ap_i for ap_i in ap["projects"]],
                        has_output="gold:" + ap["apGoldId"],
                        type=ap["apType"],
                        started_at_time=XSDDateTime(ap["addDate"]),
                        ended_at_time=XSDDateTime(mod_date),
                        was_informed_by="",
                        GOLD_analysis_project_identifiers="gold:" + ap["apGoldId"],
                    )
                )

            if re.search("Metatranscriptome Analysis", ap["apType"], re.IGNORECASE):
                self.nmdc_db.metatranscriptome_activity_set.append(
                    nmdc.MetatranscriptomeAnnotationActivity(
                        id="gold:" + ap["apGoldId"],
                        name=ap["apName"],
                        part_of="gold:" + self.study_id,
                        execution_resource="",
                        git_url="",
                        has_input=["gold:" + ap_i for ap_i in ap["projects"]],
                        has_output="gold:" + ap["apGoldId"],
                        type=ap["apType"],
                        started_at_time=XSDDateTime(ap["addDate"]),
                        ended_at_time=XSDDateTime(mod_date),
                        was_informed_by="",
                        GOLD_analysis_project_identifiers="gold:" + ap["apGoldId"],
                    )
                )

    def transform_gold_nmdc(
        self, file_path: Union[str, bytes, os.PathLike] = None
    ) -> str:
        """Transform any dataset fetched from GOLD Database into
        NMDC Schema compliant JSON data.

        :param study_id: Gold study id
        :param file_path: optional file name argument to write JSON dump
            output to
        :return: JSON string
        """
        projects = self.fetch_projects_by_study(self.study_id)
        
        biosamples = self.fetch_biosamples_by_study(self.study_id)

        analysis_projects = self.fetch_analysis_projects_by_study(self.study_id)

        path_to_subset_ids = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "nmdc", "input", "project_ids_subset.txt"
        )

        projects_subset = self.project_ids_subset(path_to_subset_ids)

        if projects_subset:
            # subsetted list of projects filtered
            projects = [proj for proj in projects if proj["projectGoldId"] in projects_subset]
            
            biosamples_subset = [proj["biosampleGoldId"] for proj in projects]

            # subsetted list of biosamples filtered
            biosamples = [
                samp for samp in biosamples if samp["biosampleGoldId"] in biosamples_subset
            ]
        
            # subsetted list of analysis projects filtered
            analysis_projects = [
                ap
                for ap in analysis_projects
                if any(e in projects_subset for e in ap["projects"])
            ]

        study_data = self.fetch_study(id=self.study_id)

        self.compute_study_set(study_data)

        self.compute_biosample_set(study_data, biosamples, projects)

        self.compute_project_set(projects)

        # nit: enable if you want to pull AP information from GOLD
        # self.compute_analysis_project_set(analysis_projects)

        # dump JSON string serialization of NMDC Schema object
        json_str = json_dumper.dumps(self.nmdc_db, inject_type=False)

        # if file_path is provided then additionally write to file at path
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(json.loads(json_str), f, ensure_ascii=False, indent=4)

        return json_str
