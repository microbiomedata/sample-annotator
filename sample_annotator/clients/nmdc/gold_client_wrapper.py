from cmath import pi
import io
import os
import json
import pkgutil
import logging
import re

from typing import Dict, List, Union

import jsonschema
import pandas as pd
import nmdc_schema.nmdc as nmdc

from linkml_runtime.dumpers import json_dumper
from linkml_runtime.linkml_model.types import XSDDateTime
from sample_annotator.clients.gold_client import GoldClient


logger = logging.getLogger(__name__)  # module level logger


class GoldNMDC(GoldClient):
    def __init__(self, study_id: str) -> None:

        # construct MongoDB with study_set, biosample_set, omics_processing_set
        self.nmdc_db = nmdc.Database()

        # set the GOLD study id
        self.study_id = study_id

    def soil_projects(self) -> List[str]:
        """Get all project ids associated with soil samples from the
        EMP500 study on GOLD.

        :return: list of soil sample project ids
        """
        path_to_soil_ids = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "input", "soil_ids.txt"
        )
        df = pd.read_csv(path_to_soil_ids)

        # this assumes that at all times there is only one
        # column in the soil_ids.txt file
        return df[df.columns.values[0]].to_list()

    def validate_nmdc(
        self, file_name: Union[str, bytes, os.PathLike], database_set: str = None
    ) -> bool:
        """Validate JSON files against the NMDC Schema using the
        jsonschema library.

        :param file_name: path to input JSON file
        :param database_set: optional top level database set
            (e.g, study_set, biosample_set) that contains the data,
            defaults to None
        :return: True if no validation errors are raised, else False
        """
        nmdc_json_schema_bytes = io.BytesIO(
            pkgutil.get_data("nmdc_schema", "nmdc.schema.json")
        )
        nmdc_json_schema = json.loads(nmdc_json_schema_bytes.getvalue())

        with open(file_name, "r") as fh:
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
            os.path.dirname(os.path.abspath(__file__)), "input", "EMP_soil_readQC.json"
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

    def transform_gold_nmdc(
        self, file_name: Union[str, bytes, os.PathLike] = None
    ) -> str:
        """Transform EMP500 data fetched from GOLD Database into
        NMDC Schema compliant JSON data.

        :param study_id: Gold study id
        :param file_name: optional file name argument to write JSON dump
            output to
        :return: JSON string
        """
        projects = self.fetch_projects_by_study(self.study_id)

        soil_projects = self.soil_projects()

        # subsetted list of projects filtered
        # only for soil related GOLD project IDs
        projects = [proj for proj in projects if proj["projectGoldId"] in soil_projects]

        soil_biosamples = [proj["biosampleGoldId"] for proj in projects]

        biosamples = self.fetch_biosamples_by_study(self.study_id)

        # subsetted list of biosamples filtered
        # only for soil related GOLD project IDs
        biosamples = [
            samp for samp in biosamples if samp["biosampleGoldId"] in soil_biosamples
        ]

        study_data = self.fetch_study(id=self.study_id)

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
                        geo_loc_name=nmdc.TextValue(
                            has_raw_value=biosample["geoLocation"]
                        ),
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
                            has_raw_value=biosample["envoMedium"]["id"].replace(
                                "_", ":"
                            )
                        ),
                    )
                )
            except:
                logger.error(
                    f"Biosample not properly annotated: {biosample['biosampleGoldId']}"
                )

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
                        + project["biosampleGoldId"],
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
                        processing_institution=project["sequencingCenters"][0],
                    )
                )
            except:
                logger.error(
                    f"Omics processing set not properly annotated: {project['projectGoldId']}"
                )

        # dump JSON string serialization of NMDC Schema object
        json_str = json_dumper.dumps(self.nmdc_db, inject_type=False)

        # if file_name is provided then additionally write to file at path
        if file_name:
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(json.loads(json_str), f, ensure_ascii=False, indent=4)

        return json_str
