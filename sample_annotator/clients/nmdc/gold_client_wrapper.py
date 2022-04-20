import io
import os
import json
import pkgutil
import logging

from typing import List, Union

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

    def transform_emp500_nmdc(
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

        self.nmdc_db.study_set.append(
            nmdc.Study(
                id=study_data["studyGoldId"],
                description=study_data["description"],
                title=study_data["studyName"],
                GOLD_study_identifiers=study_data["studyGoldId"],
                type="nmdc:Study",
            )
        )

        for biosample in biosamples:
            try:
                self.nmdc_db.biosample_set.append(
                    nmdc.Biosample(
                        # biosample identifiers
                        id="gold:" + biosample["biosampleGoldId"],
                        GOLD_sample_identifiers="gold:" + biosample["biosampleGoldId"],
                        
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
                        mod_date=XSDDateTime(biosample["modDate"]),
                        
                        # Earth fields
                        depth=nmdc.QuantityValue(
                            has_raw_value=biosample["depthInMeters"],
                            has_numeric_value=biosample["depthInMeters"],
                            has_unit="meter",
                        ),
                        depth2=nmdc.QuantityValue(
                            has_raw_value=biosample["depthInMeters2"],
                            has_numeric_value=biosample["depthInMeters2"],
                            has_unit="meter",
                        ),
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
                            term=nmdc.OntologyClass(
                                biosample["envoBroadScale"]["id"].replace("_", ":")
                            )
                        ),
                        env_local_scale=nmdc.ControlledTermValue(
                            term=nmdc.OntologyClass(
                                biosample["envoLocalScale"]["id"].replace("_", ":")
                            )
                        ),
                        env_medium=nmdc.ControlledTermValue(
                            term=nmdc.OntologyClass(
                                biosample["envoMedium"]["id"].replace("_", ":")
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
                pi_dict = next(
                    (
                        contact
                        for contact in project["contacts"]
                        if contact["roles"] == ["PI"]
                    )
                )

                self.nmdc_db.omics_processing_set.append(
                    nmdc.OmicsProcessing(
                        # omics processing metadata
                        id="gold:" + project["projectGoldId"],
                        name=project["projectName"],
                        GOLD_sequencing_project_identifiers="gold:"
                        + project["biosampleGoldId"],
                        ncbi_project_name=project["projectName"],
                        type="nmdc:OmicsProcessing",
                        
                        # omics processing date fields
                        add_date=XSDDateTime(project["addDate"]),
                        mod_date=XSDDateTime(project["modDate"]),
                        principal_investigator=nmdc.PersonValue(
                            has_raw_value=pi_dict["name"],
                            name=pi_dict["name"],
                            email=pi_dict["email"],
                        ),
                        
                        # sequencing details fields
                        has_input="gold:" + project["biosampleGoldId"],
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
