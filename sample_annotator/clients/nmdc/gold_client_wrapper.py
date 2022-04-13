import io
import os
import json
import pkgutil
import logging

from typing import Union

import jsonschema
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

        biosamples = self.fetch_biosamples_by_study(self.study_id)

        self.nmdc_db.study_set.append(
            nmdc.Study(id=self.study_id, GOLD_study_identifiers=self.study_id)
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
                        
                        # biosample date information
                        add_date=XSDDateTime(biosample["addDate"]),
                        collection_date=nmdc.TimestampValue(
                            has_raw_value=biosample["dateCollected"]
                        ),
                        mod_date=XSDDateTime(biosample["modDate"]),
                        
                        # Earth fields
                        depth=nmdc.QuantityValue(
                            has_numeric_value=biosample["depthInMeters"],
                            has_unit="meters",
                        ),
                        depth2=nmdc.QuantityValue(
                            has_numeric_value=biosample["depthInMeters2"],
                            has_unit="meters",
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
                self.nmdc_db.omics_processing_set.append(
                    nmdc.OmicsProcessing(
                        # omics processing metadata
                        id="gold:" + project["projectGoldId"],
                        name=project["projectName"],
                        GOLD_sequencing_project_identifiers="gold:"
                        + project["biosampleGoldId"],
                        
                        # omics processing date fields
                        add_date=XSDDateTime(project["addDate"]),
                        
                        # sequencing details fields
                        has_input="gold:" + project["biosampleGoldId"],
                        omics_type=nmdc.ControlledTermValue(
                            has_raw_value=project["sequencingStrategy"]
                        ),
                        instrument_name=project["itsSequencingProductName"],
                        processing_institution=project["sequencingCenters"],
                        seq_meth=nmdc.TextValue(has_raw_value=project["seqMethod"]),
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
