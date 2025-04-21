import json
import logging
import re
import sys
from dataclasses import dataclass, field
from typing import List

import bioregistry
import click
from linkml_runtime.linkml_model.meta import ClassDefinition
from nmdc_schema.nmdc import slots as nmdc_slots

from sample_annotator.geolocation.geotools import GeoEngine
from sample_annotator.metadata.sample_schema import SampleSchema
from .measurements.measurements import MeasurementEngine
from .metadata.sample_schema import underscore
from .report_model import AnnotationReport, PackageCombo, AnnotationMultiSampleReport, Category, SAMPLE, STUDY

KEY_ENV_PACKAGE = nmdc_slots.env_package.name
KEY_CHECKLIST = 'checklist'
KEY_LAT_LON = nmdc_slots.lat_lon.name
KEY_ELEV = nmdc_slots.elev.name


@dataclass
class SampleAnnotator():
    """
    TODO
    """

    target_class: ClassDefinition = None
    geoengine: GeoEngine = field(default_factory=GeoEngine)
    measurement_engine: MeasurementEngine = field(default_factory=MeasurementEngine)

    schema: SampleSchema = field(default_factory=SampleSchema)

    def annotate_all(self, samples: List[SAMPLE], study: STUDY = None) -> AnnotationMultiSampleReport:
        """
        Annotate a list of samples
        """
        reports = []
        amsr = AnnotationMultiSampleReport(reports=[])
        for sample in samples:
            amsr.reports.append(self.annotate(sample, study=study))
        return amsr

    def annotate(self, sample: SAMPLE, study: STUDY = None) -> AnnotationReport:
        """
        Annotate a sample

        Returns an AnnotationReport object that includes a transformed sample representation,
        plus reports of all errors/warnings found, and repairs made

        Performs a sequential series of tidy activities. Each report
        """
        report = AnnotationReport()
        report.messages = []
        report.input = sample
        sample = sample.copy()
        self.validate_identifier(sample, report)
        self.infer_package(sample, report)
        self.tidy_nulls(sample, report)
        self.tidy_keys(sample, report)
        self.tidy_enumerations(sample, report)
        self.tidy_measurements(sample, report)
        self.perform_text_mining(sample, report)
        self.perform_geolocation_inference(sample, report)
        self.perform_inference(sample, report)
        report.output = sample
        return report

    def validate_identifier(self, sample: SAMPLE, report: AnnotationReport):
        id_fields = ['id', 'source_mat_id', 'identifier']
        id = None
        id_field = None
        for f in id_fields:
            if f in sample:
                if id is None:
                    id = sample.get(f)
                    id_field = f
                else:
                    report.add_message(f'Multiple ID fields: {f}={sample.get(f)} already set to {id}')
        if id is None:
            report.add_message(f'No identifier set', severity=2, category=Category.Identifier)
        else:
            if ':' in id:
                parts = id.split(':')
                if len(parts) > 2:
                    report.add_message(f'Invalid CURIE syntax; multiple parts = {parts}',
                                       severity=2,
                                       category=Category.Identifier)
                prefix = parts[0]
                local_id = parts[1]
                # TODO: cache to avoid multiple calls
                normalized_prefix = bioregistry.normalize_prefix(prefix)
                if normalized_prefix is None:
                    report.add_message(f'No such prefix: {normalized_prefix}',
                                       severity=2,
                                       category=Category.Identifier)
                else:
                    if normalized_prefix != prefix:
                        report.add_message(f'Normalizing prefix {prefix} => {normalized_prefix}',
                                           severity=1,
                                           was_repaired=True,
                                           category=Category.Identifier)
                        id = id.replace(prefix, normalized_prefix)
                    pattern = bioregistry.get_pattern(normalized_prefix)
                    if pattern is not None:
                        logging.debug(f'Testing {id} against {pattern}')
                        if not re.match(pattern, id):
                            report.add_message(f'ID {id} does not match {pattern}',
                                               severity=1,
                                               category=Category.Identifier)
            else:
                report.add_message(f'ID is not a CURIE')
            report.sample_id = id
            sample['id'] = id

    def infer_package(self, sample: SAMPLE, report: AnnotationReport):
        """
        Infer the environment package / checklist combo, either
        from directly asserted fields, or other means
        """
        package = sample.get(KEY_ENV_PACKAGE, None)
        checklist = sample.get(KEY_CHECKLIST, None)
        if package is None:
            report.add_message(f'No package specified', category=Category.MissingCore)
        if checklist is None:
            report.add_message(f'No checklist specified')
        report.package = PackageCombo(environmental_package=package,
                                      checklist=checklist)

    def tidy_nulls(self, sample: SAMPLE, report: AnnotationReport):
        """
        Normalizes to EBI standard null values

        https://ena-docs.readthedocs.io/en/latest/submit/samples/missing-values.html
        """
        for k, v in sample.copy().items():
            if v is None or str(v).strip() == '' or v == []:
                del sample[k]
        # TODO: use missing-value vocabulary

    def tidy_keys(self, sample: SAMPLE, report: AnnotationReport):
        """
        Performs tidying on all keys/fields/slots in the sample dictionary

         - uses mappings, e.g. between MIxS5 vs 6
         - performs case normalization
        """
        schema = self.schema
        for orig_k, v in sample.copy().items():
            k = underscore(orig_k)
            if k != orig_k:
                report.add_message(f'Key not underscored: {orig_k}', was_repaired=True)
                sample[k] = v
                del sample[orig_k]
        for k, v in sample.copy().items():
            slot = schema.get_slot(k)
            if slot is None:
                slot = schema.get_slot(k, use_aliases=True)
                if slot is None:
                    report.add_message(f'Invalid field: {k}', category=Category.UnknownField)
                else:
                    new_k = slot.get('name')
                    report.add_message(f'Alias used: {k} => {new_k}', was_repaired=True)
                    sample[new_k] = v
                    del sample[k]

    def tidy_measurements(self, sample: SAMPLE, report: AnnotationReport):
        """
        Tidies measurement fields
        """
        me = self.measurement_engine
        schema = self.schema
        for k, v in sample.copy().items():
            if schema.is_measurement_field(k):
                qv = me.repair(v, report=report)
                sample[k] = qv

    def tidy_enumerations(self, sample: SAMPLE, report: AnnotationReport):
        """
        Tidies measurement fields
        """
        schema = self.schema
        for k, v in sample.items():
            enum = schema.get_enumerations(k)
            if enum is not None:
                pvs = enum.get('permissible_values', {})
                if v not in pvs.keys():
                    report.add_message(f'Value {v} is not in enum: {pvs.keys()}',
                                       category=Category.ControlledVocabulary)
                    # TODO: use basic NER to repair
                else:
                    pv = pvs.get(v)
                    # TODO: use meaning field

    def perform_text_mining(self, sample: SAMPLE, report: AnnotationReport):
        """
        Performs text mining
        """
        # TODO: Mark and Harshad to populate
        ...

    def perform_geolocation_inference(self, sample: SAMPLE, report: AnnotationReport):
        """
        Performs inference using geolocation information
        """
        # TODO: Stan to populate
        ll_str = sample.get(KEY_LAT_LON, None)
        if ll_str is None:
            report.add_message(f'No lat_long specified',
                               severity=3,
                               category=Category.MissingCore)
            return
        try:
            lat_lon = tuple([float(x.strip()) for x in ll_str.split(' ')])
        except:
            report.add_message(f'Incorrect format for lat_lon: {ll_str}', severity=3)
            return
        sample[KEY_LAT_LON] = {'latitude': lat_lon[0], 'longitude': lat_lon[1]}

        ge = self.geoengine
        soiltype = ge.get_fao_soil_type(lat_lon)
        report.add_message('Soil type is ' + soiltype, severity=0)

        logging.info('Using geoengine')
        elev = ge.get_elevation(lat_lon)
        if len(elev) > 0:
            report.add_message(f'Filling in missing value for elevation {elev}',
                               was_repaired=True)
            sample[KEY_ELEV] = {'has_unit': 'meter',
                                'has_numeric_value': elev}
        return

    def perform_inference(self, sample: SAMPLE, report: AnnotationReport):
        """
        Performs Machine Learning inference
        """
        # TODO: @realmarcin and @wdduncan to populate
        ...


@click.command()
@click.option("--validateonly/--generate", "-v/-g", default=False,
              help="Just validate / generate output (default: generate)")
@click.option("--output", "-s",
              help="JSON for tidied samples")
@click.option("--report-file", "-R",
              help="report file")
@click.option("--googlemaps-api-key-path", "-G",
              help="path to file containing google maps API KEY")
@click.option("--bioportal-api-key-path", "-B",
              help="path to file containing bioportal API KEY")
@click.argument("samplefile")
def cli(samplefile: str, output: str = None, report_file: str = None,
        validateonly: bool = False,
        googlemaps_api_key_path: str = None, bioportal_api_key_path: str = None):
    """
Annotate a file of samples, producing a "repaired"/enhanced sample file as output, together
with a report

The input file must be a JSON fine containing an array of dicts
    """
    annotator = SampleAnnotator()
    if googlemaps_api_key_path:
        annotator.geoengine = GeoEngine()
        annotator.geoengine.load_key(googlemaps_api_key_path)
    with open(samplefile) as stream:
        samples = json.load(stream)
    report = annotator.annotate_all(samples)
    df = report.as_dataframe()
    if report_file is not None:
        df.to_csv(report_file, sep='\t', index=False)
    else:
        sys.stderr.write(df.to_csv(sep='\t', index=False))
    out_json = json.dumps(report.all_outputs(), indent=4, sort_keys=True)
    if output is not None:
        with open(output, 'w') as stream:
            stream.write(out_json)
    else:
        # print(out_json)
        print('Done')


if __name__ == '__main__':
    cli()
