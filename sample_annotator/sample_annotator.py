import json
import click
from typing import Optional, List, Set, Any
from dataclasses import dataclass
import logging
import pandas as pd

from nmdc_schema.nmdc import Biosample, GeolocationValue
from nmdc_schema.nmdc import slots as nmdc_slots

from .geolocation.geotools import GeoEngine
from .measurements.measurements import MeasurementEngine
from .metadata.sample_schema import SampleSchema, underscore
from .report_model import AnnotationReport, Message, PackageCombo, AnnotationMultiSampleReport, Category


from linkml_runtime.linkml_model.meta import ClassDefinition, SchemaDefinition, SlotDefinition, Definition

KEY_ENV_PACKAGE = nmdc_slots.env_package.name
KEY_CHECKLIST = 'checklist'

SAMPLE = dict[str, Any]


@dataclass
class SampleAnnotator():
    """
    TODO
    """

    target_class: ClassDefinition = None
    geoengine: GeoEngine = None
    measurement_engine: MeasurementEngine = MeasurementEngine()

    schema: SampleSchema = SampleSchema()

    def annotate_all(self, samples: List[SAMPLE]) -> AnnotationMultiSampleReport:
        """
        Annotate a list of samples
        """
        reports = []
        amsr = AnnotationMultiSampleReport(reports=[])
        for sample in samples:
            amsr.reports.append(self.annotate(sample))
        return amsr

    def annotate(self, sample: SAMPLE) -> AnnotationReport:
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
        if self.geoengine is None:
            report.add_message('Skipping geo-checks', severity=0)
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
def cli(yamlfile, raw: bool, **args):
    """ Validate input and produce fully resolved yaml equivalent """
    if raw:
        with open(yamlfile, 'r') as stream:
            s = load_raw_schema(stream)
            print(as_yaml(s))
    else:
        gen = YAMLGenerator(yamlfile, **args)
        print(gen.serialize(**args))

if __name__ == '__main__':
    cli()
