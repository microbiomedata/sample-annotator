# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator.measurements.measurements import MeasurementEngine
from sample_annotator.report_model import AnnotationReport

"""Test the module can be imported."""

import unittest


class TestMeasurements(unittest.TestCase):
    """annotation test."""

    def test_functionality(self):
        report = AnnotationReport(messages=[])
        m = MeasurementEngine()
        qv = m.repair('2cm', report=report)
        print(qv)


