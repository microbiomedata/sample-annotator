# -*- coding: utf-8 -*-
import os
from tests import MODEL_DIR, INPUT_DIR, OUTPUT_DIR

from sample_annotator import SampleAnnotator
from sample_annotator.metadata.sample_schema import SampleSchema

"""Test the module can be imported."""

import unittest
import yaml

TEST_DATA = os.path.join(INPUT_DIR, 'test_sample_info.yaml')
REPORT_OUT = os.path.join(OUTPUT_DIR, 'report.tsv')
SAMPLES_OUT = os.path.join(OUTPUT_DIR, 'samples.yaml')

class TestAnnotate(unittest.TestCase):
    """annotation test."""

    def test_annotate(self):
        annotator = SampleAnnotator()
        with open(TEST_DATA) as stream:
            test_obj = yaml.load(stream)

        cumulative_df = None
        output_samples = []
        for t in test_obj.get('tests'):
            desc = t.get('description', None)
            print(f'TEST: {desc}')
            sample = t.get('sample')
            report = annotator.annotate(sample)
            print(report)
            output_samples.append(report.output)
            df = report.as_dataframe()
            #print(df)

            if 'must_pass' in t:
                assert report.passes() == t.get('must_pass')

            mbc = report.messages_by_category()
            #print(mbc)
            expected_failures = t.get('expected_failures', {})
            print(f'EF={expected_failures}')
            for category, num_expected in expected_failures.items():
                num_actual_messages = len(mbc.get(category, []))
                print(f'Expected = {num_expected} actual={num_actual_messages}')
                if isinstance(num_expected, int):
                    assert num_expected == num_actual_messages
                elif num_expected.startswith(">"):
                    assert num_actual_messages > int(num_expected.replace('>',''))
                elif num_expected.startswith("="):
                    assert num_actual_messages == int(num_expected.replace('=',''))
                elif str(num_expected).isdigit():
                    assert num_actual_messages == int(num_expected)
                else:
                    assert False


            if cumulative_df is None:
                cumulative_df = df
            else:
                cumulative_df.append(df)
        cumulative_df.to_csv(REPORT_OUT, sep='\t', index=False)
        with open(SAMPLES_OUT, 'w') as stream:
            yaml.safe_dump(output_samples, stream)


