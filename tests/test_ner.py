import os
from unittest.case import TestCase
from tests import INPUT_DIR
from sample_annotator.text_mining.TextMining import SETTINGS_FILENAME, TextMining
import unittest
import yaml
import pandas as pd
from pandas._testing import assert_frame_equal

PWD = os.path.dirname(os.path.realpath(__file__))
TEST_DATA = os.path.join(INPUT_DIR, 'test_sample_info.yaml')
TMP_DIR = os.path.join(PWD, 'tmp')
NER_INPUT = os.path.join(PWD,'tmp/input')


class TestNER(unittest.TestCase):
    """
    Test runner's NER process
    """
    def test_ner(self):
        
        with open(TEST_DATA) as stream:
            test_obj = yaml.load(stream, Loader=yaml.FullLoader)
        
        cols_1 = ['id', 'text', 'env_broad_scale']
        cols_2 = ['id', 'type', 'term', 'entity_id', 'origin']
        cols_3 = ['id', 'text']
        cols_4 = ['DOCUMENT ID', 'TYPE', 'MATCHED TERM', 'ENTITY ID', 'ORIGIN']
        sample_df = pd.DataFrame(columns=cols_1)
        input_df = pd.DataFrame(columns=cols_1)
        expected_df = pd.DataFrame(columns=cols_2)

        for t in test_obj.get('tests'):
            desc = t.get('description', None)
            if desc == 'TEST(NER)':
                sample = t.get('sample')
                sample_df = sample_df.append(pd.DataFrame([[sample['id'], \
                                                        sample['description'], \
                                                        sample['env_broad_scale']]], \
                                                            columns=cols_1), ignore_index=True)
                reference_output = t.get('output')
                expected_df = expected_df.append(pd.DataFrame([[reference_output['id'], \
                                                                reference_output['type'], \
                                                                reference_output['term'], \
                                                                reference_output['entity_id'], \
                                                                reference_output['origin']]], \
                                                                    columns=cols_2), ignore_index=True)

        input_df = sample_df[cols_3]

        #Export input df
        input_file = os.path.join(NER_INPUT,'input.tsv')
        input_df.to_csv(input_file, sep='\t', index=None)

        # Steps that lead to NER
        textMiner = TextMining()
        textMiner.create_settings_file(TMP_DIR, ['ENVO'])
        textMiner.mine(os.path.join(TMP_DIR,SETTINGS_FILENAME))


        # Read output file
        output_df = pd.read_csv(os.path.join(TMP_DIR,'output/runNER_Output.tsv'), sep= '\t', low_memory=False, usecols=cols_4)
        output_df = output_df.rename(columns={'DOCUMENT ID': 'id', 'TYPE': 'type', 'MATCHED TERM': 'term', 'ENTITY ID': 'entity_id', 'ORIGIN': 'origin' })
        # Pandas testing
        assert_frame_equal(output_df, expected_df)

    
        
