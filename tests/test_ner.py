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
        
        cols = ['DOCUMENT ID', 'TYPE', 'MATCHED TERM', 'ENTITY ID', 'ORIGIN']

        for t in test_obj.get('tests'):
            source = None
            desc = t.get('description', None)
            if desc == 'TEST(NER)':
                sample_df = pd.DataFrame()
                input_df = pd.DataFrame()
                expected_df = pd.DataFrame()
                sample = t.get('sample')
                sample_df = pd.DataFrame([sample])
                expected_output = t.get('output')
                expected_df = pd.DataFrame([expected_output])

                if 'description' in sample_df.columns:
                    new_sample_df = sample_df.rename(columns={'description': 'text'})
                    source = 'description'
                elif 'env_broad_scale'in sample_df.columns:
                    new_sample_df = sample_df.rename(columns={'env_broad_scale': 'text'})
                    source = 'env_broad_scale'
                else:
                    new_sample_df = sample_df

                input_df = new_sample_df[['id', 'text']]

                #Export input df
                input_file = os.path.join(NER_INPUT,'input.tsv')
                input_df.to_csv(input_file, sep='\t', index=None)

                # Steps that lead to NER
                textMiner = TextMining()
                textMiner.create_settings_file(TMP_DIR, ['ENVO'])
                textMiner.mine(os.path.join(TMP_DIR,SETTINGS_FILENAME))


                # Read output file
                output_df = pd.read_csv(os.path.join(TMP_DIR,'output/runNER_Output.tsv'), sep= '\t', low_memory=False, usecols=cols)
                output_df = output_df.rename(columns={'DOCUMENT ID': 'id', 'TYPE': 'type', 'MATCHED TERM': 'term', 'ENTITY ID': 'entity_id', 'ORIGIN': 'origin' })
                
                # Get exact match
                if source == 'description':
                    # TODO: make a more robust test
                    # Pandas testing
                    assert_frame_equal(output_df, expected_df)
                elif source == 'env_broad_scale':
                    output_df = output_df.loc[output_df['term'] == sample_df.iloc[0][source]]
                    assert(output_df['entity_id'][0] == expected_df[source][0])

    
        
