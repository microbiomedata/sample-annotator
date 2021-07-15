from datetime import datetime
from typing import Optional, List, Set, Any
from dataclasses import dataclass
import logging
from nmdc_schema.nmdc import QuantityValue
import re
import os
import configparser

SETTINGS_FILENAME = 'settings.ini'
PATH = '.'

@dataclass
class TextMining():
    """
    Text mining Class
    """
    

    def create_settings_file(self, path: str = PATH, ontList: List = ['ENVO']) -> None: 
        """
        Dynamically creates the settings.ini file for OGER to get parameters.

        :param path: Path of the 'nlp' folder
        :param ontList: The ontology to be used as dictionary e.g. ['ENVO', 'CHEBI']
        :return: None.

        -   The 'Shared' section declares global variables that can be used in other sections
            e.g. Data root.
            root = location of the working directory
            accessed in other sections using => ${Shared:root}/

        -   Input formats accepted:
            txt, txt_json, bioc_xml, bioc_json, conll, pubmed,
            pxml, pxml.gz, pmc, nxml, pubtator, pubtator_fbk,
            becalmabstracts, becalmpatents

        -   Two iter-modes available: [collection or document]
            document:- 'n' input files = 'n' output files
            (provided every file has ontology terms)
            collection:- n input files = 1 output file

        -   Export formats possible:
            tsv, txt, text_tsv, xml, text_xml, bioc_xml,
            bioc_json, bionlp, bionlp.ann, brat, brat.ann,
            conll, pubtator, pubanno_json, pubtator, pubtator_fbk,
            europepmc, europepmc.zip, odin, becalm_tsv, becalm_json
            These can be passed as a list for multiple outputs too.

        -   Multiple Termlists can be declared in separate sections
            e.g. [Termlist1], [Termlist2] ...[Termlistn] with each having
            their own paths
        """

        config = configparser.ConfigParser()
        config['Section'] = {}
        config['Shared'] = {}
        
        # Settings required by OGER
        config['Main'] = {
            'include_header' : True,
            'input-directory' : os.path.join(path,'input'),
            'output-directory' : os.path.join(path,'output'),
            'pointer-type' : 'glob',
            'pointers' : '*.tsv',
            'iter-mode' : 'collection',
            'article-format' : 'txt_tsv',
            'export_format': 'tsv',
            'termlist_stopwords': os.path.join(path,'stopwords','stopWords.txt'),
            'termlist_normalize': 'lowercase stem-Porter'
        }

        # Iterate throough ontoList to register paths of corresponding termlists
        for idx, ont in enumerate(ontList):
            termlist_path = 'terms/'+ont.lower()+'_termlist.tsv'
            config.set('Main','termlist'+str(idx+1)+'_filename', termlist_path)
        
        # Write
        with open(os.path.join(path, SETTINGS_FILENAME), 'w') as settings_file:
            config.write(settings_file)

if __name__ == '__main__':
    text_mining = TextMining()
    text_mining.create_settings_file('.', ['ENVO'])