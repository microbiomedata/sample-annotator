"""
examine strings from biosample metadata and return matching MIxS traid terms if possible

MIxS trad columns = env_broad_scale, env_local_scale, env_medium
Might want to look at the harmonized_table.db created by https://github.com/INCATools/biosample-analysis
path = XXX
query = XXX

Basic methods are provided fro extracting OBO foundry term IDs from strings,
including obtaining label/term reference data
Does not yet obtain synonyms etc.
alternatives: deeper parse of JSON
direct rdftab, semantic sql... setup
ols, bioportal term lookup or search... slower
"""
import json
import re
from typing import Optional

import pandas as pd
import requests
from src.curieutil import CurieUtil
from strsimpy.cosine import Cosine

from sample_annotator.ontology.package_checklist_normalizer import standard_tidy

prefix_commons_context_url = 'https://raw.githubusercontent.com/prefixcommons/biocontext/master/registry' \
                             '/obo_context.jsonld'
envo_json_url = "http://purl.obolibrary.org/obo/envo.json"
cosine_ngram_size = 2
qualified_stringdist = Cosine(cosine_ngram_size)


# put some caching mechanism in place
# what to do with NULLs and empty strings?
# also see notes in sample_annotator/ontology/package_checklist_normalizer.py

def get_web_json(url: str) -> dict:
    r = requests.get(url)
    t = r.text
    json_dict = json.loads(t)
    return json_dict


prefix_commons_context_res = requests.get(prefix_commons_context_url)
prefix_commons_context_mapping = CurieUtil.parseContext(prefix_commons_context_res.json())
curie_mapper = CurieUtil(prefix_commons_context_mapping)

envo_dict = get_web_json(envo_json_url)
envo_label_frame = pd.DataFrame(envo_dict['graphs'][0]['nodes'])
envo_label_frame = envo_label_frame[['id', 'lbl']]
# convert IRIs <-> CURIEs
envo_label_frame['curie'] = envo_label_frame['id'].apply(curie_mapper.getCurie)
envo_label_frame['tidied'] = envo_label_frame['lbl'].apply(standard_tidy)


def get_bio_registry_content(url="https://raw.githubusercontent.com/bioregistry/bioregistry/main/src/bioregistry/data"
                                 "/bioregistry.json") -> dict:
    br_dict = get_web_json(url)
    return br_dict


# this should be global ie not re-downloaded and recreated for each normalization
bio_registry_content = get_bio_registry_content()


def get_pattern_from_bioregistry(br_dict, prefix: str, authority="miriam") -> str:
    """
    Look in the bioregistry for a regular expression that describes the IDs in an ontology

    convert prefix and authority to lowercase?
    some authorities have patterns and others don't
    some may be more specific than others
    the pattern will probably be bookended by ^ and $
    will probably want to remove those
    could these tests be more compact?
    what will be returned in any of the fall-through cases?
    """

    if isinstance(br_dict, dict):
        if prefix in br_dict.keys():
            if isinstance(br_dict[prefix], dict) and authority in br_dict[prefix].keys():
                if isinstance(br_dict[prefix][authority], dict) and 'pattern' in br_dict[prefix][authority].keys():
                    pattern = br_dict[prefix][authority]['pattern']
                    pattern = re.sub(r'^\^', '', pattern)
                    pattern = re.sub(r'\$$', '', pattern)
                    return pattern


envo_patttern = get_pattern_from_bioregistry(bio_registry_content, "envo")


def extract_curie(input_str, term_pattern=envo_patttern) -> dict:
    """
    if possible, extract an OBO foundry term ID from a string

    return the ID and the string separately
    include or exclude brackets from match?
    we have seen cases where envo was misspelled
    or there was a textual local part
    or other edge cases...

    should we attempt to handle multiple matches?
    """
    term_search = re.search(term_pattern, input_str)

    if term_search:
        term_match = term_search.group(0)
        depleted = re.sub(term_match, '', input_str)
        depleted = standard_tidy(depleted)
        asserted_label = envo_label_frame.loc[envo_label_frame['curie'] == term_match, "lbl"]
        if len(asserted_label) == 1:
            al_val = asserted_label.squeeze()
            return {"depleted": depleted, "term_match": term_match, "asserted_label": al_val}
        else:
            return {"depleted": depleted, "term_match": term_match}
    else:
        tidied = standard_tidy(input_str)
        return {"tidied": tidied}


def calculate_cosine_dist(a: str, b: str) -> float:
    stringdist_res = qualified_stringdist.distance(a, b)
    return stringdist_res


def normalize_triad_slot(raw_triad_value: str) -> Optional[str]:
    """
    This is the main method for normalizing strings about the environments from which biosamples were obtained

    returned pattern should contain one or more pipe-delimited patterns like this:
    OBO foundry label and [term id] (usually EnvO)
    for example "terrestrial biome [ENVO:00000446]|marine biome [ENVO:00000447]"

    Currently extracts embedded OBO foundry term IDs and pulls in matching label
    Also for exact string matches with labels and synonyms in ontology JSON file or rdftab database?
    Apply this to ontologies other than EnvO?
    """
    curie_extract_res = extract_curie(raw_triad_value, envo_patttern)
    if "term_match" in curie_extract_res.keys():
        # confirm that the ID and the label match?
        # currently using envo's json ontology file. alternatives inc. rdftab.
        depleted_asserted_dist = calculate_cosine_dist(curie_extract_res['depleted'],
                                                       standard_tidy(curie_extract_res['asserted_label']))
        print(curie_extract_res['depleted'])
        print(standard_tidy(curie_extract_res['asserted_label']))
        print(depleted_asserted_dist)
        placeholder = curie_extract_res['asserted_label'] + " [" + curie_extract_res['term_match'] + "]"
    else:
        placeholder = curie_extract_res['tidied']
    return placeholder
