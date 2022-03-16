import logging
import pprint
import re
from typing import List, Dict, Any
from urllib.parse import quote

import click
import click_log
import pandas as pd
import requests

import sample_annotator.clients.biosample_sqlite_client as bsq
import sample_annotator.rel_to_oxygen_example as r2oe

# todo align observed values with MIxS enum, with the FAO spec, or with OBO terms?
# todo ontology suggester example?
# todo start refactoring OLS and BP term search code

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option("--sqlite_path", type=click.Path(exists=True), required=True)
@click.option("--mixs_packages_path", type=click.Path(exists=True), required=True)
def cli(sqlite_path: str, mixs_packages_path: str):
    """
    :param mixs_packages_path:
    :param sqlite_path:
    :return:
    """

    fc_term_search = TermSearch()

    mixs_core_frame = pd.read_csv(mixs_packages_path, sep="\t")

    # .squash()
    fao_vs_str = mixs_core_frame.loc[
        mixs_core_frame["Structured comment name"].eq("fao_class")
        & mixs_core_frame["Environmental package"].eq("soil"),
        "Value syntax",
    ].squeeze()

    # # used with different ranges in soil and agriculture!
    # print(fao_vs_str)

    fao_pvs = r2oe.mixs_enum_to_list(fao_vs_str)
    fao_pvs.sort()

    fc_term_search.mixs_permitteds = fao_pvs

    conn = bsq.create_connection(sqlite_path)
    fao_count_q = """
        select
    	fao_class,
    	count(1) as biosample_count
    from
    	harmonized_wide_sel_envs hw
    group by
    	fao_class
    	order by count(1) desc;
    	"""
    fao_count_res = bsq.q_to_frame(conn, fao_count_q)
    fcr_lod = fao_count_res.to_dict(orient="records")

    fc_term_search.raw_query_lod = fcr_lod

    fc_term_search.lod_to_dod()

    dirty = "   A   B   "
    step1 = fc_term_search.norm_one_whitespace(dirty)
    step2 = fc_term_search.lowercase_one(step1)
    print(step2)

    # ontologies=["envo", "obi"],
    fc_term_search.ols_search_one_term(tidied_text="salt water", rows=99)

    # fc_term_search.get_ols_term_details()

    fc_term_search.get_named_response_doc_frame(text='salt water')


if __name__ == "__main__":
    cli()


# todo ols and bp subclasses
# todo any other sources?
# todo use ... optional typing?
class TermSearch:
    def __init__(self):
        self.mixs_permitteds: List[str] = []
        self.raw_query_lod: List[Dict[str, Any]] = []
        self.raw_query_dod: Dict[str, Any] = {}
        self.prepared_query_dict: Dict[str, str] = {}
        self.prepared_query_list: List[str] = []
        self.ols_results: Dict[str, Any] = {}

    def lod_to_dod(self):
        temp_dict = {}
        for i in self.raw_query_lod:
            if i["fao_class"] is not None:
                temp_dict[i["fao_class"]] = {"biosample_count": i["biosample_count"]}
                whitespace_normed = self.norm_one_whitespace(i["fao_class"])
                lower_cased = self.lowercase_one(whitespace_normed)
                temp_dict[i["fao_class"]]["tidied"] = lower_cased
        self.raw_query_dod = temp_dict

    def prepare_queries(self):
        """convert list of dicts with values and counts to ...
        want a list of uniquer queries after tidying (lc, whitespace norm. strip user specified punct)
        may have an entry where the key is None"""
        pass

    def norm_one_whitespace(self, dirty):
        temp = dirty
        temp = re.sub(r"^ +", "", temp)
        temp = re.sub(r" +$", "", temp)
        temp = re.sub(r"\s+", " ", temp)
        return temp

    def lowercase_one(self, dirty):
        temp = dirty
        temp = temp.lower()
        return temp

    def remove_one_puncts(self, dirty):
        pass

    def normalize_one_plurals(self, dirty):
        pass

    def ols_search_one_term(self, tidied_text: str, rows: int, ontologies: List = None):
        # todo get list of valid ontologies
        # todo always a class? {class,property,individual,ontology}
        # todo need to tune punctuation replacement
        # todo need to tune query fields
        # todo should specify return fields, just for efficiency
        # what fields are available?
        # slim
        # 'facet': False, "hl": False
        params = {"q": tidied_text, "type": "class", "exact": False, "obsoletes": False, "local": False, "rows": rows}
        params['fieldList'] = 'iri,ontology_name,ontology_prefix,obo_id,short_form,type,label,synonym,annotations'
        if ontologies is not None:
            params["ontology"] = ",".join(ontologies)
        r = requests.get("http://www.ebi.ac.uk/ols/api/search", params=params)
        rj = r.json()
        # todo not versioning
        # todo add rank
        self.ols_results[tidied_text] = rj

    def get_ols_term_details(self, ontology: str = "obi",
                             raw_iri: str = "http://purl.obolibrary.org/obo/UBERON_0000178"):
        once = quote(raw_iri, safe="")
        twice = quote(once, safe="")
        r = requests.get(f"http://www.ebi.ac.uk/ols/api/ontologies/{ontology}/terms/{twice}")
        rj = r.json()
        pprint.pprint(rj)

    def get_named_response_doc_frame(self, text):
        named_result = self.ols_results[text]
        nr_docs = named_result["response"]["docs"]

        nr_docs_frame = pd.DataFrame(nr_docs)
        print(nr_docs_frame)

        nr_docs_frame.to_csv("nr_docs_frame.tsv", sep="\t", index=False)

# http://www.ebi.ac.uk/ols/

# The search API is independent of the REST API and supports free text search over the ontologies.
# The default search is across all textual fields in the ontology, but results are ranked towards
# hits in labels, then synonyms, then definitions, then annotations.
#
# GET /api/search?q={query}
#
# query
# The terms to search. By default the search is performed over term labels, synonyms, descriptions, identifiers and annotation properties.
# You can override the fields that are searched by supplying a queryFields argument. For example, to query on labels and synonyms use
#
# GET /ols/api/search?q={query}&queryFields={label,synonym}
#
# fieldList
# Specifcy the fields to return, the defaults are {iri,label,short_form,obo_id,ontology_name,ontology_prefix,description,type}
#
# queryFields
# Specifcy the fields to query, the defaults are {label, synonym, description, short_form, obo_id, annotations, logical_description, iri}
#
# groupField
# Set to true to group results by unique id (IRI)
#
# childrenOf
# You can restrict a search to children of a given term. Supply a list of IRI for the terms that you want to search under
#
# allChildrenOf
# You can restrict a search to all children of a given term. Supply a list of IRI for the terms that you want to search under (subclassOf/is-a plus any hierarchical/transitive properties like 'part of' or 'develops from')
#
# rows
# How many results per page
#
# start
# The results page number

# responseHeader -> params
# facet_counts -> facet_fields
# highlighting

# http://purl.obolibrary.org/obo/UBERON_0000178

# Retrieve a term
# Example request
# GET /ols/api/ontologies/{ontology}/terms/{iri}
# Parameter	Description
#
# ontology
# The OLS ontology id e.g. go
#
# iri
# The IRI of the terms, this value must be double URL encoded

# {'fao_class': None, 'biosample_count': 48345}
# {'fao_class': 'Cambisol', 'biosample_count': 405}
# {'fao_class': 'Luvic Phaeozem', 'biosample_count': 54}
# {'fao_class': 'Pellic Vertisol', 'biosample_count': 27}
# {'fao_class': 'Haplic Phaeozem', 'biosample_count': 27}
# {'fao_class': 'arenosols', 'biosample_count': 24}
# {'fao_class': 'Cryosol', 'biosample_count': 7}
# {'fao_class': 'gypsisol', 'biosample_count': 2}
# {'fao_class': 'fao_class', 'biosample_count': 1}
