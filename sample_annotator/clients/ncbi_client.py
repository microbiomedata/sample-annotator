import json
import logging
import pprint
from dataclasses import dataclass, field
from typing import List, Dict, Any

import click
import click_log
import xmltodict
# requires eutils apps on path?
# https://www.ncbi.nlm.nih.gov/books/NBK179288/
from Bio import Entrez

from sample_annotator.clients.gold_client import GoldClient


# todo could have probably retrieved one XML BioSampleSet for all of the biosamples
# todo this is currently returning dicts with teh same structure as the per-biosample XML
#   decide how to turn into a simple dict
#   request as JSON ion the first place?
#   XQuery within python?


@dataclass
class GoldStudyBiosamples:
    """Class for relating a Gold Study to all of its NCBI Biosample metadata."""
    gold_study_id: str
    entrez_email: str
    gold_nmdc_credentials: str
    gc = GoldClient()
    output_file: str

    bioproject_accessions: Dict[str, Dict] = field(default_factory=dict)
    biosamples_by_accession: Dict[str, Dict] = field(default_factory=dict)

    def set_entrez_email(self):
        Entrez.email = self.entrez_email

    def gold_client_setup(self):
        gc = GoldClient()
        gc.clear_cache()
        gc.load_key(self.gold_nmdc_credentials)
        self.gc = gc

    def get_bioproject_accessions_by_gold_study_id(self):
        projs_of_study = self.gc.fetch_projects_by_study(self.gold_study_id)
        bioproject_accessions = list(
            {i['ncbiBioProjectAccession'] for i in projs_of_study if "ncbiBioProjectAccession" in i})
        for bioproject_accession in bioproject_accessions:
            self.bioproject_accessions[bioproject_accession] = {}
        # https://www.ncbi.nlm.nih.gov/bioproject/PRJEB42019

    def get_bioproject_ids_by_bioproject_accession(self):
        bioproject_accession_keys = list(self.bioproject_accessions.keys())
        for bioproject_accession in bioproject_accession_keys:
            handle = Entrez.esearch(db="bioproject", retmax=10, term=bioproject_accession, idtype="acc")
            record = Entrez.read(handle)
            handle.close()
            for id_val in record['IdList']:
                self.bioproject_accessions[bioproject_accession][id_val] = {}

    def get_biosample_linksets_from_bioproject_id(self) -> None:
        for bioproject_accession, bioproject_ids in self.bioproject_accessions.items():
            for bioproject_id, _ in bioproject_ids.items():
                linksets = self._get_linksets(bioproject_id)
                self._store_linksets(bioproject_accession, bioproject_id, linksets)

    def _get_linksets(self, bioproject_id: str) -> List[Dict[str, Any]]:
        handle = Entrez.elink(
            dbfrom="bioproject", id=bioproject_id, linkname="bioproject_biosample", retmode="json"
        )
        record = handle.read()
        handle.close()
        biosample_links_dict = json.loads(record.decode("utf-8"))
        return biosample_links_dict.get("linksets", [])

    def _store_linksets(self, bioproject_accession: str, bioproject_id: str, linksets: List[Dict[str, Any]]) -> None:
        for linkset in linksets:
            dbfrom = linkset["dbfrom"]
            self.bioproject_accessions[bioproject_accession][bioproject_id][dbfrom] = {}
            for linksetdb in linkset["linksetdbs"]:
                dbto = linksetdb["dbto"]
                self.bioproject_accessions[bioproject_accession][bioproject_id][dbfrom][dbto] = {}
                for link in linksetdb["links"]:
                    self.bioproject_accessions[bioproject_accession][bioproject_id][dbfrom][dbto][link] = {}

    def populate_biosamples(self):
        for bioproject_accession, bioproject_ids in self.bioproject_accessions.items():
            for bioproject_id, bioproject in bioproject_ids.items():
                for dbfrom, linksetdb in bioproject.items():
                    for dbto, links in linksetdb.items():
                        for biosample_id, biosample in links.items():
                            try:
                                biosample_xml_dict = self._fetch_biosample_xml(biosample_id)
                                biosample_accession = biosample_xml_dict['@accession']
                                self.biosamples_by_accession[biosample_accession] = biosample_xml_dict
                                print(biosample_accession)
                            except Exception as e:
                                print(f"Error fetching Biosample {biosample_id}: {e}")

    def _fetch_biosample_xml(self, biosample_id):
        handle = Entrez.efetch(db="biosample", id=biosample_id)
        record = handle.read()
        handle.close()
        biosample_xml_dict = xmltodict.parse(record.decode("utf-8"))['BioSampleSet']['BioSample']
        return biosample_xml_dict

    def write_biosamples_to_file(self):
        with open(self.output_file, 'w') as f:
            json.dump(self.biosamples_by_accession, f, indent=4)


@click.command()
@click_log.simple_verbosity_option()
@click.option('--gold-study', required=True)
@click.option('--entrez-email', required=True)
@click.option('--gold-nmdc-credentials', required=True, type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.option('--output-file', type=click.Path(), required=True)
def cli(gold_study: str, entrez_email: str, gold_nmdc_credentials: str, output_file: str):
    logging.info("Start")

    gsb = GoldStudyBiosamples(gold_study_id=gold_study, entrez_email=entrez_email,
                              gold_nmdc_credentials=gold_nmdc_credentials, output_file=output_file)

    gsb.set_entrez_email()
    gsb.gold_client_setup()
    print(gsb)

    gsb.get_bioproject_accessions_by_gold_study_id()
    print(gsb)
    gsb.get_bioproject_ids_by_bioproject_accession()
    print(gsb)
    gsb.get_biosample_linksets_from_bioproject_id()
    gsb.populate_biosamples()

    gsb.write_biosamples_to_file()

    # pprint.pprint(gsb.biosamples_by_accession)

    logging.info("Finished")


if __name__ == "__main__":
    cli()
