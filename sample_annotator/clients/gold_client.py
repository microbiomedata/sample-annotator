import json
import sys
import yaml
import logging
from typing import Tuple, List, Dict, Any, Union, TextIO

import click
import requests
from requests.auth import HTTPBasicAuth

USERPASS = Tuple[str, str]
URL = str
#JSON = Union[Dict[str, Any], List[Dict[str, Any]]]
JSON = Any
SampleDict = JSON
StudyDict = JSON

class GoldClient:
    """
    A wrapper for fetching from the GOLD API

    `Gold Service Docs <https://docs.google.com/document/d/1PgrFYmc7AU7Kd5Dtg-xbpAyC6ZcLw4ChFwg3bHV1JQg/edit>`_

    """

    gold_key: USERPASS = None
    url: URL = "https://gold.jgi.doe.gov/rest/nmdc"

    def load_key(self, path: str) -> None:
        """
        Loads username/password key from a path

        Should be a one-line file with entry USER:PASS
        :param path:
        :return: username-password
        """
        with open(path) as stream:
            lines = stream.readlines()
            [user, passwd] = lines[0].strip().split(':')
            self.gold_key = user, passwd

    def _normalize_id(self, id: str) -> str:
        """
        Translates a CURIE into LocalId form

        :param id: CURIE or LocalId
        :return: LocalId
        """
        return id.replace('gold:', '')

    def _call(self, endpoint: str, params: Dict = {}) -> JSON:
        (user, passwd) = self.gold_key
        endpoint_url = f'{self.url}/{endpoint}'
        results = requests.get(endpoint_url,
                               params=params,
                               auth=HTTPBasicAuth(user, passwd))
        logging.info(f'STATUS={results.status_code}')
        if results.status_code != 200:
            raise Exception(f'API call to {endpoint_url} failed, code={results.status_code}')
        return results.json()

    def fetch_biosamples_by_study(self, id: str) -> List[SampleDict]:
        """

        :param id: e.g. Gs0144570
        :return: List of sample Dict objects
        """
        id = self._normalize_id(id)
        results = self._call('biosamples', {'studyGoldId': id})
        return results

    def fetch_study(self, id: str, include_biosamples=False) -> StudyDict:
        """
        :param id: E.g. Gs0144570
        :param include_biosamples: if true, will also inject all biosamples for study
        :return:
        """
        id = self._normalize_id(id)
        results = self._call('studies', {'studyGoldId': id})
        study = results[0]
        if include_biosamples:
            study['biosamples'] = self.fetch_biosamples_by_study(id)
        return study

    def fetch_studies(self, ids: List[str], **kwargs) -> List[StudyDict]:
        """
        Fetches multiple studies

        :param ids:
        :param kwargs:
        :return:
        """
        return [self.fetch_study(id, **kwargs) for id in ids]

    def fetch_studies_from_file(self, path: str, **kwargs) -> List[StudyDict]:
        """

        :param path:
        :param kwargs:
        :return:
        """
        ids = []
        with open(path) as file:
            for line in file:
                if line.startswith('Gs'):
                    ids.append(line.strip())
        return self.fetch_studies(ids, **kwargs)

@click.group()
@click.option("-v", "--verbose", count=True)
@click.option("-q", "--quiet")
def main(verbose: int, quiet: bool):
    """Main."""
    if verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)
    if quiet:
        logging.basicConfig(level=logging.ERROR)


@main.command()
@click.argument('idfile')
@click.option('--output', '-o',
              type=click.File(mode="w"),
              default=sys.stdout,
              help="Path to output file")
@click.option("-O",
              "--output-format",
              default='yaml',
               help=f'Desired output format: json or yaml')
@click.option('--include-biosamples/--no-include-biosamples',
              default=False,
              help="if set, include full biosamples")
@click.option('--authentication-file', '-A',
              default='config/gold-key.txt',
              help="Path auth file. Contents should be user:pass")
def fetch_studies(idfile, output: TextIO, output_format, authentication_file, **args):
    """
    Fetch studies from gold

    E.g.

    Fetch all studies where IDs are in a file:
        goldapi fetch-studies -A config/gold-key.txt tests/inputs/gold-studies-subset.tsv

    As above, save as YAML, include sample data:
        goldapi fetch-studies -O yaml -o foo.yaml --include-biosamples -A config/gold-key.txt tests/inputs/gold-studies-subset.tsv
    """
    logging.info(f'Additional args: {args}')
    gc = GoldClient()
    gc.load_key(authentication_file)
    studies = gc.fetch_studies_from_file(idfile, **args)
    logging.info(f'Retrieved {len(studies)} studies')
    with output as stream:
        if output_format == 'yaml':
            yaml.dump(studies, stream=stream, default_flow_style=False, sort_keys=False)
        else:
            json.dump(studies, stream, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()