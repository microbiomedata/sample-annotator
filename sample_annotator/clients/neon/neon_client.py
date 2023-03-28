import pprint

import requests
from typing import Dict, List


class NeonClient:
    def __init__(self):
        self.product_summaries: Dict = {}
        self.products: Dict = {}
        self.releases: Dict = {}
        self.site_summaries: Dict = {}
        self.sites: Dict = {}

    def populate_products(self) -> None:
        response = requests.get('https://data.neonscience.org/api/v0/products')
        self.products = response.json()

    def populate_releases(self) -> None:
        response = requests.get('https://data.neonscience.org/api/v0/releases')
        self.releases = response.json()

    def populate_sites(self) -> None:
        response = requests.get('https://api.neonscience.org/v0/sites')
        self.sites = response.json()

    def add_site_summary(self, site: str, release: str = "RELEASE-2023") -> None:
        url = f"https://data.neonscience.org/api/v0/sites/{site}?release={release}"
        site_summary = requests.get(url).json()
        self.site_summaries[site] = site_summary

    def add_product_summary(self, product: str, release: str = "RELEASE-2023") -> None:
        url = f"https://data.neonscience.org/api/v0/products/{product}?release={release}"
        prod_summary = requests.get(url).json()
        self.product_summaries[product] = prod_summary

    def get_release_names(self) -> List[str]:
        if not self.releases:
            self.populate_releases()
        return [release['release'] for release in self.releases['data']]

    def get_product_code_to_product_name(self) -> Dict[str, str]:
        if not self.products:
            self.populate_products()
        pctpn = {i['productCode']: i['productName'] for i in self.products['data']}
        pctpn_sorted = sorted(pctpn.items(), key=lambda x: x[1])
        pctpn_sorted_dict = dict(pctpn_sorted)
        return pctpn_sorted_dict

    def get_codes_names_by_string(self, search_string: str) -> Dict[str, str]:
        codes_names_by_string = {}
        pctpn_sorted_dict = self.get_product_code_to_product_name()
        for k, v in pctpn_sorted_dict.items():
            if search_string.lower() in v.lower():
                codes_names_by_string[k] = v
        return codes_names_by_string

# nc = NeonClient()
#
# x = nc.get_product_code_to_product_name()
#
# pprint.pprint(x)