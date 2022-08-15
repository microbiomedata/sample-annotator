import os
import pprint

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv('../../../local/.env')

user = os.getenv('nmdc_gold_api_user')
password = os.getenv('nmdc_gold_api_password')

endpoint_url = 'https://gold.jgi.doe.gov/rest/nmdc/biosamples'

params = {"studyGoldId": "Gs0110119"}

results = requests.get(
    endpoint_url, params=params, auth=HTTPBasicAuth(user, password)
)

rj = results.json()

biosampleGoldIds = [i['biosampleGoldId'] for i in rj]

pprint.pprint(biosampleGoldIds)