import requests
import pandas as pd

# get this from user
session_cookie_val = ""

url = "https://data.dev.microbiomedata.org/api/metadata_submission"

cookies = {"session": session_cookie_val}
params = {'offset': 0,
          'limit': 3}

response = requests.get(url, cookies=cookies)
rj = response.json()

# print(rj.keys())
# # dict_keys(['count', 'results'])

total_submissions = rj['count']
submissions_list = rj['results']

# print(submissions_list[0].keys())
# # dict_keys(['metadata_submission', 'status', 'id', 'author_orcid', 'created'])

external_keys = ['status', 'id', 'author_orcid', 'created']
inner_key = 'metadata_submission'

# print(submissions_list[0][inner_key].keys())
# # ['template', 'studyForm', 'sampleData', 'multiOmicsForm'

# print(submissions_list[0][inner_key]['sampleData'])
# # list of lists

df = pd.DataFrame(submissions_list[0][inner_key]['sampleData'])

print(df)

# for i in
