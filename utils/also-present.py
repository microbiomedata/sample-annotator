import requests
import pandas as pd
import sqlite3
import sys

# sqlite_file = "/Users/MAM/Documents/biosample_basex.db"
# not checking in any way yet
sqlite_file = sys.argv[1]

biosample_query = """
select "id" from non_attribute_metadata
"""

flattenation_candiates = [
    #     "id",
    #     "identifier",
    "part_of",
    "alternative_identifiers",
    "GOLD_sample_identifiers",
    "INSDC_biosample_identifiers",
]

shared_out = "assets/shared_ids.tsv"
nmdc_only_out = "assets/nmdc_only.tsv"

current_page = 1
cumulative_bs = []
while True:
    url = f"https://api.dev.microbiomedata.org/biosamples?per_page=200&page={current_page}"
    result = requests.get(url)
    res_dict = result.json()
    res_res = res_dict["results"]
    res_len = len(res_res)
    print(f"page {current_page}: {res_len} biosamples")
    if res_len == 0:
        break
    cumulative_bs = cumulative_bs + res_res
    print(f"cumulative biosamples: {len(cumulative_bs)}")
    current_page = current_page + 1
api_df = pd.DataFrame(cumulative_bs)


def flatten(inner_df, col):
    as_list = list(inner_df[col])
    flattened = ["|".join(i) if isinstance(i, list) else None for i in as_list]
    return flattened


for i in flattenation_candiates:
    print(i)
    api_df[i] = flatten(api_df, i)

api_df["INSDC_biosample_identifiers"] = api_df[
    "INSDC_biosample_identifiers"
].str.upper()

con = sqlite3.connect(sqlite_file)
sqlite_df = pd.read_sql_query(biosample_query, con)

shared = api_df.merge(
    right=sqlite_df, how="left", left_on="INSDC_biosample_identifiers", right_on="id"
)
shared = shared.loc[~shared["INSDC_biosample_identifiers"].isna()]

shared.to_csv(shared_out, sep="\t", index=False)

shared_ids = shared["id_x"]

nmdc_only = api_df.loc[~api_df["id"].isin(shared_ids)]

print(nmdc_only)

# nmdc_only_out
nmdc_only.to_csv(nmdc_only_out, sep="\t", index=False)
