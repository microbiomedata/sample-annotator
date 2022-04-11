from pathlib import Path

# MAM: numpy is only being used once, for its where method.
#   I bet we can replace that with .loc indexing
import numpy as np
import pandas as pd
# MAM: Using sqlalchemy probably IS a best practice, since it's flexible wrt database backends
#   and encourages object orientated/relational integration, but that's not being used here yet.
#  It might be a little heavyweight here. Could use native sqlite3 library instead
import sqlalchemy

# # MAM: I don't think these are necessary
# import matplotlib.pyplot as plt
# import nltk
# import re
# import seaborn as sns
# import string

# plt.style.use("fivethirtyeight")
# sns.set()
# sns.set_context("talk")

# Setup - Load the SQL extension and connect to the Mini IMDB dataset we've prepared
# MAM what does IMDB stand for here?
# MAM: we should either ask the user where their database file is
#   or the Makefile should contain code to download the file into and expected directory
db_path = Path("/home/mark/biosample_basex_data_good_subset.db")

engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
connection = engine.connect()

# MAM: what's this for?
inspector = sqlalchemy.inspect(engine)

query_name = """
SELECT *
FROM harmonized_wide_sel_envs
"""
harmonized_wide_sel_envs_df = pd.read_sql(query_name, engine)

# Hardcoded rules below
# MAM: this is now the precedent: repaired columns get a _rep suffix
#   and their status metadata gets a _status suffix
# We should consider putting them in a different table lin the database
# the repair rules should come form an external, shared document. they shouldn't be embedded in the code
# see biosamples-annotation-dg sample sheet
#   https://docs.google.com/spreadsheets/d/1c3l487XdqFSZZ8PmptE3t3NHI4uPYoziSn3DX4A8_kA/edit#gid=0
# and https://github.com/turbomam/pygsheets-examples
# the proper_values should come from either the MIxS Google Sheet
#   https://docs.google.com/spreadsheets/d/1QDeeUcDqXes69Y2RjU2aWgOpCVWo5OVsBX9MKmMqi_o/edit?pli=1#gid=178015749&range=33:33
# or the MIxS LinkML model (which is more up to date, nut which we haven't discussed before)
#   https://github.com/GenomicsStandardsConsortium/mixs/blob/74744eea9b149b0587b58f48e3dd8d1f879820d2/model/schema/terms.yaml#L13559-L13567

aerobe_pattern = "^[Aa]erob(ic)*(-){0}"
anaerobe_pattern = "^(anaero)"
facultative_pattern = "^(facultative)"
microaerophilic_pattern = "microaerophilic"  # microaerophilic already looks clean
microanaerobe_pattern = "microanaerobe"  # None microanaerobe ?
obligate_aerobe_pattern = "obligate aerobe"  # None obligate aerobe ?
obligate_anaerobe_pattern = "^obligate anaerobe"  # already looks clean

proper_values = [
    "aerobe",
    "anaerobe",
    "facultative",
    "microaerophilic",
    "microanaerobe",
    "obligate aerobe",
    "obligate anaerobe",
]

harmonized_wide_sel_envs_df["rel_to_oxygen_rep"] = harmonized_wide_sel_envs_df[
    "rel_to_oxygen"
].astype(str)

# MAM: don't test with == False
# use "if not" instead
harmonized_wide_sel_envs_df["rel_to_oxygen_rep_status"] = (
                                                                  harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].isin(
                                                                      proper_values) == False
                                                          ) & (harmonized_wide_sel_envs_df[
                                                                   "rel_to_oxygen_rep"] != "None")

harmonized_wide_sel_envs_df["rel_to_oxygen_rep_status"] = harmonized_wide_sel_envs_df[
    "rel_to_oxygen_rep_status"
].replace(to_replace=False, value="Unchanged")
harmonized_wide_sel_envs_df["rel_to_oxygen_rep_status"] = harmonized_wide_sel_envs_df[
    "rel_to_oxygen_rep_status"
].replace(to_replace=True, value="Repaired")

harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        aerobe_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "aerobe"

harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        anaerobe_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "anaerobe"
harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        facultative_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "facultative"
harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        microaerophilic_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "microaerophilic"
harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        microanaerobe_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "microanaerobe"
harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        obligate_aerobe_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "obligate aerobe"
harmonized_wide_sel_envs_df.loc[
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(
        obligate_anaerobe_pattern, regex=True
    ),
    "rel_to_oxygen_rep",
] = "obligate anaerobe"

# note this UserWarning:
# UserWarning: This pattern is interpreted as a regular expression, and has match groups. To actually get the groups, use str.extract.
#   harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].str.contains(

# harmonized_wide_sel_envs_df.loc[~df["rel_to_oxygen_rep"].isin(proper_values), "rel_to_oxygen_rep"] = None
harmonized_wide_sel_envs_df["rel_to_oxygen_rep"] = np.where(
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"].isin(proper_values),
    harmonized_wide_sel_envs_df["rel_to_oxygen_rep"],
    None,
)

harmonized_wide_sel_envs_df["air_temp"] = harmonized_wide_sel_envs_df[
    "air_temp"
].str.replace(" degree Celsius", "")
harmonized_wide_sel_envs_df["air_temp"] = (
        harmonized_wide_sel_envs_df["air_temp"] + " degree Celsius"
)

# MAM just printing for now
#  need to inject back into the SQLite database
print(harmonized_wide_sel_envs_df)
