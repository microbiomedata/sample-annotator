import quantulum3
import batch_utils.batch_utils as bu
import pandas as pd
import sqlite3

pd.set_option("display.max_columns", None)

# determine which INDC biosample fields/columns are applicable to q3 extraction
# in NMDC we would call them quantity values
# something with a value and a unit

# could get the information from the (legacy?) MIxS Google Sheet
# or the linkml model
# put those functionalities in a package

# formatting with Black

# add this in manually?
# size_frac	size fraction selected	Filtering pore size used in sample preparation	filter size value range	{float}-{float} {unit}
# don't forget other unit bearers from the env packs sheet
curated_mvs = ["size_frac"]

mixs_core_df = bu.get_df_from_gsheets_csv(
    "1QDeeUcDqXes69Y2RjU2aWgOpCVWo5OVsBX9MKmMqi_o", "178015749"
)
mixs_env_packs_df = bu.get_df_from_gsheets_csv(
    "1QDeeUcDqXes69Y2RjU2aWgOpCVWo5OVsBX9MKmMqi_o", "750683809"
)

if_exists = "replace"

# mixs_core_df.to_csv("mixs_core_df.tsv", sep="\t", index=False)

# print(mixs_core_df.columns)
# Index(['Structured comment name', 'Item (rdfs:label)', 'Definition',
#        'Expected value', 'Value syntax', 'Example', 'Section', 'migs_eu',
#        'migs_ba', 'migs_pl', 'migs_vi', 'migs_org', 'mims', 'mimarks_s',
#        'mimarks_c', 'misag', 'mimag', 'miuvig', 'Preferred unit', 'Occurence',
#        'MIXS ID', 'MIGS ID (mapping to GOLD)', ' '],
#       dtype='object')

# what are the different kinds of Expected values and Value syntaxes?
temp = mixs_core_df["Expected value"].value_counts()
temp = temp[temp > 1]

# enumeration                                           19
# measurement value                                      6
# names and versions of software(s), parameters used     4
# text                                                   3
# PMID, DOI or URL                                       3
# value                                                  2
# software name, version and relevant parameters         2
# names and versions of software(s) used                 2
# boolean                                                2
# name and version of software, parameters used          2
# Name: Expected value, dtype: int64

temp = mixs_core_df["Value syntax"].value_counts()
temp = temp[temp > 1]

# {text}                               16
# {software};{version};{parameters}     8
# {integer}                             7
# {float} {unit}                        6
# {PMID}|{DOI}|{URL}                    5
# {termLabel} {[termID]}|{text}         4
# {termLabel} {[termID]}                4
# {boolean}                             2
# Name: Value syntax, dtype: int64

# any subset of sections?
# print(mixs_core_df['Section'].value_counts())

# Name: Value syntax, dtype: int64
# sequencing                      57
# nucleic acid sequence source    25
# environment                     10
# investigation                    6
# Name: Section, dtype: int64

# # are we interested in any checklists besides mims?

mixs_core_mvs_df = mixs_core_df.loc[
    mixs_core_df["Expected value"].eq("measurement value"),
    [
        "Structured comment name",
        "Item (rdfs:label)",
        "Expected value",
        "Value syntax",
        "Section",
    ],
]
mixs_core_mvs_df["Environmental package"] = None
mixs_core_mvs_df.rename(columns={"Item (rdfs:label)": "Item title"}, inplace=True)

# any subset of packages?

# mixs_env_packs_df = bu.get_df_from_gsheets_csv("1QDeeUcDqXes69Y2RjU2aWgOpCVWo5OVsBX9MKmMqi_o", "750683809")
# print(mixs_env_packs_df.columns)
# Index(['Environmental package', 'Structured comment name', 'Package item',
#        'Definition', 'Expected value', 'Value syntax', 'Example', 'Section',
#        'Requirement', 'Preferred unit', 'Occurrence', 'MIXS ID',
#        'github ticket', 'Unnamed: 13'],
#       dtype='object')


mixs_env_packs_mvs_df = mixs_env_packs_df.loc[
    mixs_env_packs_df["Expected value"].eq("measurement value"),
    [
        "Structured comment name",
        "Package item",
        "Expected value",
        "Value syntax",
        "Section",
        "Environmental package",
    ],
]
mixs_env_packs_mvs_df.rename(columns={"Package item": "Item title"}, inplace=True)

mixs_all_mvs_df = pd.concat([mixs_core_mvs_df, mixs_env_packs_mvs_df])

mixs_all_mvs = list(set(mixs_all_mvs_df["Structured comment name"]))
mixs_all_mvs = mixs_all_mvs + curated_mvs
mixs_all_mvs.sort()

con = sqlite3.connect("/Users/MAM/biosample_basex_data_good_subset.db")
observed_cols = bu.get_sqlite_colnames(con, "harmonized_wide_sel_envs")

observed_mvs = list(set(mixs_all_mvs).intersection(set(observed_cols)))
observed_mvs.sort()

# bu.do_q3_one_col("elev", con, "harmonized_wide_sel_envs")
df_list = []
for i in observed_mvs:
    df_list.append(bu.do_q3_one_col(i, con, "harmonized_wide_sel_envs"))
catted = pd.concat(df_list)

catted.to_sql("q3_lookup", con, if_exists=if_exists, index=False)

# if quantulum3 thought it saw multiple quantities with units, only the first is reported in this database table
#   however, the total number of potential hits is reported (see below)
# columns
# raw: a unique value found in one of the measurement value columns
# surface: the portion of that raw value that quantulum3 tried to process
# value: the quantity part that quantulum3 parsed out
# unit_name: the unit part that quantulum3 parsed out
#   see also...
# hit_count: how many quantity/unit paris did quantulum3 think it could parse of out the raw text?
# uncertainty: did quantulum3 think it saw a range of values or a plus/minus in the raw text?
# char_coverage: what fraction of raw is covered by the surface?
# calc_min: if quantulum3 saw a range of values, what is the lower bound of that range?
# calc_max: if quantulum3 saw a range of values, what is the upper bound of that range
