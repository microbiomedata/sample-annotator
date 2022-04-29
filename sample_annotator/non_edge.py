import sqlite3
import pandas as pd
import numpy as np
import re

#biosample.db in this case is already in directory, 
#replace biosample.db with the path of the database.
#in this case, biosample.db contains the env_mapping table that I
#have created.
def to_csv():
    db = sqlite3.connect('biosample.db')
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table_name in tables:
        table_name = table_name[0]
        table = pd.read_sql_query("SELECT * from %s" % table_name, db)
        table.to_csv(table_name + '.csv', index_label='index')
    cursor.close()
    db.close()
to_csv()

#tried dumping env_mapping into biosample instead
map = pd.read_csv('new_env_mapping.csv')
main = pd.read_csv('harmonized_wide_sel_envs.csv')

#fixing with some edge cases
main['env_broad_scale'] = main['env_broad_scale'].str.lower()
main['env_broad_scale'] = main['env_broad_scale'].str.replace('envo:','', regex=True)
main['env_broad_scale'] = main['env_broad_scale'].str.replace('env:','', regex=True)
main['env_broad_scale'] = main ['env_broad_scale'].str.replace('\\(\d+\\)', '', regex=True)
main['env_broad_scale'] = main ['env_broad_scale'].str.replace('\\[\d+\\]', '', regex=True)
main['env_broad_scale'] = main ['env_broad_scale'].str.replace('marine biome ', 'marine biome', regex=True)
main['env_broad_scale'] = main ['env_broad_scale'].str.replace('ocean biome ', 'ocean biome', regex=True)
main['env_broad_scale'] = main['env_broad_scale'].str.replace('marine abyssal zone biome ', 'marine abyssal zone biome', regex=True)
main['env_broad_scale'] = main['env_broad_scale'].str.replace(r"(\d+,?)(\s.+)", r"\1",regex=True)
merge_table = main.merge(map, left_on = 'env_broad_scale', right_on = 'label', how='left')
merge_table['broad_scale_fixed'] = merge_table['env_broad_scale'].astype(str) + ' ' + '['+merge_table['term_id'].astype(str) +']'
merge_table['broad_scale_fixed'] = merge_table['broad_scale_fixed'].str.replace('(\\[nan\\])','', regex=True)

#still need to change the 'nan[nan]' string values to NaN (np.nan) values 
#not sure how
merge_table['broad_scale_fixed'] = merge_table['broad_scale_fixed'].replace('nan[nan]',np.nan)