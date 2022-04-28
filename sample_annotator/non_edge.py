import sqlite3
import pandas as pd
import numpy as np
import re

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

main['env_broad_scale'] = main['env_broad_scale'].str.lower()
main['env_broad_scale'] = main['env_broad_scale'].str.replace('envo:','', regex=True)
merge_table = main.merge(map, left_on = 'env_broad_scale', right_on = 'label', how='left')
merge_table['broad_scale_fixed'] = merge_table['env_broad_scale'].astype(str) + ' ' + '['+merge_table['term_id'].astype(str) +']'

#still need to change the 'nan[nan]' string values to NaN (np.nan) values 
#not sure how
merge_table['broad_scale_fixed'] = merge_table['broad_scale_fixed'].replace('nan[nan]',np.nan)
merge_table['broad_scale_fixed'].value_counts()