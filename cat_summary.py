# This is the Python script to produce summary statistics for categorical variables.

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import csv
import nltk


# change this string variable to whatever column you want to look at
column_ = ""

# Data Location after to_csv
DATA_DIR = '/content/harmonized_wide_sel_envs.csv'

def to_csv():
    """Grabs database and converts into usable python dataframe."""
    db = sqlite3.connect('biosample_basex_data_good_subset.db')
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

data = pd.read_csv(DATA_DIR)

###### PREMADE VISUALIZATIONS ######
def rel_to_oxygen_visualizations(data):
  repair_rel_to_oxygen(data)
  chart = sns.countplot(x = 'rel_to_oxygen',
                      data = data,
                      order = data['rel_to_oxygen'].value_counts().index)

  chart.set_xticklabels(chart.get_xticklabels(), rotation=90)
  plt.show()

  chart = sns.countplot(x = 'rel_to_oxygen_rep',
                      data = data,
                      order = data['rel_to_oxygen_rep'].value_counts().index)
  for p in chart.patches:
    chart.annotate('{:.1f}'.format(p.get_height()), (p.get_x()+0.25, p.get_height()+0.01))

  chart.set_xticklabels(chart.get_xticklabels(), rotation=90)
  plt.show()

def repair_rel_to_oxygen(data):
  ''' taken from other group's code '''
  data['rel_to_oxygen_rep'] = data['rel_to_oxygen'].astype(str).astype('string')
  data['rel_to_oxygen_status'] = np.NaN
  arr = data['rel_to_oxygen_rep'].values
  ps = nltk.PorterStemmer()
  data['rel_to_oxygen_rep'] = [ps.stem(word) for word in arr]
  comparison = data['rel_to_oxygen_rep'].copy()

  data['rel_to_oxygen_rep'] = data['rel_to_oxygen_rep'].replace(to_replace = ["obligate anaerob", "facultative anaerob", "anaerob", "facultative anaerob"], value="anaerobe")
  data['rel_to_oxygen_rep'] = data['rel_to_oxygen_rep'].replace(to_replace = ["aerob"], value="aerobe")
  #replacing NaN values with "repair" for the words just replaced
  data['rel_to_oxygen_status'] = data['rel_to_oxygen_status'].where(comparison == data['rel_to_oxygen_rep'], 'Repaired')

  bool_list = data['rel_to_oxygen_rep'].str.contains('mg\/l$', regex = True)
  data['rel_to_oxygen_rep'] = data['rel_to_oxygen_rep'].where(bool_list == False, np.NaN)
  data['rel_to_oxygen_rep'] = data['rel_to_oxygen_rep'].where(data['rel_to_oxygen_rep'] != 'none', np.NaN)

  data['rel_to_oxygen_rep'] = data['rel_to_oxygen_rep'].replace(to_replace = ['oblig', 'oxic', 'hypox', 'aerobic-anaerob', 'oxic/anoxic boundari', 'microaerophil', 'normal oxic seawat', 'facult'], value=np.NaN)
  filter1 = data['rel_to_oxygen_status']== 'Repaired'
  data['rel_to_oxygen_status'] = data['rel_to_oxygen_status'].where(filter1, "Couldn't Repair")
###### END OF PREMADE VISUALIZATIONS ######

# produce various summary statistics given data and column of interest
def summary_statistics(df, column):
    unique_vals = df[column].unique()
    counts_df = df[column].value_counts()
    means = df.groupby(column).agg(np.nanmean)
    print("Unique values of", column, ": \n", unique_vals, "\n")
    print("Counts of unique values of", column, ": \n", counts_df)
    print("Means of numerical variables after grouped by", column, ": \n", means)


summary_statistics(data, column_)


# create visualizations
def visualizations(df, cat_column, num_column = None):
    fig1 = sns.catplot(x=cat_column, kind="count", data=df)
    fig2 = None
    fig3 = None
    plt.xticks(rotation=90)
    if num_column:
        fig2 = sns.catplot(x=cat_column, y=num_column, data=df)
        fig3 = sns.catplot(x=cat_column, y=num_column, kind="box", data=df)
    return fig1, fig2, fig3


# input numerical column name as parameter in following line if applicable
visualizations(data, column_)