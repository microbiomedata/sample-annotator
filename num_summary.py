# This is the Python script to produce summary statistics for numerical variables.
# Additionally this script cleans a numerical column
#######################################
## Authors: Aaron Lee, Jihee Choo    ##
#######################################

''' 
Instructions for running the script:
1. Set 2 variables: the directory of the location with the csv data: "DATA_DIR"
(may need to run to_csv() with the biosample_basex_data_good_subset.db in
the current directory) and "column_" the column of the csv file that you'd like
to fix.
2. Optionally, if the dataframe is already loaded can just do 
"from num_summary import *" 
3. the script automatically runs clean_fast on the specified column, and
produces visualizations, and the function clean_fast can be used outside
of the script (see step 2)
4. Optionally, run air_temp_visualizations() or depth_visualizations() for 
manually made visualizations for the respective columns.
'''

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from quantulum3 import parser
import nltk
import string

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

def clean_fast(df, col, force_numeric=False, verbose=False):
  '''takes in a dataframe and a column and returns a new dataframe with
  that column cleaned -> 'cleaned_' + col and the units for that value.
  if force_numeric, will make sure there are no strings by making any value
  that couldn't be cleaned NaN. 
  @params: 
  df pandas dataframe
  col column to clean
  force_numeric turns any column that couldn't be converted to a numerical value
  to NaN, otherwise just returns the uncleaned string
  verbose print
  '''
  df = df.copy()
  series = df[col]
  cleaned = np.array([])
  type_cleaned = np.array([])

  def get_value(item):
    try:
      quants = parser.parse(item)
      if len(quants) == 0:
        # no value
        if force_numeric:
          return np.NaN
        return item
      if len(quants) > 1 and verbose:
        print('multiple unit types detected will be using first unit type detected', item)
      return quants[0].value
    except:
      try:
        # will try to remove any spacing and pass through quantulum
        nospaces = item.replace(' ', "")
        quants = parser.parse(nospaces)
        return quants[0].value
      except:
        if item is np.NaN:
          return item
        if verbose: print('error with quantulum parsing will not clean', item)
        if force_numeric:
          return np.NaN
        return item
    
    return np.NaN
  def get_unit(item):
    try:
      quants = parser.parse(item)
      if len(quants) == 0:
        return 'dimensionless'
    except:
      return 'dimensionless'
    return quants[0].unit.name # use whatever was detected first if multiple types
      
  df['cleaned_' + col] = df[col].apply(get_value)
  df['cleaned_' + col + '_type'] = df[col].apply(get_unit)
  return df

def depth_visualizations(data):
  depth = clean_fast(data, 'depth', force_numeric=True)
  # removing extreme values
  depth = depth[depth['cleaned_depth']<20000]
  plt.boxplot(x=depth["cleaned_depth"].dropna())
  plt.show()
  depth_cleaned_low = depth[(depth['depth'] < 1000) & (depth['depth'] > -1)]
  depth_cleaned_high = depth[depth['depth'] >= 1000]
  # does appear to be noticeable difference
  plt.boxplot(x=depth_cleaned_low["depth"])
  plt.show()
  plt.boxplot(x=depth_cleaned_high["depth"])
  plt.show()

  # all of the larger depths come from aerobe
  import seaborn as sns
  sns.catplot(x="rel_to_oxygen_rep", y="depth", data=depth_cleaned_high)
  
  plt.show()



def air_temp_visualizations(data):
  air_temp = clean_fast(data, 'air_temp')
  plt.hist(air_temp['cleaned_air_temp'])
  plt.show()
  plt.boxplot(air_temp['cleaned_air_temp'].dropna())
  plt.show()



data = clean_fast(data, column_, force_numeric=True)
# produce summary statistics given data and column of interest
data[column_].describe()


# create basic visualizations, will likely need to do more data cleaning
# to obtain meaningful visualizations
plt.hist(data[column_])
plt.show()

plt.boxplot(data[column_].dropna())
plt.show()

plt.violinplot(data[column_].dropna(), showmeans=True)
