import sqlalchemy
import matplotlib.pyplot as plt
from pathlib import Path
import pygsheets
import re
import nltk
import string
import measurement
from measurement.utils import guess
from measurement.measures import Distance
import nltk
nltk.download('punkt')
from nltk.tokenize import word_tokenize


plt.style.use('fivethirtyeight')
sns.set()
sns.set_context("talk")

pd.set_option('display.max_rows', 20)

# Setup - Load the SQL extension and connect to the Mini IMDB dataset we've prepared
db_path = Path('data/biosample_basex_data_good_subset.db')

engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
connection = engine.connect()
inspector = sqlalchemy.inspect(engine)

query_name = """
SELECT *
FROM harmonized_wide_sel_envs
"""
harmonized_wide_sel_envs_df = pd.read_sql(query_name, engine)

#Creating a generalized df that pull from google sheet. For other columns, just need to call from generalized_df
gc = pygsheets.authorize(service_file='biosamples-annotation-dg-32fd479a2039.json')
sh = gc.open('biosamples-annotation-dg sample sheet')
wks = sh[0]
generalized_df = wks.get_as_df()
rto_reference = generalized_df[generalized_df["column_name"] == 'rel_to_oxygen']
rto_reference

#Rel_to_oxygen column
#Creating a new column 'rel_to_oxygen_rep' which is rel_to_oxygen but just cleaned with the values we want

harmonized_wide_sel_envs_df['rel_to_oxygen_rep'] = harmonized_wide_sel_envs_df['rel_to_oxygen'].astype(str).astype('string')
harmonized_wide_sel_envs_df['rel_to_oxygen_rep'].value_counts()

#Looping through all possible values in raw value from the google Spreadsheet and replacing all of them with the mapped values
#that we mapped already on the spreadsheet. This should be able to be generalized for all enumerable columns, and one just needs
#to input the specific column.

for i in rto_reference['raw_value'].astype('string'):
    mapv = rto_reference[rto_reference['raw_value'] == i]['replacement_value'].astype('string')
    harmonized_wide_sel_envs_df = harmonized_wide_sel_envs_df.replace({'rel_to_oxygen_rep': {i: mapv.values[0]}})
harmonized_wide_sel_envs_df['rel_to_oxygen_rep'].value_counts()

def replace_with_mapped(df_name, spreadsheet, c_name):
    for i in spreadsheet['raw_value'].astype('string'):
        mapv = spreadsheet[spreadsheet['raw_value'] == i]['replacement_value'].astype('string')
        df_name = df_name.replace({c_name: {i: mapv.values[0]}})
        return df_name[c_name].value_counts()

replace_with_mapped(harmonized_wide_sel_envs_df, rto_reference, 'rel_to_oxygen_rep')

#Air_temp column

#We've assumed that all of the columns are in degrees Celsius, which is why we removed the degrees from those that did have them
#and add them back in so all of the values are in degrees Celsius

harmonized_wide_sel_envs_df['air_temp_rep'] = harmonized_wide_sel_envs_df['air_temp'].str.replace(" degree Celsius", "")
harmonized_wide_sel_envs_df['air_temp_rep'] = harmonized_wide_sel_envs_df['air_temp'] + " degree Celsius"
harmonized_wide_sel_envs_df['air_temp_rep_status'] = harmonized_wide_sel_envs_df['air_temp']

print("rel_to_oxygen_rep Value Counts: ")
display(harmonized_wide_sel_envs_df['rel_to_oxygen_rep'].value_counts())
print("air_temp Value Counts: ")
display(harmonized_wide_sel_envs_df['air_temp'].value_counts())
print("air_temp Unique Values: ")
display(harmonized_wide_sel_envs_df['air_temp'].unique())

#Depth Column: Depth value syntax should be {float} {unit}. preferred unit is meter

#regex:=
mmRegex = r"mm"
cmRegex = r"(cm)|(centimeter)"
feetRegex = r"feet|ft"
missingRegex = r".*([Mm]issing).*|.*([Nn]one).*|.*(not).*|.*(N/*A).*"
surfaceRegex = r".*[Ss]urface.*"
dateRegex = r"[0-9][-][A-Za-z]{2,}"
mixedRegex = r".*mixed.*"
topsoilRegex = r".*[Tt]op( )*soil.*"


#used to fix surface
harmonized_wide_sel_envs_df["depth"] = harmonized_wide_sel_envs_df["depth"].str.replace(surfaceRegex, "0 m", regex=True)

harmonized_wide_sel_envs_df[harmonized_wide_sel_envs_df["depth"].str.contains(surfaceRegex) == True]['depth']

#fix topsoil
harmonized_wide_sel_envs_df[harmonized_wide_sel_envs_df["depth"].str.contains(topsoilRegex) == True]['depth']

def is_valid(arr): #determines validity of current stuff
    indexes = []
    for x in range(len(arr)):
        try:
            y = float(arr[x])
            arr.iloc[x] = y
        except:
            if arr[x] == None:
                arr.iloc[x] = None
            else:
                indexes.append(x)
    return indexes

differences = is_valid(harmonized_wide_sel_envs_df['depth']) #all none is none, nums is float, and else is examine
harmonized_wide_sel_envs_df['depth'][29:34] #test

def split(series, anomalies): #input series and indices that require attention
    new_s = series
    for x in anomalies:
        new_s.iloc[x] = word_tokenize(new_s.iloc[x])
    return new_s

def measured(split_series, anomalies): #turn isol format num, letter or measurement into measurement class
    measured_indices = set(anomalies)
    new_s = split_series
    for x in anomalies:
        if len(new_s.iloc[x]) != 2:
            measured_indices = measured_indices - {x}
        else:#it IS 2
            try:
                new_s.iloc[x] = guess(float(split_series.iloc[x][0]), split_series.iloc[x][1])
            except:
                measured_indices = measured_indices - {x}
                pass
    return new_s, measured_indices


split_series_final = split(harmonized_wide_sel_envs_df['depth'], differences)

split_series_final[39]
isol_measured, isol_measured_indices = measured(split_series_final, differences)
isol_measured[isol_measured_indices]
type(isol_measured.iloc[8133])

for x in isol_measured_indices:
    temp = isol_measured.iloc[x]
    if type(temp) == measurement.measures.distance.Distance:
        isol_measured.iloc[x] = float(isol_measured.iloc[x].m)

sol_measured.iloc[x].m)
isol_measured[isol_measured_indices]
differences_1 = set(differences) - set(isol_measured_indices)
isol_measured[differences_1]
harmonized_wide_sel_envs_df['soil_type'].value_counts()
