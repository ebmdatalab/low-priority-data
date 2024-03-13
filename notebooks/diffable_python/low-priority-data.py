# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## Obtaining OpenPrescribing low-priority measures data

#import required libraries
import pandas as pd
from ebmdatalab import bq
import os
import requests
from io import StringIO
import json
from pandas.io.json import json_normalize
import math

#get list of names of measures from GitHub
res = requests.get('https://api.github.com/repos/ebmdatalab/openprescribing/contents/openprescribing/measures/definitions') #uses GitHub API to get list of all files listed in measure definitions - defaults to main branch
data = res.text #creates text from API result
github_df = pd.read_json(data) #turns JSON from API result into dataframe
lp_names_df = github_df[(github_df['name'].str.startswith('lp') | github_df['name'].str.contains('opioidome')) & 
                        ~github_df['name'].str.startswith('lpz')] #get all measures with either "lp" prefix, or OME marker.  Excludes omnibus, as lpz.
lp_df = pd.DataFrame(lp_names_df['name'].str.split('.').str[0].copy(), columns=['name']) #creates df with measure name
lp_df['name'] =  'practice_data_' + lp_df['name'] # create list of measures as named in BQ

display(lp_df) #show list of names

# +
##create blank dataframe
lp_measures_df=pd.DataFrame()

#create for next loop to go through each table name in the measures names list and get data from BigQuery
for name in lp_df['name']:
    
    sql = """
    SELECT
      '{}' AS table_name, --selects current table name in for next loop
      month, 
      practice_id, 
      pct_id, 
      stp_id, 
      numerator, 
      denominator, 
      calc_value, 
      percentile
    FROM
      `ebmdatalab.measures.{}` AS a
    """
    
    sql = sql.format(name, name) #using python string to add table_name to SQL in two places
    #concatenate each table name into single file during for next loop
    lp_measures_df = pd.concat([lp_measures_df, bq.cached_read(sql, os.path.join("..","data","{}_df.csv").format(name), use_cache=True)])

# As Github has 100mb file size limit, split zip file into chunks
num_chunks = 2 #2 should be enough for this
chunk_size = math.ceil(len(lp_measures_df) / num_chunks) # Calculate the size of each chunk
chunks = [lp_measures_df[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)] # Split the DataFrame into chunks
# Export each chunk to a separate CSV file
for i, chunk in enumerate(chunks):
    chunk.to_csv(
    os.path.join("..", "data",f'lp_measures_df_{i}.zip'),
    compression={'method': 'zip', 'archive_name': 'lp_measures_df.csv'})
# -


