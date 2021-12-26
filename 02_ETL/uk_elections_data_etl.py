# -- Importing Libraries -- #

print('\n')
print('Importing libraries to perform ETL...')

import numpy as np
import pandas as pd
import pyfiglet
import warnings
warnings.filterwarnings('ignore')

print('Initiating ETL Process...')
print('\n')

# -- Starting ETL Process --#

etl_title = "UK ELECTIONS-2019 DATA ETL"
ascii_art_title = pyfiglet.figlet_format(etl_title, font='small')
print(ascii_art_title)
print('\n')

# -- Connecting to Dataset -- #

print('Connecting to source dataset')

uk_elections = pd.read_csv("../01_SOURCE/UK_results_by_constituency_2019.csv", index_col=None)

print('\n')

# -- Adding calculated column --#

print('Adding Calculated Column')

mg_conditions = [
    (uk_elections['Majority'] < 1000),
    (uk_elections['Majority'] < 5000),
    (uk_elections['Majority'] < 10000),
    (uk_elections['Majority'] < 20000),
    (uk_elections['Majority'] >= 20000)
]

mg_results = ['Very Low', 'Low', 'Medium', 'High', 'Very High']

uk_elections['Majority Group'] = np.select(mg_conditions, mg_results)

uk_elections['Party Group'] = [
    i if i=='CON' or i=='LAB' or i=='SNP' or i=='LD' or i=='DUP' else 'Others' for i in uk_elections['Party']
]

pg_conditions = [
    (uk_elections['Party Group'] == 'CON'),
    (uk_elections['Party Group'] == 'LAB'),
    (uk_elections['Party Group'] == 'SNP'),
    (uk_elections['Party Group'] == 'LD'),
    (uk_elections['Party Group'] == 'DUP'),
    (uk_elections['Party Group'] == 'Others')
]

pg_results = [1,2,3,4,5,6]

uk_elections['PartyGroupSort'] = np.select(pg_conditions, pg_results)

print(f'Shape of the dataset: {uk_elections.shape}')
print(f'Columns in the dataset: {list(uk_elections.columns)}')
print('\n')

# -- Exporting Data to CSV File --#

print('Exporting the dataframes to CSV files...')

uk_elections.to_csv('../03_DATA/uk_elections_2019_data.csv', encoding='utf-8', index=False)

print('Data exported to CSV...')
print('\n')