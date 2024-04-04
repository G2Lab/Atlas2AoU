# get UKBB prevalence estimates from postgres DB (for prevalence comparison with AoU)
import pandas as pd
from sqlalchemy import create_engine
import argparse
import configparser
import sys
import os

### READ INPUTS
parser = argparse.ArgumentParser(description="Process some arguments.")
parser.add_argument('-o', required=True,type=str, help='Output path')
parser.add_argument('-t', required=True,type=str, help='Path with tested ohdsi phenos')
parser.add_argument('-csv_path', required=True,type=str, help='Path to csv with desired OHDSI phenos')
parser.add_argument('-c', required=True, type=str, help='Config file for database')
args = parser.parse_args()
### READ INPUTS

sys.path.append(f'{os.path.dirname(args.o)}/py_files')
from utilities import  get_co_prev_df

# Read the configuration from the .ini file (config.ini)
config = configparser.ConfigParser()
config.read(args.c)  # Provide the path to your config.ini file
# Database configuration
username = config.get('postgres', 'username')
password = config.get('postgres', 'password')
host = config.get('postgres', 'host')
database = config.get('postgres', 'database')
# Create the database engine using the configuration
engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')

# Read in cohortIds
df = pd.read_csv(args.csv_path,index_col=0)
tested_files = [int(f.replace('.py','')) for f in os.listdir(args.t) if os.path.isfile(os.path.join(args.t, f))]
df = df[df.cohortId.isin(tested_files)].copy()
assert df.shape[0] == 425

# Get # of people total in UKBB
sql = 'SELECT COUNT(DISTINCT person_id) FROM PERSON;'
query_df = pd.read_sql(sql, con=engine)
N_ukbb = query_df['count'][0]
df['N_ukbb'] = N_ukbb

# Get # of people for each cohort
print('getting number of ppl')
def get_person_and_count(x,engine):
    print(x,flush=True)
    sql = f'SELECT DISTINCT subject_id FROM COHORT WHERE cohort_definition_id = {x};'
    query_df = pd.read_sql(sql, con=engine)
    Count_ukbb = len(query_df.subject_id.unique())
    return Count_ukbb,set(query_df.subject_id.unique())
df[['Count_ukbb','People_ukbb']] = df.cohortId.apply(lambda x: pd.Series(get_person_and_count(x,engine)))

cohorts = df.set_index('cohortId')['People_ukbb'].to_dict()
# remove people_ukbb
df.drop('People_ukbb',inplace=True,axis=1)
# mask for PII
df['Count_ukbb'] = df['Count_ukbb'].apply(lambda x: 'Diagnoses <= 20' if x<=20 else x)

# Prevalence
df['Prev_ukbb'] = df.apply(lambda x: x['Count_ukbb']/x['N_ukbb'] if x['Count_ukbb']!= 'Diagnoses <= 20' else 'Diagnoses <= 20',axis=1)


# Export to csv
df.to_csv(f'{args.o}/UKBBPrev.csv')

coprev = get_co_prev_df(cohorts,N_ukbb,df,'ukbb')
coprev.to_csv(f'{args.o}/UKBBCoPrev.csv')

print('done!')