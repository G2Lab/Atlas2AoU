
# get estimates of prevalence by age bin

# get UKBB prevalence estimates from postgres DB (for prevalence comparison with AoU)
import pandas as pd
from sqlalchemy import create_engine, text
import argparse
import configparser
import sys
import os
import numpy as np

### READ INPUTS
parser = argparse.ArgumentParser(description="Process some arguments.")
parser.add_argument('-o', required=True,type=str, help='Output path')
parser.add_argument('-t', required=True,type=str, help='Path with tested ohdsi phenos')
parser.add_argument('-csv_path', required=True,type=str, help='Path to csv with desired OHDSI phenos')
parser.add_argument('-c', required=True, type=str, help='Config file for database')
parser.add_argument('--ukbb_environ', type=str, help='dir with ukbb environmental data',default='')
args = parser.parse_args()
### READ INPUTS

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
with engine.connect() as conn:
    query_df = pd.read_sql(text(sql), conn)
N_ukbb = query_df['count'][0]
df['N_ukbb'] = N_ukbb

# get age bin
# recode age
def recode_age(x):
    if x['21022']>=35 and x['21022']<50:
        return '35-49'
    elif x['21022'] >=50 and x['21022'] < 65:
        return '50-64'
    elif x['21022'] >=65:
        return '65+'
    else:
        print(x['21022'])
        assert np.isnan(x['21022']) == True
        return np.nan
raw_sdoh = pd.read_csv(f'{args.ukbb_environ}/ukb676180.csv', usecols=['eid','21022-0.0']) 
raw_sdoh.rename(columns={'21022-0.0':'21022','eid':'subject_id'},inplace=True)
raw_sdoh['21022'] = raw_sdoh.apply(lambda x: recode_age(x),axis=1) 
age_counts = pd.DataFrame(raw_sdoh['21022'].value_counts())
df['N_ukbb_35_49'] = age_counts.loc['35-49']['21022']
df['N_ukbb_50_64'] = age_counts.loc['50-64']['21022']
df['N_ukbb_65_plus'] = age_counts.loc['65+']['21022']

# Get # of people for each cohort
print('getting number of ppl')
def get_person_and_count(x,raw_sdoh,engine):
    print(x,flush=True)
    sql = f'SELECT DISTINCT subject_id FROM COHORT WHERE cohort_definition_id = {x};'
    with engine.connect() as conn:
        query_df = pd.read_sql(text(sql), conn)
    Count_ukbb_35_49 = len(query_df.merge(raw_sdoh[raw_sdoh['21022']=='35-49'],on='subject_id').subject_id.unique())
    Count_ukbb_50_64 = len(query_df.merge(raw_sdoh[raw_sdoh['21022']=='50-64'],on='subject_id').subject_id.unique())
    Count_ukbb_65_plus = len(query_df.merge(raw_sdoh[raw_sdoh['21022']=='65+'],on='subject_id').subject_id.unique())
    return Count_ukbb_35_49,Count_ukbb_50_64,Count_ukbb_65_plus
df[['Count_ukbb_35_49','Count_ukbb_50_64','Count_ukbb_65_plus']] = df.cohortId.apply(lambda x: pd.Series(get_person_and_count(x,raw_sdoh,engine)))

# mask for PII
for count_col in ['Count_ukbb_35_49', 'Count_ukbb_50_64', 'Count_ukbb_65_plus']:
    df[count_col] = df[count_col].apply(lambda x: 'Diagnoses <= 20' if x<=20 else x)
    total_count_col = count_col.replace('Count','N')
    df[count_col.replace('Count_','Prev_')] = df.apply(lambda x: x[count_col]/x[total_count_col] if x[count_col]!= 'Diagnoses <= 20' else 'Diagnoses <= 20',axis=1)

# Export to csv
df.to_csv(f'{args.o}/UKBBPrev_by_age.csv')


print('done!')