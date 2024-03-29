#! /gpfs/commons/home/anewbury/miniconda/envs/jupyter/bin/python3
#SBATCH --job-name=sdoh
#SBATCH --partition=pe2
#SBATCH --mail-type=ALL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --time=80:00:00
#SBATCH --output=/gpfs/commons/groups/gursoy_lab/anewbury/sdoh_output.txt
#SBATCH --error=/gpfs/commons/groups/gursoy_lab/anewbury/sdoh_errors.txt



import pandas as pd
import numpy as np
import configparser
import argparse
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Process parameters.')
parser.add_argument('--f', type=str, help='aou-atlas-phenotyping dir',default='')
parser.add_argument('--ukbb_environ', type=str, help='dir with ukbb environmental data',default='')
parser.add_argument('--analysis_output_dir', type=str, help='analysis output directory (in aou-atlas-phenotyping folder)',default='')
parser.add_argument('--c', type=str, help='path to config file for postgres db',default='')
args = parser.parse_args()

# cohort ids of interest
cohortids = [288,28,71]

config = configparser.ConfigParser()
config.read(args.c)  # Provide the path to your config.ini file
# Database configuration
username = config.get('postgres', 'username')
password = config.get('postgres', 'password')
host = config.get('postgres', 'host')
database = config.get('postgres', 'database')
# Create the database engine using the configuration
engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')

# read in mapping main
file_path = f'{args.f}/analysis/data/MappingMain-FineGrained.xlsx'
xls = pd.ExcelFile(file_path, engine='openpyxl')
sheets_dict = pd.read_excel(xls, sheet_name=None)
mapping_main = sheets_dict['Sheet 1']
mapping_main = mapping_main[(mapping_main['used for']=='sdoh')|(mapping_main['variable name']=='age')].copy()
mapping_main['UKBB Field ID'] = pd.to_numeric(mapping_main['UKBB Field ID'])


raw_sdoh_columns = pd.read_csv(f'{args.ukbb_environ}/ukb676180.csv', nrows=0)  
raw_sdoh_columns2 = pd.read_csv(f'{args.ukbb_environ}/ukb677887.csv', nrows=0)  

# ukbb field and the data coding file name
field_and_coding_dict = {6138:100305,6142:100295,680:100287,738:100294,1558:100402,2644:100349,29174:1930,29173:1930,29172:1930,20161:None,1249:100348,21022:None}


# get first instance - use_cols else takes long to load
filtered_columns = [col for col in raw_sdoh_columns.columns if any(col.startswith(f'{field}-0') for field in mapping_main['UKBB Field ID'].unique().tolist()+[1249])] + ['eid']
raw_sdoh = pd.read_csv(f'{args.ukbb_environ}/ukb676180.csv', usecols=filtered_columns)  
# two sdoh csvs, from different UKBB baskets - merge
filtered_columns = [col for col in raw_sdoh_columns2.columns if any(col.startswith(f'{field}-0') for field in mapping_main['UKBB Field ID'].unique().tolist()+[1249])] + ['eid']
raw_sdoh2 = pd.read_csv(f'{args.ukbb_environ}/ukb677887.csv', usecols=filtered_columns)

raw_sdoh = raw_sdoh.merge(raw_sdoh2,how='outer',on='eid')
# make sure that raw_sdoh has all 502365 person ids, since all will have dob, age, sex per omop person
sql = f'SELECT DISTINCT person_id AS eid FROM PERSON;'
query_df = pd.read_sql(sql, con=engine)
raw_sdoh = pd.concat([raw_sdoh, query_df], ignore_index=True).drop_duplicates(subset=['eid'], keep='first')
# remove 4 individuals not in OMOP CDM
raw_sdoh = raw_sdoh[raw_sdoh['eid'].isin(query_df.eid.unique())]

assert raw_sdoh.shape[0] == 502365

# change -7 and -3 and -1 to na values (None of the above, prefer not to answer, do not know)
raw_sdoh.replace({-7: np.nan, -3: np.nan, -1:np.nan}, inplace=True)

# for 6138 - take highest education level (starts at 1) into 6138-0.0 and drop the other 6138 columns
raw_sdoh['6138-0.0'] = raw_sdoh[[col for col in raw_sdoh.columns if col.startswith(f'6138-0')]].min(axis=1)
raw_sdoh.drop([col for col in raw_sdoh.columns if col.startswith(f'6138-0') and col!='6138-0.0'],axis=1,inplace=True)
# for 6142, combine them into a list in 6142-0.0 and drop the other 6142 columns
raw_sdoh['6142-0.0'] = raw_sdoh[[col for col in raw_sdoh.columns if col.startswith(f'6142-0')]].apply(lambda row: [i for i in row.tolist() if not np.isnan(i)],axis=1)
raw_sdoh.drop([col for col in raw_sdoh.columns if col.startswith(f'6142-0') and col!='6142-0.0'],axis=1,inplace=True)

# now all columns should just be -0.0 (can drop this)
assert all([col.endswith('-0.0') or col=='eid' for col in raw_sdoh.columns])
raw_sdoh.columns = raw_sdoh.columns.str.replace(r'-0.0', '')


# add cohorts as columns to sdoh data (e.g. cohort_x)
for cohortid in cohortids:
    print(f'running for cohort id:{cohortid}')
    # get list of person_ids in each cohort
    sql = f'SELECT DISTINCT subject_id FROM COHORT where cohort_definition_id = {cohortid};'
    query_df = pd.read_sql(sql, con=engine)
    print('done querying')
    raw_sdoh[f'cohort_{cohortid}'] = raw_sdoh.eid.isin(query_df['subject_id'].unique())

#  grab biological sex, YOB and race from OMOP Person table --> map to their concept_name 
sql = '''SELECT
    p.person_id AS eid,
    gender_concept.concept_name AS gender,
    race_concept.concept_name AS race
FROM
    PERSON p
LEFT JOIN CONCEPT gender_concept ON p.gender_concept_id = gender_concept.concept_id
LEFT JOIN CONCEPT race_concept ON p.race_concept_id = race_concept.concept_id;'''
query_df = pd.read_sql(sql, con=engine)
raw_sdoh = pd.merge(raw_sdoh,query_df,on='eid',how='inner')
assert raw_sdoh.shape[0] == 502365

def recode_list(value_list, mapping):
    return [mapping[item] for item in value_list]

# for those fields that we have currently, recode those with data coding
for field in field_and_coding_dict.keys():
    if field_and_coding_dict[field] is not None and str(field) in raw_sdoh.columns:
        data_coding = pd.read_csv(f'{args.f}/analysis/data/data_coding/coding{field_and_coding_dict[field]}.tsv',sep='\t')
        data_coding_dict = data_coding.set_index('coding')['meaning'].to_dict()
        if field!=6142: # if not in coding dict, fill in with original value
            raw_sdoh[str(field)] = raw_sdoh[str(field)].map(data_coding_dict).fillna(raw_sdoh[str(field)])
            raw_sdoh[str(field)] = raw_sdoh[str(field)].infer_objects()
        # multi-select options
        else:
            raw_sdoh[str(field)] = raw_sdoh[str(field)].apply(lambda x: recode_list(x, data_coding_dict))


# for home ownership, only keep categories 'own' or 'rent' - send 'Live in accommodation rent free' to NaN 680
raw_sdoh['680'] = raw_sdoh['680'].replace('Live in accommodation rent free', np.nan)

# for employment, remove 'Doing unpaid or voluntary work' - no mapping
raw_sdoh['6142'] = raw_sdoh['6142'].apply(lambda x: [i for i in x if i != 'Doing unpaid or voluntary work'])

# remove no matching concept from race categories
raw_sdoh['race'] = raw_sdoh['race'].replace('No matching concept', np.nan)
# code race into coarser grained concepts
raw_sdoh.replace({'race': {'Asian Indian': 'Asian', 'Pakistani': 'Asian','Asian':'Asian','Chinese':'Asian','Bangladeshi':'Asian','African':'Black'}},inplace=True)

# smoking_hundred_cig: UKBB: count those who say “Smoked on most or all days” to first question as “Yes” in the 2nd and those who say “I have never smoked” to first question as “No” in the 2nd 
# from 1249
def recode_smoking(x):
    if x['1249'] == 'Smoked on most or all days':
        return 'Yes'
    elif x['1249'] == 'I have never smoked':
        return 'No'
    else:
        return x['2644']
raw_sdoh['2644'] = raw_sdoh.apply(lambda x: recode_smoking(x),axis=1)
raw_sdoh.drop('1249',axis=1,inplace=True)

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
raw_sdoh['21022'] = raw_sdoh.apply(lambda x: recode_age(x),axis=1)

# rename columns
for field in field_and_coding_dict.keys():
    if str(field) in raw_sdoh.columns:
        # rename columns per mapping main
        raw_sdoh.rename(columns={str(field): mapping_main[mapping_main['UKBB Field ID']==field]['variable name'].values[0]}, inplace=True)

# need to remap categories per fine-grained mapping with AoU data 
for field,v in sheets_dict.items():
    if field not in ['Sheet 1']:
        # explode ukbb answers (sep. by ;)
        df = v.copy()
        df['ukbb_answer'] = df['ukbb_answer'].str.split('; ')
        # create dict with ukbb_answer: category_name
        df = df.explode('ukbb_answer')
        category_remapping = df.set_index('ukbb_answer')['category_name'].to_dict()
        # remap to category_name
        if field!='employment':
            raw_sdoh[str(field)] = raw_sdoh[str(field)].map(category_remapping).fillna(raw_sdoh[str(field)])
            raw_sdoh[str(field)] = raw_sdoh[str(field)].infer_objects()
        else:
            raw_sdoh[str(field)] = raw_sdoh[str(field)].apply(lambda x: recode_list(x, category_remapping))

# get summary statistics 
results = []
for cohortid in cohortids:
    for column in raw_sdoh.columns:
        if column.startswith('cohort') or column=='eid':  
            continue
        # Check if column is numerical
        if np.issubdtype(raw_sdoh[column].dtype, np.number):
            mean = raw_sdoh[raw_sdoh[f'cohort_{cohortid}'] == True][column].mean()
            sd = raw_sdoh[raw_sdoh[f'cohort_{cohortid}'] == True][column].std()
            result_dict = {'Mean':mean,'SD':sd}
        else:  # Column is categorical
            # check if any categories have less than 20 people - rename them to na and don't use to calc prevalence
            if column == 'employment': # explode such that there can be multiple values per person
                # calculate count 
                count = raw_sdoh[raw_sdoh[f'cohort_{cohortid}'] == True].explode('employment').groupby([column]).size().reset_index()
                # for these three cohorts employment count is over 20 in all categories so we just make assert check
                assert count[count[0]<=20].shape[0]==0
                # proportion over N= # answers
                prop = raw_sdoh[raw_sdoh[f'cohort_{cohortid}'] == True].explode('employment').groupby([column]).size().transform({'prop':lambda x: x / x.sum()}).unstack(level=0).reset_index()
            else:
                count = raw_sdoh[raw_sdoh[f'cohort_{cohortid}'] == True].groupby([column]).size().reset_index()
                # fix if cell counts less than 20
                if count[count[0]<=20].shape[0]>0:
                    count = count[count[0] > 20].copy()
                assert count[count[0]<=20].shape[0]==0
                # only report a category with enough of a cell count
                accepted_values = count[column].values.tolist()
                # proportion over N=# ppl answering
                prop = raw_sdoh[(raw_sdoh[f'cohort_{cohortid}'] == True)&(raw_sdoh[column].isin(accepted_values))].groupby([column]).size().transform({'prop':lambda x: x / x.sum()}).unstack(level=0).reset_index()
            # make sure prop adds to 1
            assert abs(prop[0].sum() - 1) < 1e-10
            result_dict = (dict(zip(prop[column], prop[0])))
        results.append([cohortid,column,result_dict])

sdoh = pd.DataFrame(results,columns=['cohortId','column','data'])
sdoh = sdoh.pivot(index='cohortId', columns='column', values='data')
sdoh.to_csv(f'{args.analysis_output_dir}/UKBBSDOH.csv')