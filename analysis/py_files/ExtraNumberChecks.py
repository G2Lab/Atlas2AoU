# double checking numbers cited in manuscript

import pandas as pd
from sqlalchemy import create_engine
import configparser
import argparse

parser = argparse.ArgumentParser(description='Process hyperparameters.')
parser.add_argument('-f', required=True,type=str, help='Path to aou-atlas-phenotyping folder')
parser.add_argument('-c', required=True,type=str, help='Path to config file')
parser.add_argument('-ukbb_data_dir', required=True,type=str, help='Path to ukbb folder')
args = parser.parse_args()


# Read the configuration from the .ini file (config.ini)
config = configparser.ConfigParser()
config.read(args.c)
# Database configuration
username = config.get('postgres', 'username')
password = config.get('postgres', 'password')
host = config.get('postgres', 'host')
database = config.get('postgres', 'database')
# Create the database engine using the configuration
engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')


## One of 30 broader disease categories available for selection
liu_condition_cat = pd.read_csv(f'{args.f}/analysis/data/liu_condition_cat.csv')
assert len(liu_condition_cat['Condition category'].unique()) == 30

## N = 502,365 for UKBB
sql = "SELECT COUNT(DISTINCT person_id) FROM PERSON;"
N_ukbb = pd.read_sql(sql, con=engine)
assert N_ukbb['count'].values[0] == 502365

## GWAS Params used
# check params in qc file
with open(f'{args.ukbb_data_dir}/SNPS/FILE_QC_direct.log','r') as f:
    log_file = f.read()
assert '--geno 0.05' in log_file
assert '--maf 0.05' in log_file
assert '--hwe 0.000001' in log_file
assert '--indep-pairwise 50 5 0.5' in log_file
assert '--mind 0.05' in log_file

## After QC, 251k variants remained
with open(f'{args.ukbb_data_dir}/PHENO_28/RESULTS_FILE.log','r') as f:
    log_file = f.read()
assert '250850 variants loaded from' in log_file
assert f'{args.ukbb_data_dir}/SNPS/FILE_QC_direct.bim' in log_file

## Regions of highest diabetes prev. in UK: Newport, Blaenau Gwent, Caerphilly
c288 = pd.read_csv(f'{args.f}/analysis/output/c288_regions_w_highest_prev_ukbb.csv')
assert list(c288.sort_values('prev',ascending=False).head(3).CTYUA23NM.unique()) == ['Newport', 'Blaenau Gwent', 'Caerphilly']

## Regions of highest COPD prev. in UK: West Dunbartonshire, Glasgow City, and West Lothian
c28 = pd.read_csv(f'{args.f}/analysis/output/c28_regions_w_highest_prev_ukbb.csv')
assert list(c28.sort_values('prev',ascending=False).head(3).CTYUA23NM.unique()) == ['West Dunbartonshire', 'Glasgow City', 'West Lothian']

## Regions of highest Acute MI prev. in UK: East Lothian, Midlothian, and Knowsley
c71 = pd.read_csv(f'{args.f}/analysis/output/c71_regions_w_highest_prev_ukbb.csv')
assert list(c71.sort_values('prev',ascending=False).head(3).CTYUA23NM.unique()) == ['East Lothian', 'Midlothian', 'Knowsley']

# Check no less than 21 pushed to Git
for cohort in [28,288,71]:
    rgns_w_high_prev = pd.read_csv(f'{args.f}/analysis/output/c{cohort}_regions_w_highest_prev_ukbb.csv')
    assert rgns_w_high_prev[rgns_w_high_prev['count']<21].shape[0] == 0
    control_count = rgns_w_high_prev['total_count'] - rgns_w_high_prev['count']
    assert any(control_count<21) is False