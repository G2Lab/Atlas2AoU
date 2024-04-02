
#  run GWAS using PLINK for UKBB

import pandas as pd
import os
from sqlalchemy import create_engine
import argparse
import configparser
import subprocess
from plotnine import *
import numpy as np


parser = argparse.ArgumentParser(description='Process parameters.')
parser.add_argument('--ukbb_data_dir', type=str, help='ukbb data directory',default='')
parser.add_argument('--cohortId', type=str, help='id of ohdsi cohort',default='')
parser.add_argument('--analysis_output_dir', type=str, help='analysis output directory (in aou-atlas-phenotyping folder)',default='')
parser.add_argument('--annot_data_dir', type=str, help='data directory where gtf annot file gencode.v44lift37.annotation.gtf is',default='')
parser.add_argument('--c', type=str, help='path to config file for postgres db',default='')
args = parser.parse_args()

import sys
sys.path.append(f'{os.path.dirname(args.analysis_output_dir)}/py_files')
from utilities import  make_manhattan_plot, get_gtf, get_gene_annot

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


# merge files and run QC
if not os.path.exists(f'{args.ukbb_data_dir}/Atlas2AoU/SNPS/FILE_QC_direct.bed'):
    print("running qc")
    result = subprocess.run(f'module load plink/1.90b6.24 && plink --merge-list {args.ukbb_data_dir}/Atlas2AoU/SNPS/merge.txt --out {args.ukbb_data_dir}/Atlas2AoU/SNPS/merged_direct', shell=True, capture_output=True, text=True, executable='/bin/bash')
    print("stdout:", result.stdout)
    print("stderr:", result.stderr)
    # QC
    result = subprocess.run(f'module load plink/1.90b6.24 && plink --bfile {args.ukbb_data_dir}/Atlas2AoU/SNPS/merged_direct --mind 0.05 --geno 0.05 --maf 0.05 --hwe 0.000001 --indep-pairwise 50 5 0.5 --make-bed --out {args.ukbb_data_dir}/Atlas2AoU/SNPS/FILE_QC_direct', shell=True, capture_output=True, text=True, executable='/bin/bash')
    print('--------')
    print("stdout:", result.stdout)
    print("stderr:", result.stderr)
# write to pheno and covar files for plink
# covar file
if not os.path.exists(f'{args.ukbb_data_dir}/Atlas2AoU/COVARIATE_FILE'):
    query = '''SELECT DISTINCT person_id AS IID, year_of_birth AS age, gender_concept_id AS gender FROM PERSON;'''
    demo= pd.read_sql(query, con=engine)
    demo.columns=['IID','Age','Sex']
    demo.set_index('IID',inplace=True)
    # read in pcs
    pcs = pd.read_csv(f'{args.ukbb_data_dir}/principal_components.csv')
    pcs = pcs.rename(columns={'eid':'IID','26201-0.0':'PC1','26201-0.1':'PC2','26201-0.2':'PC3','26201-0.3':'PC4'})
    pcs.set_index('IID',inplace=True)
    # remove those where the pcs are empty
    pcs = pcs.dropna()
    covar = demo.merge(pcs, left_index=True, right_index=True, how='inner')
    # make sex binary - 0 for male and 1 for female
    covar['Sex'] = covar['Sex'].replace({8507: 0, 8532: 1})
    # Read the .fam file and create a list of dictionaries with FID and IID
    data = []
    with open(f'{args.ukbb_data_dir}/Atlas2AoU/SNPS/merged_direct.fam', 'r') as fam_file:
        for line in fam_file:
            fields = line.strip().split()
            family_id = fields[0]
            individual_id = fields[1]
            batch = fields[5]
            data.append({'FID': family_id, 'IID': individual_id, 'Batch':batch})
    fam = pd.DataFrame(data)
    fam = fam.astype(int)
    fam.set_index('IID',inplace=True)

    covar = covar.merge(fam, left_index=True, right_index=True, how='inner')
    covar.reset_index(inplace=True)
    covar.set_index('FID',inplace=True)
    # ensure columns in correct order
    covar=covar[['IID','Age','Sex','Batch','PC1','PC2','PC3','PC4']]
    assert covar.isnull().values.any() == False
    # write to covariate table
    covar.to_csv(f'{args.ukbb_data_dir}/Atlas2AoU/COVARIATE_FILE')
else:
    covar = pd.read_csv(f'{args.ukbb_data_dir}/Atlas2AoU/COVARIATE_FILE',index_col=0)
# pheno
covar.reset_index(inplace=True)
covar.set_index('IID',inplace=True)
# get cases
query = f'''SELECT DISTINCT subject_id as IID, 1 as presence
        FROM COHORT WHERE cohort_definition_id={args.cohortId};'''
cases = pd.read_sql(query, con=engine)
cases.columns=['IID','Phenotype']
# controls are just all other patients
query = f'''SELECT DISTINCT person_id, 0 as presence FROM PERSON p
WHERE person_id NOT IN {tuple(cases['IID'].unique())};'''
controls = pd.read_sql(query, con=engine)
controls.columns=['IID','Phenotype']
assert len(set(cases.IID.unique()).intersection(set(controls.IID.unique()))) == 0
cohort = pd.concat([cases,controls])
# drop people counting for same cohort more than once - just how cohorts work in OHDSI
cohort.drop_duplicates(inplace=True)
assert cohort.shape[0] == cases.shape[0] + controls.shape[0]
cohort.set_index('IID',inplace=True)
# merge to get FID - inner merge so only left with those indvs. with covariates
pheno = cohort.merge(covar,left_index=True, right_index=True, how='inner')
pheno.reset_index(inplace=True)
pheno = pheno[['FID','IID','Phenotype']]
pheno.set_index('FID',inplace=True)
os.makedirs(f'{args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}', exist_ok=True)
pheno.to_csv(f'{args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/PHENOTYPE_FILE')

# submit job for gwas
result = subprocess.run(f'module load plink && plink --bfile {args.ukbb_data_dir}/Atlas2AoU/SNPS/FILE_QC_direct --covar {args.ukbb_data_dir}/Atlas2AoU/COVARIATE_FILE --covar-variance-standardize --pheno {args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/PHENOTYPE_FILE --glm --out {args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/RESULTS_FILE --1 --no-pheno --memory 20000 --threads 10', shell=True, capture_output=True, text=True, executable='/bin/bash')
print("stdout:", result.stdout)
print("stderr:", result.stderr)

# once job is finished, create manhattan plot
plink_results = pd.read_csv(f'{args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/RESULTS_FILE.Phenotype.glm.logistic',sep='\t')
make_manhattan_plot(plink_results,args.analysis_output_dir,args.cohortId,'ukbb')


sig_results = plink_results[(plink_results['P']<5e-8)&(plink_results['TEST']=='ADD')].copy()
sig_results = get_gene_annot(sig_results,f'{args.annot_data_dir}/gencode.v44lift37.annotation.gtf')
# write to txt file
with open(f'{args.analysis_output_dir}/Exons_c{args.cohortId}_ukbb.txt','w') as f:
    for ge in sig_results.explode('gene_annot_exon')[~sig_results.explode('gene_annot_exon')['gene_annot_exon'].isna()].gene_annot_exon.unique().tolist():
        f.write(ge)
        f.write('\n')
