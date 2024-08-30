#! /gpfs/commons/home/anewbury/miniconda/envs/jupyter/bin/python3
#SBATCH --job-name=atlas2aougwas
#SBATCH --nodes=1
#SBATCH --mem=64G
#SBATCH --cpus-per-task=8
#SBATCH --time=120:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --output=output.txt
#SBATCH --error=errors.txt
#SBATCH --constraint=v3|v5
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


# QC and COVARIATE_FILE creation from /gpfs/commons/groups/gursoy_lab/anewbury/gwas/code/SetupGWASScore.py

# write to pheno file plink
# covar file
covar = pd.read_csv(f'{args.ukbb_data_dir}/anewbury/COVARIATE_FILE',index_col=0)
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
result = subprocess.run(f'module unload plink && module load plink/2.0a5.10 && plink --bfile {args.ukbb_data_dir}/anewbury/SNPS/FINAL_QC --covar {args.ukbb_data_dir}/anewbury/COVARIATE_FILE --covar-variance-standardize --pheno {args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/PHENOTYPE_FILE --glm omit-ref --out {args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/RESULTS_FILE --1 --no-pheno', shell=True, capture_output=True, text=True, executable='/bin/bash')
print("stdout:", result.stdout)
print("stderr:", result.stderr)

# once job is finished, create manhattan plot
plink_results = pd.read_csv(f'{args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/RESULTS_FILE.Phenotype.glm.logistic.hybrid',sep='\t')
make_manhattan_plot(plink_results,args.analysis_output_dir,args.cohortId,'ukbb')


sig_results = plink_results[(plink_results['P']<5e-8)&(plink_results['TEST']=='ADD')].copy()
sig_results = get_gene_annot(sig_results,f'{args.annot_data_dir}/gencode.v44lift37.annotation.gtf')
# write to txt file
with open(f'{args.analysis_output_dir}/Exons_c{args.cohortId}_ukbb.txt','w') as f:
    for ge in sig_results.explode('gene_annot_exon')[~sig_results.explode('gene_annot_exon')['gene_annot_exon'].isna()].gene_annot_exon.unique().tolist():
        f.write(ge)
        f.write('\n')
