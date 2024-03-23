#! /gpfs/commons/home/anewbury/miniconda/envs/jupyter/bin/python3
#SBATCH --job-name=atlas2aou_gwas
#SBATCH --partition=pe2
#SBATCH --mail-type=ALL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --mem=15G
#SBATCH --cpus-per-task=8
#SBATCH --time=80:00:00
#SBATCH --output=/gpfs/commons/groups/gursoy_lab/anewbury/atlas2aou_gwas_output.txt
#SBATCH --error=/gpfs/commons/groups/gursoy_lab/anewbury/atlas2aou_gwas_errors.txt

#  run GWAS using PLINK for UKBB

import pandas as pd
import os
from sqlalchemy import create_engine
import argparse
import configparser
import subprocess
from plotnine import *
import numpy as np
from gtfparse import read_gtf



parser = argparse.ArgumentParser(description='Process parameters.')
parser.add_argument('--ukbb_data_dir', type=str, help='ukbb data directory',default='')
parser.add_argument('--cohortId', type=str, help='id of ohdsi cohort',default='')
parser.add_argument('--analysis_output_dir', type=str, help='analysis output directory (in aou-atlas-phenotyping folder)',default='')
parser.add_argument('--annot_data_dir', type=str, help='data directory where gtf annot file gencode.v44lift37.annotation.gtf is',default='')
parser.add_argument('--c', type=str, help='path to config file for postgres db',default='')
args = parser.parse_args()

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
if not os.path.exists(f'{args.ukbb_data_dir}/SNPS/FILE_QC.bed'):
    result = subprocess.run(f'module load plink/1.90b6.24 && plink --vcf {args.ukbb_data_dir}/ImputationV3/allchromosomes.vcf.gz --make-bed --out {args.ukbb_data_dir}/Atlas2AoU/SNPS/merged', shell=True, capture_output=True, text=True, executable='/bin/bash')
    # QC
    result = subprocess.run(f'module load plink/1.90b6.24 && plink --bfile {args.ukbb_data_dir}/Atlas2AoU/SNPS/merged --mind 0.05 --geno 0.05 --maf 0.05 --hwe 0.000001 --indep-pairwise 50 5 0.5 --make-bed --out {args.ukbb_data_dir}/SNPS/FILE_QC', shell=True, capture_output=True, text=True, executable='/bin/bash')

# write to pheno and covar files for plink
# covar file
if not os.path.exists(f'{args.ukbb_data_dir}/Atlas2AoU/COVARIATE_FILE'):
    query = '''SELECT DISTINCT person_id AS IID, 2012-year_of_birth AS age, gender_concept_id AS gender FROM PERSON;'''
    demo= pd.read_sql(query, con=engine)
    demo.columns=['IID','Age','Sex']
    demo.set_index('IID',inplace=True)
    # read in pcs
    pcs = pd.read_csv(f'{os.path.dirname(args.ukbb_data_dir)}/principal_components.csv')
    pcs = pcs.rename(columns={'eid':'IID','26201-0.0':'PC1','26201-0.1':'PC2','26201-0.2':'PC3','26201-0.3':'PC4'})
    pcs.set_index('IID',inplace=True)
    # remove those where the pcs are empty
    pcs = pcs.dropna()
    covar = demo.merge(pcs, left_index=True, right_index=True, how='inner')
    # make sex binary - 0 for male and 1 for female
    covar['Sex'] = covar['Sex'].replace({8507: 0, 8532: 1})
    # Read the .fam file and create a list of dictionaries with FID and IID
    data = []
    with open(f'{args.ukbb_data_dir}/Atlas2AoU/SNPS/merged.fam', 'r') as fam_file:
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
result = subprocess.run(f'module load plink && plink --bfile {args.ukbb_data_dir}/Atlas2AoU/SNPS/FILE_QC --covar {args.ukbb_data_dir}/Atlas2AoU/COVARIATE_FILE --pheno {args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/PHENOTYPE_FILE --glm --out {args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/RESULTS_FILE --1 --no-pheno --memory 20000 --threads 10', shell=True, capture_output=True, text=True, executable='/bin/bash')


# once job is finished, create manhattan plot
plink_results = pd.read_csv(f'{args.ukbb_data_dir}/Atlas2AoU/PHENO_{args.cohortId}/RESULTS_FILE.Phenotype.glm.logistic',sep='\t')
plink_results = plink_results[plink_results['TEST']=='ADD'].copy()
plink_results['color_group'] = plink_results['#CHROM'] % 2
# get chromosome midpoint and adjust pos for graphing
chromosome_lengths = plink_results.groupby('#CHROM')['POS'].max()
chromosome_starts = chromosome_lengths.cumsum().shift(1).fillna(0)
plink_results = plink_results.merge(chromosome_starts.rename('start_pos'), on='#CHROM')
plink_results['adjusted_POS'] = plink_results['POS'] + plink_results['start_pos']
chromosome_mid = plink_results.groupby('#CHROM')['adjusted_POS'].apply(lambda x: (x.min() + x.max()) / 2)
chromosome_mid = chromosome_mid.to_dict()
plink_results['#CHROM'] = pd.Categorical(plink_results['#CHROM'])
# -log(p-value)
plink_results['minuslog10p'] = -np.log10(plink_results['P'])
plot = (ggplot(plink_results) 
 + geom_point(aes(x='adjusted_POS', y='minuslog10p', colour='factor(color_group)'), alpha=0.5,size=1)  # Plot points colored by chromosome
 + scale_color_manual(values=["#6F8FAF", "#00008B"],guide=False)
 + labs(title='Manhattan Plot', x='Chromosome', y='-log10(p-value)')  
 + theme(panel_grid=element_blank(),panel_border=element_blank(),legend_position=None, panel_background=element_rect(fill='white'),axis_text_x=element_text(size=6),figure_size=(10, 6)) 
+ scale_x_continuous(labels=list(chromosome_mid.keys()), breaks=list(chromosome_mid.values()))
+ geom_hline(yintercept=-np.log10(5e-8), colour='grey',linetype='dashed', color='gray')

)
plot.save(f'{args.analysis_output_dir}/Manhattan_Plot_c{args.cohortId}', dpi=300)

# get gene exon annotations
#get gene exons of the snps
def get_gtf(PATH_GTF):
    # gets info on autosomal, protein-coding genes
    gtf = pd.DataFrame()
    gtf= read_gtf(PATH_GTF, 
                usecols=['seqname','gene_id','feature','start',
                        'end', 'gene_type','strand', 'gene_name'])
    # get autosomal chromosome
    gtf = gtf.to_pandas()
    gtf.seqname = gtf['seqname'].apply(lambda x: x.replace('chr', ''))
    gtf = gtf[gtf['seqname'].isin([str(i) for i in range(1,23)])]
    gtf['seqname'] = gtf['seqname'].cat.remove_unused_categories()
    gtf.seqname = gtf.seqname.astype(int)
    # get protein-coding genes
    gtf = gtf[gtf.gene_type=='protein_coding'].copy()
    # can remove rows with feature 'gene' - they are redundant
    gtf = gtf[gtf.feature!='gene'].copy()
    return gtf

def get_gene_annot(results, ANNOT_PATH, gtf = None):
    # return dataframe that has additional column with gene list and another with the corresponding features
    if gtf is None:
        gtf = get_gtf(f'{ANNOT_PATH}/gencode.v44lift37.annotation.gtf')
    #get results in genes
    results[['gene_annot_exon', 'gene_annot_other']] = results.apply(lambda row: pd.Series([
        gtf[(gtf['start'] <= row['POS']) & (gtf['end'] >= row['POS']) & (row['#CHROM'] == gtf['seqname'])&(gtf['feature']=='exon')]['gene_name'].unique().tolist(),
        gtf[(gtf['start'] <= row['POS']) & (gtf['end'] >= row['POS']) & (row['#CHROM'] == gtf['seqname'])&(gtf['feature'].isin(['UTR', 'CDS', 'start_codon', 'stop_codon']))]['gene_name'].unique().tolist()
    ]),axis=1)
    # make sure that only returning unique vals
    assert all(results['gene_annot_exon'].apply(lambda lst: len(lst) == len(set(lst))).values)
    assert all(results['gene_annot_other'].apply(lambda lst: len(lst) == len(set(lst))).values)
    return results

sig_results = plink_results[plink_results['P']<5e-8].copy()
sig_results = get_gene_annot(sig_results,args.annot_data_dir)
# write to txt file
with open(f'{args.analysis_output_dir}/Exons_c{args.cohortId}','w') as f:
    for ge in sig_results.explode('gene_annot_exon')[~sig_results.explode('gene_annot_exon')['gene_annot_exon'].isna()].gene_annot_exon.unique().tolist():
        f.write(ge)
        f.write('\n')
