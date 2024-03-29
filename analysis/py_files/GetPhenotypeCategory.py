#! /gpfs/commons/home/anewbury/miniconda/envs/jupyter/bin/python3
#SBATCH --job-name=get_cat
#SBATCH --partition=pe2
#SBATCH --mail-type=ALL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --mem=200G
#SBATCH --cpus-per-task=4
#SBATCH --time=80:00:00
#SBATCH --output=/gpfs/commons/groups/gursoy_lab/anewbury/get_cat_output.txt
#SBATCH --error=/gpfs/commons/groups/gursoy_lab/anewbury/get_cat_errors.txt

import pandas as pd
import swifter
import csv
import subprocess
import json
import tqdm
import re
import ast
from collections import Counter
import multiprocessing
from functools import partial
from concurrent.futures import ProcessPoolExecutor
import os
import shutil
from json.decoder import JSONDecodeError
import argparse

parser = argparse.ArgumentParser(description='Process hyperparameters.')
parser.add_argument('-f', required=True,type=str, help='Path to aou-atlas-phenotyping folder')
parser.add_argument('-csv_path', required=True,type=str, help='Path to csv with desired OHDSI phenos')
parser.add_argument('-omop_dir', required=True,type=str, help='Path to concept and concept relationship tables')
args = parser.parse_args()

# FUNCTIONS
# get included conditions - return list of included SNOMED condition codes
def GetIncCodes(cohortid,concept,concept_rel):
    # function to write inc/exc edges to separate files which then need to be merged
    # read in json
    included_concepts = []
    json_file = f'{args.f}/analysis/json_files/{cohortid}.json'
    with open(json_file,'r',errors='ignore') as file_:
        data = json.load(file_)
    concept_sets = data['ConceptSets']
    # find any codeset ids with a specification of occurrence count 0 where concepts may not explicitly be marked as excluded
    for lst in concept_sets:
        id = lst['id']
        # occurrence count can be specified or not - if specified and = 0, treat as exclusion
        exc_ids = []
        for rule in data['InclusionRules']:
            rule = rule['expression']['CriteriaList']
            for criteria in rule:
                # in case multiple tables are mentioned in criteria (need to index on table like ConditionOccurrence)
                codeset_ids = []
                for criteria_key in criteria['Criteria']:
                    # phenotype 308 has one criteria with established codeset, and one regarding opthamologist provider specialty without
                    try:
                        codeset_id = criteria['Criteria'][criteria_key]['CodesetId']
                        codeset_ids.append(codeset_id)
                    except KeyError:
                        pass
                # if id is mentioned in inclusion rule - want to make sure specify 1 or greater occurrences to count as inclusion
                if id in codeset_ids:
                    # if count = 0 specified for codeset by any subset rules - count as exclusion
                    for indv_rules in rule:
                        occ_count = int(indv_rules['Occurrence']['Count'])
                        if occ_count == 0:
                            exc_ids.append(id)
        # get concepts - list within items which is within expression
        for concept_dict in lst['expression']['items']:
            # check that concept is a condition & from SNOMED
            concept_id = str(concept_dict['concept']['CONCEPT_ID'])
            # in phenotype 410 this concept is not present, have checked already that phenotype is categorized correctly, so can skip over this error
            if concept_id=='36102152':
                break
            domain_id = concept[concept['concept_id'] == concept_id].domain_id.values[0]
            assert type(domain_id) == str
            if domain_id == 'Condition':
                vocabulary_id = concept[concept['concept_id'] == concept_id].vocabulary_id.values[0]
                if vocabulary_id != 'SNOMED':
                    # map to standard concept(s) which is snomed
                    concept_ids = concept_rel[(concept_rel.concept_id_1=='44820047')&(concept_rel.relationship_id=='Maps to')].concept_id_2.unique().tolist()
                    vocabulary_id = concept[concept['concept_id'].isin(concept_ids)].vocabulary_id.unique().tolist()
                    assert vocabulary_id == ['SNOMED']
                    if not concept_dict['isExcluded'] and id not in exc_ids:
                        included_concepts.extend(concept_ids)
                else:
                    if not concept_dict['isExcluded'] and id not in exc_ids:
                        included_concepts.append(concept_id)
    return included_concepts

# return the corresponding category
def map_to_snomed_index(cohortId,concept_ans,concept,concept_rel,liu_cat):
    print(cohortId,flush=True)
    # get inclusion codes
    inc_codes = GetIncCodes(cohortId,concept,concept_rel)
    # map inclusion codes to snomed index code defined in Liu et al.
    index_code_dict = {key:0 for key in liu_cat['SNOMED code'].unique().tolist()}
    # get ancestor snomed codes for inclusion code
    ancestor_concepts = concept_ans[concept_ans['descendant_concept_id'].isin(inc_codes)].ancestor_concept_id.unique().tolist()
    # map from omop to snomed source codes
    inc_snomed_codes = concept[concept.concept_id.isin(ancestor_concepts)].concept_code.astype(int).unique().tolist()
    # check if any are in index_code_dict (and if so up the count)
    index_code_dict = {key: index_code_dict[key] + inc_snomed_codes.count(key) for key in index_code_dict}
    # get the snomed code with max count
    max_key = max(index_code_dict, key=index_code_dict.get)
    # return the category
    return liu_cat[liu_cat['SNOMED code']==max_key]['Condition category'].values[0]


# parallelize
# Helper function to apply a function to a DataFrame chunk
def apply_func(chunk, func):
    return chunk.apply(func)

# Function to parallelize pandas apply
def parallel_apply(df, func):
    # Split DataFrame into chunks
    chunks = [df[i:i+20] for i in range(0, len(df), 20)]
    
    # Process chunks in parallel
    with ProcessPoolExecutor() as executor:
        # Map function to each chunk
        tasks = [executor.submit(apply_func, chunk, func) for chunk in chunks]
        result = pd.concat([task.result() for task in tasks])

    
    # Concatenate the results from the chunks
    return result
# FUNCTIONS

if __name__ == '__main__':
    # get concepts included in phenotype frorm json file
    phenotypes = pd.read_csv(args.csv_path,index_col=0)
    # make cohort ids strings
    phenotypes['cohortId'] = phenotypes['cohortId'].astype(str)
    concept = pd.read_csv(f'{args.omop_dir}/omop_concept.csv',sep='\t',index_col=False,quoting=csv.QUOTE_NONE,dtype=str)
    concept_ans = pd.read_csv(f'{args.omop_dir}/omop_concept_ancestor.csv',sep='\t',index_col=False,quoting=csv.QUOTE_NONE,dtype=str)
    concept_rel = pd.read_csv(f'{args.omop_dir}/omop_concept_relationship.csv',sep='\t',index_col=False,quoting=csv.QUOTE_NONE,dtype=str)
    liu_cat = pd.read_csv(f'{args.f}/analysis/data/liu_condition_cat.csv')

    # get json files of atlas queries
    # os.makedirs(f'{args.f}/analysis/json_files', exist_ok=True)
    # phenotypes.apply(lambda row: subprocess.call(f'/usr/bin/wget -O {args.f}/analysis/json_files/{row["cohortId"]}.json https://raw.githubusercontent.com/OHDSI/PhenotypeLibrary/ac17b7af55b01ec91eb2ac1ca1ea30473f8ba621/inst/cohorts/{row["cohortId"]}.json', shell=True),axis=1)

    # map to category
    map_to_snomed_index_with_fixed_params = partial(map_to_snomed_index, concept_ans=concept_ans,concept=concept,concept_rel=concept_rel,liu_cat=liu_cat)
    phenotypes['Category'] = parallel_apply(phenotypes['cohortId'], map_to_snomed_index_with_fixed_params)
    phenotypes.to_csv(f'{args.f}/analysis/output/OHDSIPhenotypes_w_cat.csv')
    print('done!')