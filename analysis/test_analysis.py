import unittest
import ast
import pandas as pd
import configparser
from sqlalchemy import create_engine
import numpy as np
from pyplink import PyPlink
import glob
import sys
import configparser
import os
import subprocess

# path to aou phenotyping folder
f = os.environ.get('f')
# path to config file
c = os.environ.get('c')
# path to ukbb data dir
ukbb_data_dir = os.environ.get('ukbb_data_dir')
# path to gtf annot
gtf_annot_path = os.environ.get('gtf_annot_path')


sys.path.append(f'{f}/analysis/py_files')
from utilities import get_gtf

# test UKBB prev. and co-prev estimates
class TestUKBBPrevCoPrev(unittest.TestCase):
    def setUp(self):
        # Read the configuration from the .ini file (config.ini)
        config = configparser.ConfigParser()
        config.read(c)  # Provide the path to your config.ini file
        # Database configuration
        username = config.get('postgres', 'username')
        password = config.get('postgres', 'password')
        host = config.get('postgres', 'host')
        database = config.get('postgres', 'database')
        # Create the database engine using the configuration
        self.engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')
        # diagnoses less than 20 and above 
        self.cohortIds = [223,219,207,211]
        self.ukbb_prev = pd.read_csv(f'{f}/analysis/output/UKBBPrev.csv',index_col=0)
        self.ukbb_coprev = pd.read_csv(f'{f}/analysis/output/UKBBCoPrev.csv',index_col=0)
    def tearDown(self):
        self.engine.dispose()
    def test_ukbb_prev(self):
        # testing individual values
        ukbb_prev_sample = self.ukbb_prev[self.ukbb_prev.cohortId.isin(self.cohortIds)].copy()
        for index, row in ukbb_prev_sample.iterrows(): 
            query = f"SELECT COUNT(DISTINCT subject_id) FROM COHORT WHERE cohort_definition_id = {row['cohortId']}"
            df = pd.read_sql(query, con=self.engine)
            if df.iloc[0]['count'] <21:
                self.assertEqual(row['Count_ukbb'],'Diagnoses <= 20')
                self.assertEqual(row['Prev_ukbb'],'Diagnoses <= 20')
            else:
                self.assertEqual(int(row['Count_ukbb']),df.iloc[0]['count'])
                self.assertEqual(float(row['Prev_ukbb']),int(row['Count_ukbb'])/int(row['N_ukbb']))
        # PII check - counts greater than 20
        # assert that no counts less than 20
        self.assertTrue(all(self.ukbb_prev['Count_ukbb'].apply(lambda x: x=='Diagnoses <= 20' if x== 'Diagnoses <= 20' else int(x)>20)))

        # assert no prevalence that implies count less than 21
        self.assertTrue(all(self.ukbb_prev.apply(lambda x: x['Prev_ukbb']=='Diagnoses <= 20' if x['Prev_ukbb']== 'Diagnoses <= 20' else float(x['Prev_ukbb'])*float(x['N_ukbb']) > 21, axis=1)))

        # assert that prev and count are both Diagnoses <= 20 at the same time
        def check_diag(row):
            # Check if 'Diagnoses <= 20' in both columns
            has_diag_c1 = 'Diagnoses <= 20' == row['Count_ukbb']
            has_diag_c2 = 'Diagnoses <= 20' == row['Prev_ukbb']
            # Assert that either both have it or neither has it
            self.assertEqual(has_diag_c1, has_diag_c2)

        # Apply the check to each row
        self.ukbb_prev.apply(check_diag, axis=1)
    def test_ukbb_coprev(self):
        print('testing coprev')
        # test coprev
        cohortIds_in_coprev = []
        # for those with diagnoses less than 20 - should not be in coprev
        for cohortId in self.cohortIds:
            if self.ukbb_prev[self.ukbb_prev['cohortId']==cohortId]['Count_ukbb'].values[0] != 'Diagnoses <= 20':
                cohortIds_in_coprev.append(cohortId)
            else:
                self.assertTrue(cohortId not in self.ukbb_coprev.index)
        N_ukbb = self.ukbb_prev['N_ukbb'].unique()[0]
        #for those with cell counts more than 20, get coprev values and check
        for cohortId1 in cohortIds_in_coprev:
            sql = f'SELECT DISTINCT subject_id FROM COHORT WHERE cohort_definition_id = {cohortId1};'
            c1 = set(pd.read_sql(sql, con=self.engine).subject_id.unique())
            for cohortId2 in cohortIds_in_coprev:
                sql = f'SELECT DISTINCT subject_id FROM COHORT WHERE cohort_definition_id = {cohortId2};'
                c2 = set(pd.read_sql(sql, con=self.engine).subject_id.unique())
                len_int = len(c1.intersection(c2))
                coprev = len_int/N_ukbb
                self.assertTrue(abs(coprev - self.ukbb_coprev.loc[cohortId1,str(cohortId2)])<1e-10)
        # check that there is no co-prev count less than 20
        self.assertTrue(all([(i*N_ukbb>20)|(np.isnan(i)) for i in self.ukbb_coprev.values.flatten().tolist()]))
        # check that ukbb coprev diag equals ukbb prev
        coprev_make_prev = [self.ukbb_coprev.loc[i, str(i)] for i in self.ukbb_coprev.index]
        coprev_make_prev = pd.DataFrame(coprev_make_prev,index = self.ukbb_coprev.index,columns = ['Prev_ukbb_from_coprev'])
        coprev_make_prev.reset_index(inplace=True)
        coprev_make_prev.columns = ['cohortId','Prev_ukbb_from_coprev']
        test = self.ukbb_prev.merge(coprev_make_prev,how='outer',on='cohortId')
        test = test[~test['Prev_ukbb_from_coprev'].isna()].copy()
        test.Prev_ukbb = test.Prev_ukbb.astype(float)
        self.assertTrue(all(np.isclose(test['Prev_ukbb_from_coprev'], test['Prev_ukbb'], atol=1e-5)))

        

# test UKBBSDOH estimates
class TestUKBBSDOH(unittest.TestCase):
    def setUp(self):
        self.ukbb_sdoh = pd.read_csv(f'{f}/analysis/output/UKBBSDOH.csv',index_col=0)
        file_path = f'{f}/analysis/data/MappingMain-FineGrained.xlsx'
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        self.sheets_dict = pd.read_excel(xls, sheet_name=None)
        xls.close()
        self.cohortids = [28, 288, 71]
        self.ukbb_prev = pd.read_csv(f'{f}/analysis/output/UKBBPrev.csv',index_col=0)


        # Read the configuration from the .ini file (config.ini)
        config = configparser.ConfigParser()
        config.read(c)  # Provide the path to your config.ini file
        # Database configuration
        username = config.get('postgres', 'username')
        password = config.get('postgres', 'password')
        host = config.get('postgres', 'host')
        database = config.get('postgres', 'database')
        # Create the database engine using the configuration
        self.engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')
    def tearDown(self):
        self.engine.dispose()
    def test_ukbb_sdoh(self):
        # manual check: all data coding files appear to correspond to their correct UKBB data field

        for col in self.ukbb_sdoh.columns:
            self.ukbb_sdoh[col] = self.ukbb_sdoh[col].apply(ast.literal_eval)

        # make sure all categorical columns sum to 100
        for col in self.ukbb_sdoh.columns:
            if col != 'smoking_pack_years':
                for index, row in self.ukbb_sdoh.iterrows():
                    self.assertTrue(abs(sum(row[col].values()) - 1) < 1e-10)

        # manual check - smoking pack years appears in reasonable range

        # check that the only category values are those spelled out in mapping main
        for k in self.sheets_dict.keys():
            if k != 'Sheet 1':
                for dict_ in self.ukbb_sdoh[k].values:
                    self.assertTrue(all([i in self.sheets_dict[k].category_name.unique() for i in dict_.keys()]))

        # check race as an example column
        for cohortId in self.cohortids:
            # query for race categories for each cohort
            sql = f'''SELECT
            DISTINCT p.person_id AS eid,
            race_concept.concept_name AS race
            FROM
                PERSON p
            INNER JOIN COHORT co ON p.person_id = co.subject_id
            LEFT JOIN CONCEPT race_concept ON p.race_concept_id = race_concept.concept_id
            WHERE cohort_definition_id = {cohortId};'''
            query_df = pd.read_sql(sql, con=self.engine)
            query_df.replace({'race': {'Asian Indian': 'Asian', 'Pakistani': 'Asian','Asian':'Asian','Chinese':'Asian','Bangladeshi':'Asian','African':'Black'}},inplace=True)
            query_df.race = query_df.race.replace('No matching concept', np.nan)

            # check that equal to UKBBSDOH csv
            assert self.ukbb_sdoh.loc[cohortId]['race'] == query_df.race.value_counts(normalize=True).to_dict()
        # PII check
        # check that no proportions indicate cell counts less than 20 (if they do this isn't nec. a problem since this could mean cell count still greater than 20 but looks like it's not after nan and other vals removed)
        for col in self.ukbb_sdoh.columns:
            if col != 'smoking_pack_years':
                for index, row in self.ukbb_sdoh.iterrows():
                    cohortId = index
                    N_cohort = int(self.ukbb_prev[self.ukbb_prev['cohortId']==cohortId]['Count_ukbb'].values[0])
                    dict_ = row[col]
                    for i in dict_.values():
                        self.assertTrue(N_cohort*i > 20)

class TestGWAS(unittest.TestCase):
    def setUp(self):
        self.cohortids = [28,288,71]
        # Read the configuration from the .ini file (config.ini)
        config = configparser.ConfigParser()
        config.read('/gpfs/commons/home/anewbury/config.ini')  # Provide the path to your config.ini file
        # Database configuration
        username = config.get('postgres', 'username')
        password = config.get('postgres', 'password')
        host = config.get('postgres', 'host')
        database = config.get('postgres', 'database')
        self.Atlas2AoU_gwas_dir = f'{ukbb_data_dir}/Atlas2AoU'
        self.imputation_dir = f'{ukbb_data_dir}/ImputationV3'
        # Create the database engine using the configuration
        self.engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')
        self.covar_iids = set(pd.read_csv(f'{self.Atlas2AoU_gwas_dir}/COVARIATE_FILE').IID.unique())
    
    # read in pheno file and make sure correct
    def test_pheno_file(self):
        for cohortId in self.cohortids:
            pheno_file = pd.read_csv(f'{self.Atlas2AoU_gwas_dir}/PHENO_{cohortId}/PHENOTYPE_FILE')
            sql = f'SELECT DISTINCT subject_id FROM COHORT WHERE cohort_definition_id = {cohortId};'
            sql_cases = set(pd.read_sql(sql, con=self.engine).subject_id.unique())
            control_query = f'SELECT DISTINCT person_id FROM PERSON WHERE person_id NOT IN {tuple(sql_cases)}'
            sql_control = set(pd.read_sql(control_query, con=self.engine).person_id.values.tolist())
            # post subset
            assert set(pheno_file[pheno_file['Phenotype']==1].IID.tolist()).issubset(set(sql_cases))
            assert set(pheno_file[pheno_file['Phenotype']==0].IID.tolist()).issubset(set(sql_control))

            # check that those that are in pheno file are also in covar
            assert set(pheno_file[pheno_file['Phenotype']==1].IID.tolist()).issubset(self.covar_iids)

    # test_covar_file and test_qc taken from test_gwas
    def test_qc(self):
        # ensure that qc variables are as expected
        params = {'maf': 0.05,
        'hwe': format(0.000001, '.6f'), # 1e-6
        'geno': 0.05}
        # ensure that qc variables are as expected
        for chr_ in range(1,23):
            qc_log_file = f'{self.imputation_dir}/Chr{chr_}/chr{chr_}.log'
            with open(qc_log_file,'r') as f:
                lines = f.readlines()
                for param, param_value in params.items():
                    for line in lines:
                        if line.startswith(f"  --{param}"):
                            self.assertEqual(line, f"  --{param} {param_value}\n")
        # test genotype qc - 
        with open(f'{self.ukbb_data_dir}/anewbury/SNPS/pass_info_score_and_dup.txt') as f:
            passed_gqc = f.readlines()
            passed_gqc = [i.strip('\n') for i in passed_gqc]
        file_qc_bim_df = pd.read_csv(f'{self.ukbb_data_dir}/anewbury/SNPS/FILE_QC.bim', sep='\t', header=None)
        file_qc_bim_df.columns = ['#CHROM', 'ID', 'cM', 'POS', 'Allele1', 'Allele2']
        file_qc_bim_df['passed'] = file_qc_bim_df['ID'].isin(passed_gqc)
        # ensure no duplicate rsids
        assert len(passed_gqc) == len(set(passed_gqc))
        # remove duplicates from those not passed --> rest should be due to indels/INFO score errors
        assert len(file_qc_bim_df[file_qc_bim_df.ID.duplicated(keep=False)].ID.values) == 0
        file_qc_bim_df.drop_duplicates(subset='ID', keep=False, inplace=True)
        # make sure no indels passed
        file_qc_bim_df['alleles'] = file_qc_bim_df['Allele1'] + file_qc_bim_df['Allele2']
        file_qc_bim_df['alleles'] = file_qc_bim_df['alleles'].apply(lambda x: ''.join(sorted(x)))
        file_qc_bim_df['indel'] = file_qc_bim_df['alleles'].apply(lambda x: True if len(x)>2 else False)
        assert file_qc_bim_df[file_qc_bim_df['indel']==True].shape[0] == 367462
        file_qc_bim_df = file_qc_bim_df[file_qc_bim_df['indel']==False].copy()
        

        mfi_df = []
        for chr in range (1,23):
            mfi = pd.read_csv(f'{self.imputation_dir}/mfi/ukb_mfi_chr{chr}_v3.txt', sep='\t', header=None)
            mfi.columns = ['Alternate_id', 'ID', 'Position', 'Allele1', 'Allele2', 'MAF', 'Minor Allele', 'Info score'] # from https://biobank.ndph.ox.ac.uk/showcase/refer.cgi?id=531
            mfi_df.append(mfi)
        mfi_df = pd.concat(mfi_df)
        mfi_df['alleles'] = mfi_df['Allele1'] + mfi_df['Allele2']
        mfi_df['alleles'] = mfi_df['alleles'].apply(lambda x: ''.join(sorted(x)))
        file_qc_bim_df = file_qc_bim_df.merge(mfi_df, how='left', on=['ID', 'alleles'])
        # check that those that passed have instance of info score above 0.3 and vice versa
        assert file_qc_bim_df[(file_qc_bim_df['passed'] == True)&(file_qc_bim_df['Info score']<=0.3)].shape[0]==0
        assert file_qc_bim_df[(file_qc_bim_df['passed'] == False)&(file_qc_bim_df['Info score']>0.3)].shape[0]==0
        
        # check numbers
        assert len(passed_gqc) == 3686405
        # test that passed_gqc are the only ones in FINAL_QC.bim
        final_qc_bim_df = pd.read_csv(f'{self.ukbb_data_dir}/anewbury/SNPS/FINAL_QC.bim', sep='\t', header=None)
        final_qc_bim_df.columns = ['#CHROM', 'ID', 'cM', 'POS', 'Minor Allele', 'Major Allele']
        assert set(final_qc_bim_df.ID.unique()) == set(passed_gqc)

        # test sample qc - 
        # check that all samples in sample_out are: (1) not negative IIDs (not withdrawn) (2) not with F_MISS above 0.05 (not missing) (3) not in king matrix more than 10 times
        passed_sqc = pd.read_csv(f'{self.ukbb_data_dir}/anewbury/SNPS/pass_sample_qc.txt', sep='\t', header = None)
        passed_sqc.columns = ['FID','IID']
        passed_sqc = passed_sqc.IID.unique().tolist()
        assert all([i>0 for i in passed_sqc]) # none withdrawn
        miss = pd.read_csv(f'{self.ukbb_data_dir}/DirectAssayed/sample_qc/missing.imiss', sep='\s+')
        assert miss[(miss.IID.isin(passed_sqc))&(miss.F_MISS>0.05)].shape[0] == 0 # not missing
        result = subprocess.run(['cmp', '-s', f'{self.ukbb_data_dir}/anewbury/SNPS/pass_sample_qc.txt', f'{self.ukbb_data_dir}/DirectAssayed/sample_qc/kingunrelated.txt'], capture_output=True)
        assert result.returncode == 0

        # check that all other samples in FILE_QC are one of the following ^ and make sure qc data matches
        # read in file_qc_fam
        fam_df = pd.read_csv(f'{self.ukbb_data_dir}/DirectAssayed/merged_direct.fam', sep='\t', header=None)
        fam_df.columns = ['FID', 'IID', 'Father', 'Mother', 'Sex', 'Phenotype']
        not_passed_sqc = set(fam_df.IID.unique().tolist())-set(passed_sqc)
        imputed_fam_df = pd.read_csv(f'{self.ukbb_data_dir}/anewbury/SNPS/FILE_QC.fam', sep='\s+', header=None)
        imputed_fam_df.columns = ['FID', 'IID', 'Father', 'Mother', 'Sex', 'Phenotype']
        # remove not in imputed
        not_passed_sqc = [i for i in not_passed_sqc if i in imputed_fam_df.IID.unique()]
        # remove negative (withdrawn)
        init_len = len(not_passed_sqc)
        not_passed_sqc = [i for i in not_passed_sqc if i>0]
        assert self.qc_data[self.qc_data['qc type']=='withdrawn']['# removed'].values[0] == init_len-len(not_passed_sqc)
        # remove high missingness
        init_len = len(not_passed_sqc)
        not_passed_sqc = [i for i in not_passed_sqc if i in miss[miss.F_MISS<=0.05].IID.unique()]
        assert self.qc_data[self.qc_data['qc type']=='miss']['# removed'].values[0] == init_len-len(not_passed_sqc)
        # remove high relatedness
        init_len = len(not_passed_sqc)
        unrelated_toberemoved = pd.read_csv(f'{self.ukbb_data_dir}/DirectAssayed/sample_qc/kingunrelated_toberemoved.txt', sep='\t', header=None)
        not_passed_sqc = [i for i in not_passed_sqc if i not in unrelated_toberemoved[1].unique().tolist()]
        assert self.qc_data[self.qc_data['qc type']=='relatedness']['# removed'].values[0] == unrelated_toberemoved.shape[0]
        # should be empty
        assert len(not_passed_sqc) == 0
    
    def test_covar_file(self):
        # make sure that age, gender, FID and PCs are correct
        covar = pd.read_csv(f'{self.ukbb_data_dir}/anewbury/COVARIATE_FILE')
        covar = covar.rename(columns={'Sex':'sex'})
        covar['yob'] = 2012-covar['Age']
        covar['sex'] = covar['sex'].apply(lambda x: 8507 if x == 0 else 8532)
        covar.set_index('IID',inplace=True)
        covar.sort_index(inplace=True)

        # batch 
        bileve_iids = covar[covar['Array']==0].index.unique()
        non_bileve_iids = covar[covar['Array']!=0].index.unique()
        bileve_miss = pd.read_csv(f'{self.ukbb_data_dir}/DirectAssayed/sample_qc/bileve_check.smiss', sep='\s+')
        assert set(bileve_iids).issubset(set(bileve_miss[bileve_miss['F_MISS']!=1].IID.unique()))
        assert set(non_bileve_iids).issubset(set(bileve_miss[bileve_miss['F_MISS']==1].IID.unique()))

        # check age,sex
        query = f'SELECT person_id AS "IID", gender_concept_id AS sex, year_of_birth AS yob FROM person WHERE person_id IN {tuple(covar.index.unique())}'
        df = pd.read_sql(query, con=self.engine)
        df.set_index('IID',inplace=True)
        df.sort_index(inplace=True)
        assert df.equals(covar[['sex','yob']])

        # check PCs
        pcs = pd.read_csv(f'{self.ukbb_data_dir}/DirectAssayed/sample_qc/pcs.txt', sep='\t')
        pcs.set_index('IID',inplace=True)
        pcs.sort_index(inplace=True)
        pc_columns = [f'PC{i}' for i in range(1,21)]
        assert set(pcs.index) == set(covar.index) 
        assert pcs.equals(covar[['FID']+pc_columns])

        # check that covar only includes final samples
        passed_sqc = pd.read_csv(f'{self.ukbb_data_dir}/anewbury/SNPS/pass_sample_qc.txt', sep='\t', header=None)
        passed_sqc.columns=['FID','IID']
        passed_sqc = passed_sqc.IID.unique().tolist()
        assert set(passed_sqc) == set(pcs.index)

    def test_GWASCompleted(self):
        # test PLINK GWAS completed for all - and with the right snps
        for cohortId in self.cohortids:
            with open(f'{self.Atlas2AoU_gwas_dir}/PHENO_{cohortId}/RESULTS_FILE.log','r') as file_:
                log_file = file_.read()
                # make sure run completed 
                assert (f'Results written to {self.Atlas2AoU_gwas_dir}/PHENO_{cohortId}/RESULTS_FILE.Phenotype.glm.logistic.hybrid' in log_file)

                # make sure correct snps written
                assert f'{self.ukbb_data_dir}/SNPS/FINAL_QC.bim' in log_file

    def test_gwas_exons(self):
        gtf = get_gtf(gtf_annot_path)
        for cohortId in [28,288,71]:
            df = pd.read_csv(f'{self.Atlas2AoU_gwas_dir}/PHENO_{cohortId}/RESULTS_FILE.Phenotype.glm.logistic.hybrid',sep='\t')
            df = df[(df['P']<5e-8)&(df['TEST']=='ADD')].copy()
            # read in sig exons
            with open(f'{f}/analysis/output/Exons_c{cohortId}_ukbb.txt','r') as file_:
                sig_exons = file_.readlines()
            sig_exons = [i.strip('\n') for i in sig_exons]

            for sig_exon in sig_exons:
                # get regions of these sig exons
                sig_rgns = gtf[gtf.gene_name==sig_exon]
                snp_in_rgn = 0
                # check that there exist sig snps in these regions
                for _,row in sig_rgns.iterrows():
                    if df[(df['POS']<row['end'])&(df['POS']>row['start'])&(df['#CHROM']==row['seqname'])].shape[0] >0:
                        snp_in_rgn += 1
                assert snp_in_rgn>0
    def test_err_code(self):
        for cohortId in [28,288,71]:
            plink_results = pd.read_csv(f'{ukbb_data_dir}/Atlas2AoU/PHENO_{cohortId}/RESULTS_FILE.Phenotype.glm.logistic.hybrid',sep='\t')
            assert plink_results['ERRCODE'].unique().tolist() == ['.']