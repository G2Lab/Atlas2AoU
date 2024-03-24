import unittest
import ast
import pandas as pd
import configparser
from sqlalchemy import create_engine
import numpy as np

# test UKBB prev. and co-prev estimates
class TestUKBBPrevCoPrev(unittest.TestCase):
    def setUp(self):
        # Read the configuration from the .ini file (config.ini)
        config = configparser.ConfigParser()
        config.read('/gpfs/commons/home/anewbury/config.ini')  # Provide the path to your config.ini file
        # Database configuration
        username = config.get('postgres', 'username')
        password = config.get('postgres', 'password')
        host = config.get('postgres', 'host')
        database = config.get('postgres', 'database')
        # Create the database engine using the configuration
        self.engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')
        # diagnoses less than 20 and above 
        self.cohortIds = [223,219,207,211]
        self.ukbb_prev = pd.read_csv('/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/analysis/output/UKBBPrev.csv',index_col=0)
        self.ukbb_coprev = pd.read_csv('/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/analysis/output/UKBBCoPrev.csv',index_col=0)
    def tearDown(self):
        self.engine.dispose()
    def test_ukbb_prev(self):
        # testing individual values
        ukbb_prev_sample = self.ukbb_prev[self.ukbb_prev.cohortId.isin(self.cohortIds)].copy()
        for index, row in ukbb_prev_sample.iterrows(): 
            query = f"SELECT COUNT(DISTINCT subject_id) FROM COHORT WHERE cohort_definition_id = {row['cohortId']}"
            df = pd.read_sql(query, con=self.engine)
            if df.iloc[0]['count'] <20:
                self.assertEqual(row['Count_ukbb'],'Diagnoses <= 20')
                self.assertEqual(row['Prev_ukbb'],'Diagnoses <= 20')
            else:
                self.assertEqual(int(row['Count_ukbb']),df.iloc[0]['count'])
                self.assertEqual(float(row['Prev_ukbb']),int(row['Count_ukbb'])/int(row['N_ukbb']))
        # PII check - counts greater than 20
        # assert that no counts less than 20
        self.assertTrue(all(self.ukbb_prev['Count_ukbb'].apply(lambda x: x=='Diagnoses <= 20' if x== 'Diagnoses <= 20' else int(x)>20)))

        # assert no prevalence that implies count less than 20
        self.assertTrue(all(self.ukbb_prev.apply(lambda x: x['Prev_ukbb']=='Diagnoses <= 20' if x['Prev_ukbb']== 'Diagnoses <= 20' else float(x['Prev_ukbb'])*float(x['N_ukbb']) > 20, axis=1)))

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
        self.ukbb_sdoh = pd.read_csv('/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/analysis/output/UKBBSDOH.csv',index_col=0)
        file_path = f'/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/analysis/data/MappingMain-FineGrained.xlsx'
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        self.sheets_dict = pd.read_excel(xls, sheet_name=None)
        xls.close()
        self.cohortids = [28, 288, 71]
        self.ukbb_prev = pd.read_csv('/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/analysis/output/UKBBPrev.csv',index_col=0)


        # Read the configuration from the .ini file (config.ini)
        config = configparser.ConfigParser()
        config.read('/gpfs/commons/home/anewbury/config.ini')  # Provide the path to your config.ini file
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
            