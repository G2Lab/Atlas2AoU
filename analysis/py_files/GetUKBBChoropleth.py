import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import configparser 
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from utilities import get_choropleth_map

analysis_path = '/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/analysis'
ukbb_environ_path = '/gpfs/commons/datasets/controlled/ukbb-gursoylab/environ'

regions = gpd.read_file(f'{analysis_path}/data/Counties_and_Unitary_Authorities_May_2023_UK_BFC_7858717830545248014.geojson')
regions.crs = "EPSG:27700" 

cohortids = [28,288,71]
print('running!')

# Read the configuration from the .ini file (config.ini)
config = configparser.ConfigParser()
config.read('/gpfs/commons/home/anewbury/config.ini') 
# Database configuration
username = config.get('postgres', 'username')
password = config.get('postgres', 'password')
host = config.get('postgres', 'host')
database = config.get('postgres', 'database')
# Create the database engine using the configuration
engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')


birthplace = pd.read_csv(f'{ukbb_environ_path}/ukb678011.csv')
# from baseline assessment
birthplace = birthplace[['eid','129-0.0','130-0.0']].copy()
# north and east coordinates respectively
birthplace.rename({'129-0.0':'N','130-0.0':'E'},inplace=True, axis=1)


birthplace_gdf = gpd.GeoDataFrame(birthplace, geometry=gpd.points_from_xy(birthplace.E, birthplace.N))
birthplace_gdf.crs = "EPSG:27700" # set crs to match the region OSBS1936, BNG
# spatial join
birthplace_gdf = gpd.sjoin(birthplace_gdf, regions, how='left', predicate='within')
assert birthplace_gdf[birthplace_gdf.geometry.isna()].shape[0] == 0

for cohortId in cohortids:
    # get cases
    query = f'''SELECT DISTINCT subject_id
            FROM COHORT WHERE cohort_definition_id={cohortId};'''
    cases = pd.read_sql(query, con=engine)
    cases = set(cases.subject_id.unique())
    # controls are everyone else
    get_choropleth_map(merged=birthplace_gdf,cases=cases,region_column='CTYUA23CD',person_column='eid',regions=regions,
                       output_path=f'{analysis_path}/output',cohortId=cohortId)
