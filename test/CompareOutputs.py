# Compare outputs from AoU Atlas and real sql query 
# This code is only meant to handle postgresql queries currently
import argparse
import sys
import os
import configparser
import pandas as pd
from sqlalchemy import create_engine
import re
import importlib


### READ INPUTS
parser = argparse.ArgumentParser(description="Process some arguments.")
parser.add_argument('-s', required=True,type=str, help='Database schema')
parser.add_argument('-t', required=True,type=str, help='Database type')
parser.add_argument('-o', required=True,type=str, help='Output file')
parser.add_argument('-p', required=True,type=str, help='Sql file path, for reference if there is an error and also for module name (same as sql file name)')
parser.add_argument('-c', required=False, type=str, help='Config file for database (optional, not needed for AoU)')
args = parser.parse_args()
assert args.t =='postgresql', "This code is only meant to handle postgresql queries currently"
### READ INPUTS

# input from sqlrender
sql = sys.stdin.read()

# Run .py file from AoU2Atlas
sys.path.append(os.path.dirname(args.o))
module_name = os.path.basename(args.o)
module_name = module_name.replace('.py','')
module = importlib.import_module(module_name)
atlas_to_aou_df = module.create_df()

# Query db
config = configparser.ConfigParser()
config.read({args.c})  # Provide the path to your config.ini file
# Database configuration
username = config.get('postgres', 'username')
password = config.get('postgres', 'password')
host = config.get('postgres', 'host')
database = config.get('postgres', 'database')
# Create the database engine using the configuration
engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')
# only alteration after sqlrender is to remove writing to cohort table and change to select *
# Delete end - case insensitive
sql = re.split(re.escape('delete from '), sql, maxsplit=1, flags=re.IGNORECASE)[0]
# add select * from final cohort 
sql = sql + 'SELECT * FROM final_cohort;'
db_query_df = pd.read_sql(sql, con=engine)

# Check that people in cohort are same for both methods of querying
print(set(atlas_to_aou_df.person_id.unique()) == set(db_query_df.person_id.unique()))

