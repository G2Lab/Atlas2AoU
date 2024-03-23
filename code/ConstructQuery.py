# Constructing .py script that can be run on AoU in jupyter notebook
import subprocess
import argparse
from utilities import *
import sys



### READ INPUTS
parser = argparse.ArgumentParser(description="Process some arguments.")
# input from sqlrender
sql = sys.stdin.read()
parser.add_argument('-s', required=True,type=str, help='Database schema')
parser.add_argument('-t', required=True,type=str, help='Database type')
parser.add_argument('-o', required=True,type=str, help='Output file')
parser.add_argument('-c', required=False, nargs='?', type=str, help='Config file for database (optional, not needed for AoU)')
args = parser.parse_args()
assert args.t in ['bigquery','postgresql'], "The database type you specified is not recognized, please make sure it is one of: ('bigquery','postgresql')"
### READ INPUTS

#ALTER SQL AND INSERT TEMPORARY OBSERVATION_PERIOD TABLE
# create temp obs period table
obs_period_query = create_obs_period(DB_SCHEMA=args.s,DB_TYPE=args.t)
sql = alter_sql(sql,obs_period_query,DB_TYPE=args.t,DB_SCHEMA=args.s)


# OUTPUT .PY FILE
if args.t == 'bigquery':
        script = f"""
import os
from google.cloud import bigquery
def create_df():
        sql = f\"\"\"{sql}\"\"\"
        # Construct a BigQuery client object.
        client = bigquery.Client()
        query_job = client.query(sql)
        results = query_job.result()

        # get results
        ohdsi_query_df = results.to_dataframe()
        return ohdsi_query_df
        """
else:
        script = f"""
from sqlalchemy import create_engine
import configparser
import pandas as pd
def create_df():
                config = configparser.ConfigParser()
                config.read('{args.c}')  # Provide the path to your config.ini file
                # Database configuration
                username = config.get('postgres', 'username')
                password = config.get('postgres', 'password')
                host = config.get('postgres', 'host')
                database = config.get('postgres', 'database')
                # Create the database engine using the configuration
                engine = create_engine(f'postgresql://{{username}}:{{password}}@{{host}}/{{database}}')
                sql = f\"\"\"{sql}\"\"\"
                ohdsi_query_df = pd.read_sql(sql, con=engine)
                return ohdsi_query_df
        """

# WRITE TO OUT FILE
with open(args.o,'w') as f:
    f.write(script)
    f.write('\n')
