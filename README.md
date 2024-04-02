This repository allows you to run a query built in [OHDSI Atlas](https://atlas-demo.ohdsi.org/) on the All of Us Researcher workbench. Note that this code manually reconstructs the [observation period](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:observation_period) table since it is not currently constructed properly in All of Us.

---

# Basics

To run on AoU workbench, use the following code (**outside of AoU workbench**):

```bash
./code/Atlas2Aou.sh -p sql_path -s '{os.environ["WORKSPACE_CDR"]}' -t bigquery -o output_path -f path_to_aou-atlas-phenotyping
```

where sql_path is the path to the sql file from Atlas downloaded in the base OHDSI.template format, output_path is where the script should output the .py file that will then be used as a module in AoU, and path_to_aou-atlas-phenotyping is the absolute path to this directory.

Note that the python script created defines a function called create_df, so that within the AoU workbench, to acquire a dataframe with subject_id, cohort_start_date, and cohort_end_date (as in the [cohort table](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:cohort)), one must upload the .py file to workspace bucket and run:

```python
import name_of_py_file.py
df =name_of_py_file.create_df()
```

---

# Requirements

Note that Atlas2AoU.sh has been run successfully with the following dependencies:

- R 4.2.3
- Java 20.0.1
- Python3 packages: subprocess, argparse, sys, re
- R packages: rJava, SqlRender

---

# Sample Run

To perform a sample run, clone this repository and run the following:

```bash
cd path_to_aou-atlas-phenotyping
chmod +x code/Atlas2Aou.sh
./code/Atlas2Aou.sh -p data/sample.sql -s '{os.environ["WORKSPACE_CDR"]}' -t bigquery -o output/sample.py -f path_to_aou-atlas-phenotyping

```

Where path_to_aou-atlas-phenotyping is the absolute path to this directory. Then, upload sample.py to an AoU workspace bucket and run:

```python
import sample.py
df = sample.create_df()
```

---

# OHDSI Phenotype Library Phenotypes

Currently, python scripts for 423 phenotypes from the OHDSI Phenotype Library v3.1.6 have already been run and are stored in the test/bigquery_py_files folder for ease of use in the AoU workbench. (Cohorts with ids 30 and 300 are unable to run in AoU workbench due to a datetime mismatch error). These 423 phenotypes and their name per the OHDSI [Cohorts.csv](https://github.com/OHDSI/PhenotypeLibrary/blob/ac17b7af55b01ec91eb2ac1ca1ea30473f8ba621/inst/Cohorts.csv) file are stored in the data/ folder. They were run from the RunTestOHDSIPhenos.sh script in the test/ folder.

---

# Testing

This code was tested on a Postgres database in which we had write permissions and could run the Atlas query as designed.  Thus, the cohort acquired from a true run of the Atlas query was compared to the cohort acquired from this code, for all 423 OHDSI Phenotype Library phenotypes referred to above. Our test code is present in the test/ folder.

To run the test code for any sql script, on a postgresql databas run the following:

```bash
cd path_to_aou-atlas-phenotyping
chmod +x test/Test.sh
./test/Test.sh -p sql_path -s db_schema -o output_path -f path_to_aou-atlas-phenotyping -c config_file_path
```

where db_schema is the database schema in which the OHDSI standardized vocabulary tables are stored, and and config_file_path is the path to the config file containing information to connect to the postgres database.

This script will write the bigquery .py format file to the output folder if successful, otherwise it will write the unsuccessful sql path to test/unsuccessful_comparisons.txt.

Note that this test has only been performed for postgres syntax and we cannot guarantee that the test code will be compliant with other sql syntaxes.