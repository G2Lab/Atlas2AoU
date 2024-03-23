#! /usr/bin/bash
#SBATCH --job-name=TestOHDSIphenos
#SBATCH --partition=pe2
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=120:00:00
#SBATCH --mail-type=ALL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --error=/gpfs/commons/groups/gursoy_lab/anewbury/TestOHDSIphenos_errors.txt
#SBATCH --output=/gpfs/commons/groups/gursoy_lab/anewbury/TestOHDSIphenos_output.txt
# Run Test.sh on all 428 OHDSI Phenotypes in data/OHDSIPhenotypes.csv

### DEFINE PARAMETERS
helpFunction()
{
   echo ""
   echo "Usage: $0 -s DB_SCHEMA -h OHDSI_PHENO_PATH -f AOU_FOLDER_PATH -c CONFIG_FILE_PATH"
   echo -e "\t-h Path to csv with desired OHDSI phenos"
   echo -e "\t-s Database schema (of postgresdb)"
   echo -e "\t-f Path to aou-atlas-phenotyping folder"
   echo -e "\t-c Database connection config file"
   exit 1 # Exit script after printing help
}

while getopts "s:f:c:h:" opt
do
   case "$opt" in
      h ) OHDSI_PHENO_PATH="$OPTARG" ;;
      s ) DB_SCHEMA="$OPTARG" ;;
      f ) AOU_FOLDER_PATH="$OPTARG" ;;
      c ) CONFIG_FILE_PATH="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$CONFIG_FILE_PATH" ] || [ -z "$DB_SCHEMA" ] || [ -z "$AOU_FOLDER_PATH" ] || [ -z "$OHDSI_PHENO_PATH" ]
then
   echo "Some or all of the required parameters are empty";
   helpFunction
fi
### DEFINE PARAMETERS

# read in cohortIds and save template sql files
column_values=()
IFS=$','
line_number=0
while read -r -a row; do
    ((line_number++))
    # Skip the header row
    if [ "$line_number" -eq 1 ]; then
        continue
    fi
    # Get the value from the desired column
    value="${row[1]}"
    # Add the value to the array
    column_values+=("$value")
done < "$OHDSI_PHENO_PATH"

mkdir -p $AOU_FOLDER_PATH/test/template_sql_files
mkdir -p $AOU_FOLDER_PATH/test/test_runs
mkdir -p $AOU_FOLDER_PATH/test/postgres_py_files
mkdir -p $AOU_FOLDER_PATH/test/bigquery_py_files

# write scripts path to array
scripts=()

for cohortId in "${column_values[@]}"; do
        cat << EOT >> $AOU_FOLDER_PATH/test/test_runs/${cohortId}.sh
#! /usr/bin/bash
#SBATCH --job-name=${cohortId}
#SBATCH --partition=pe2
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=120:00:00
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --output=$AOU_FOLDER_PATH/test/test_runs/${cohortId}_output.txt
#SBATCH --error=$AOU_FOLDER_PATH/test/test_runs/${cohortId}_errors.txt

set -e

mkdir -p /nfs/scratch/anewbury/${cohortId}
export TMPDIR=/nfs/scratch/anewbury/${cohortId}
export TMP=/nfs/scratch/anewbury/${cohortId}

# download sql file OHDSI Phenotype Library Release v3.1.6
/usr/bin/wget -O "$AOU_FOLDER_PATH/test/template_sql_files/${cohortId}.sql" "https://raw.githubusercontent.com/OHDSI/PhenotypeLibrary/ac17b7af55b01ec91eb2ac1ca1ea30473f8ba621/inst/sql/${cohortId}.sql"

# run Test.sh 
$AOU_FOLDER_PATH/test/Test.sh -p $AOU_FOLDER_PATH/test/template_sql_files/${cohortId}.sql -s $DB_SCHEMA -o $AOU_FOLDER_PATH/test/postgres_py_files/$cohortId.py -v $AOU_FOLDER_PATH/test/bigquery_py_files/$cohortId.py -f $AOU_FOLDER_PATH -c $CONFIG_FILE_PATH

exit
EOT
        chmod +x $AOU_FOLDER_PATH/test/test_runs/${cohortId}.sh
        scripts+=($AOU_FOLDER_PATH/test/test_runs/${cohortId}.sh)
done

# pass array to parallel (run up to 7 scripts in parallel due to tmp folder space)
parallel -j 7 ::: "${scripts[@]}"

# # delete downloaded template sql files and postgres py files and test run path folder (all we need is output bigquery ohdsi files if test successful)
# rm -rf $AOU_FOLDER_PATH/test/template_sql_files
# rm -rf $AOU_FOLDER_PATH/test/postgres_py_files
# rm -rf $AOU_FOLDER_PATH/test/test_runs
