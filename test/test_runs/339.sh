#! /usr/bin/bash
#SBATCH --job-name=339
#SBATCH --partition=pe2
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=120:00:00
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=anewbury@nygenome.org
#SBATCH --output=/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/test_runs/339_output.txt
#SBATCH --error=/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/test_runs/339_errors.txt

set -e

mkdir -p /nfs/scratch/anewbury/339
export TMPDIR=/nfs/scratch/anewbury/339
export TMP=/nfs/scratch/anewbury/339

# download sql file OHDSI Phenotype Library Release v3.1.6
/usr/bin/wget -O "/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/template_sql_files/339.sql" "https://raw.githubusercontent.com/OHDSI/PhenotypeLibrary/ac17b7af55b01ec91eb2ac1ca1ea30473f8ba621/inst/sql/339.sql"

# run Test.sh 
/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/Test.sh -p /gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/template_sql_files/339.sql -s ukbb -o /gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/postgres_py_files/339.py -v /gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping/test/bigquery_py_files/339.py -f /gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping -c /gpfs/commons/home/anewbury/config.ini

exit
