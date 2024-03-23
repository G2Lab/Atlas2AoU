
# Convert Atlas query to be AoU compatible

### LOAD ENVIRONMENT
module load R/4.2.3
module load java/20.0.1
source ~/miniconda/bin/activate ~/miniconda/envs/jupyter/
### LOAD ENVIRONMENT

### DEFINE PARAMETERS
helpFunction()
{
   echo ""
   echo "Usage: $0 -p SQL_PATH -s DB_SCHEMA -t DB_TYPE -o OUT -f AOU_FOLDER_PATH -c CONFIG_FILE_PATH"
   echo -e "\t-p Path to sql file produced by Atlas in OHDSI.template format"
   echo -e "\t-s Database schema (for All of Us this is {os.environ["WORKSPACE_CDR"]}"
   echo -e "\t-t Type of database (for All of Us this is bigquery)"
   echo -e "\t-o Path for output .py script"
   echo -e "\t-f Path to aou-atlas-phenotyping folder"
   echo -e "\t-c Database connection config file (optional - not used for AoU)"
   exit 1 # Exit script after printing help
}

while getopts "p:s:t:o:f:c:" opt
do
   case "$opt" in
      p ) SQL_PATH="$OPTARG" ;;
      s ) DB_SCHEMA="$OPTARG" ;;
      t ) DB_TYPE="$OPTARG" ;;
      o ) OUT="$OPTARG" ;;
      f ) AOU_FOLDER_PATH="$OPTARG" ;;
      c ) CONFIG_FILE_PATH="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$SQL_PATH" ] || [ -z "$DB_SCHEMA" ] || [ -z "$DB_TYPE" ] || [ -z "$OUT" ] || [ -z "$AOU_FOLDER_PATH" ]
then
   echo "Some or all of the required parameters are empty";
   helpFunction
fi
### DEFINE PARAMETERS

cd $AOU_FOLDER_PATH
# render sql (translate to dialect and insert db schemas) -> construct query
Rscript code/SqlRender.R $SQL_PATH $DB_SCHEMA $DB_TYPE | python3 code/ConstructQuery.py -s $DB_SCHEMA -t $DB_TYPE -o $OUT -c $CONFIG_FILE_PATH