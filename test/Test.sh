# takes as input an Atlas sql file and tests if it yields the same cohort (only focused on individuals in cohort more than one time, not on start and end date)
# using Atlas2AoU code to run .py file vs. running it on real database. This code assumes a postgres db.
# If file run is successful, writes file in bigquery format to output/ folder to be used in AoU. Otherwise records sql path in unsuccessful_comparison.txt folder

### LOAD ENVIRONMENT
module load R/4.2.3
module load java/20.0.1
source ~/miniconda/bin/activate ~/miniconda/envs/jupyter/
### LOAD ENVIRONMENT

### DEFINE PARAMETERS
helpFunction()
{
   echo ""
   echo "Usage: $0 -p SQL_PATH -s DB_SCHEMA -o OUT -f AOU_FOLDER_PATH -c CONFIG_FILE_PATH -v SUCCESSFUL_COMP_FILE_PATH"
   echo -e "\t-p Path to sql file produced by Atlas in OHDSI.template format"
   echo -e "\t-s Database schema (for All of Us this is {os.environ["WORKSPACE_CDR"]}"
   echo -e "\t-o Path for output .py script from Atlas2Aou (for running test)"
   echo -e "\t-v Path for output .py script from Atlas2Aou (if comparison successful)"
   echo -e "\t-f Path to aou-atlas-phenotyping folder"
   echo -e "\t-c Database connection config file (optional - not used for AoU)"
   exit 1 # Exit script after printing help
}

while getopts "p:s:o:f:c:v:" opt
do
   case "$opt" in
      p ) SQL_PATH="$OPTARG" ;;
      s ) DB_SCHEMA="$OPTARG" ;;
      o ) OUT="$OPTARG" ;;
      f ) AOU_FOLDER_PATH="$OPTARG" ;;
      c ) CONFIG_FILE_PATH="$OPTARG" ;;
      v ) SUCCESSFUL_COMP_FILE_PATH="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

# Print helpFunction in case parameters are empty
if [ -z "$SQL_PATH" ] || [ -z "$DB_SCHEMA" ] || [ -z "$OUT" ] || [ -z "$AOU_FOLDER_PATH" ] || [ -z "$SUCCESSFUL_COMP_FILE_PATH" ]
then
   echo "Some or all of the required parameters are empty";
   helpFunction
fi
DB_TYPE="postgresql"
### DEFINE PARAMETERS

# Run Atlas2Aou
echo "running Atlas2Aou"
JOBID=$($AOU_FOLDER_PATH/code/Atlas2Aou.sh -p $SQL_PATH -s $DB_SCHEMA -t $DB_TYPE -o $OUT -f $AOU_FOLDER_PATH -c $CONFIG_FILE_PATH | awk '{print $4}')

# Wait for job to finish 
while squeue | grep -q $JOBID; do
    sleep 5 # Wait for 5 seconds before checking again
done

# Run sql render to get original rendered script then compare outputs
echo "running R script"
result=$(Rscript $AOU_FOLDER_PATH/code/SqlRender.R $SQL_PATH $DB_SCHEMA $DB_TYPE | python3 $AOU_FOLDER_PATH/test/CompareOutputs.py -p $SQL_PATH -s $DB_SCHEMA -t $DB_TYPE -o $OUT -c $CONFIG_FILE_PATH)
# if comparison successful, add bigquery .py file in output folder in Github; otherwise, remove and write to unsuccessful_comparisons.txt in test/ folder
if [ "$result" = "True" ]; then
   Rscript $AOU_FOLDER_PATH/code/SqlRender.R $SQL_PATH '{os.environ["WORKSPACE_CDR"]}' bigquery | python3 $AOU_FOLDER_PATH/code/ConstructQuery.py -s '{os.environ["WORKSPACE_CDR"]}' -t bigquery -o $SUCCESSFUL_COMP_FILE_PATH -c $CONFIG_FILE_PATH
else
   echo -e "$SQL_PATH" >> $AOU_FOLDER_PATH/test/unsuccessful_comparisons.txt
fi



