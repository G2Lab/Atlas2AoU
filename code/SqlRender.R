# Using OHDSI SqlRender package to change sql dialect from OHDSI.template to desired 
conda_env <- Sys.getenv("CONDA_PREFIX")
library(rJava)
library(SqlRender)
# input: (1) the path of the OHDSI.template sql (2) the database schema for OMOP tables (for AoU this is {os.environ["WORKSPACE_CDR"]}) and (3) the sql dialect
# cohort_id, target_cohort_table, and target_database_schema are not relevant since cohort will not be written to sql db table
args <- commandArgs(trailingOnly=TRUE)
sql_path <- as.character(args[1])
db_schema <- as.character(args[2])
sql_dialect <- as.character(args[3])
sql <- readChar(sql_path,file.info(sql_path)$size)
sql <- render(sql,cdm_database_schema=db_schema,results_database_schema=db_schema,target_cohort_id=0,target_cohort_table="",target_database_schema="",vocabulary_database_schema=db_schema)
sql <- translate(sql,targetDialect=sql_dialect)
# write to tmp folder
#writeLines(sql,sql_file)
cat(sql, "\n")
