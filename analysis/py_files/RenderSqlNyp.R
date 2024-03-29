# render sql for nyp prev/coprev analysis
conda_env <- Sys.getenv("CONDA_PREFIX")
library(rJava)
library(SqlRender)
library(dplyr)
library(stringr)

AOU_FOLDER_PATH <- "/gpfs/commons/groups/gursoy_lab/anewbury/aou-atlas-phenotyping"
setwd(AOU_FOLDER_PATH)
if (!file.exists('analysis/sqlserver_sql_files')) {
  dir.create('analysis/sqlserver_sql_files')
}

# get cohort ids to run
ohdsi_phenos <- read.csv("data/OHDSIPhenotypes.csv")
# only use those that are accepted (not prepended with [ like [P] or [W])
ohdsi_phenos <- ohdsi_phenos %>% filter(!grepl("^\\[", cohortName))

for (cohortId in unique(ohdsi_phenos$cohortId)) {
# get sql templates
url <- paste0("https://raw.githubusercontent.com/OHDSI/PhenotypeLibrary/ac17b7af55b01ec91eb2ac1ca1ea30473f8ba621/inst/sql/",cohortId,".sql")
output_dir <- "analysis/sqlserver_sql_files"
command <- paste("wget", "-P", output_dir, url)
system(command)


# Using OHDSI SqlRender package to change sql dialect from OHDSI.template to desired 
cohort_definition_id = paste0(cohortId,"000")
sql_path <- paste0('analysis/sqlserver_sql_files/',cohortId,'.sql')
sql_dialect <- "sql server"
sql <- readChar(sql_path,file.info(sql_path)$size)
sql <- render(sql,cdm_database_schema="ohdsi_cumc_deid_2023q4r1.dbo",results_database_schema="ohdsi_cumc_deid_2023q4r1.results",target_cohort_id=cohort_definition_id,target_cohort_table="cohort",target_database_schema="ohdsi_cumc_deid_2023q4r1.results",vocabulary_database_schema="ohdsi_cumc_deid_2023q4r1.dbo")
sql <- translate(sql,targetDialect=sql_dialect)
# write to tmp folder
writeLines(sql,con=sql_path)
}