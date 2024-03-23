import re


def create_obs_period(DB_SCHEMA,DB_TYPE):
    if DB_TYPE == 'bigquery':
        int_name = 'INT64'
    else:
        int_name = 'INT'
    sql = f"""
    CREATE TEMPORARY TABLE observation_period (
      observation_period_id {int_name} not null,
      person_id {int_name} not null,
      observation_period_start_date DATE not null,
      observation_period_end_date DATE not null,
      period_type_concept_id {int_name} not null
    )
    ; insert into observation_period (observation_period_id, person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)
    --- look at min and max recorded dates for patient
    SELECT 1 AS observation_period_id, person_id, MIN(event_date) AS observation_period_start_date, MAX(event_date) AS observation_period_end_date, 1 AS  period_type_concept_id
    FROM
    (SELECT person_id,specimen_date AS event_date FROM {DB_SCHEMA}.specimen
    UNION ALL
    SELECT person_id, death_date AS event_date FROM {DB_SCHEMA}.death
    UNION ALL 
    SELECT person_id, visit_start_date AS event_date FROM {DB_SCHEMA}.visit_occurrence
    UNION ALL 
    SELECT person_id, visit_end_date AS event_date FROM {DB_SCHEMA}.visit_occurrence
    UNION ALL
    SELECT person_id, procedure_date AS event_date FROM {DB_SCHEMA}.procedure_occurrence
    UNION ALL
    SELECT person_id, drug_exposure_start_date AS event_date FROM {DB_SCHEMA}.drug_exposure
    UNION ALL 
    SELECT person_id, drug_exposure_end_date AS event_date FROM {DB_SCHEMA}.drug_exposure
    UNION ALL 
    SELECT person_id, device_exposure_start_date AS event_date FROM {DB_SCHEMA}.device_exposure
    UNION ALL 
    SELECT person_id, device_exposure_end_date AS event_date FROM {DB_SCHEMA}.device_exposure
    UNION ALL
    SELECT person_id, condition_start_date AS event_date FROM {DB_SCHEMA}.condition_occurrence
    UNION ALL 
    SELECT person_id, condition_end_date AS event_date FROM {DB_SCHEMA}.condition_occurrence
    UNION ALL 
    SELECT person_id, measurement_date AS event_date FROM {DB_SCHEMA}.measurement
    UNION ALL 
    SELECT person_id, observation_date AS event_date FROM {DB_SCHEMA}.observation
    ) AS combined_events
    GROUP BY 
    person_id;
    
    """
    return sql

def alter_sql(sql,obs_period_query,DB_TYPE,DB_SCHEMA):
    if DB_TYPE == 'bigquery':
        # Atlas prepends random letters/numbers to temp table names for bigquery - need to remove
        pattern = r'create table (\w+)codesets\s*\('
        match = re.search(pattern, sql)
        sql = sql.replace(match.group(1),'')

        # change from CREATE TABLE to CREATE TEMPORARY TABLE - for postgresql SqlRender already outputs in the form CREATE TEMP TABLE
        sql = sql.replace(f'CREATE TABLE','CREATE TEMPORARY TABLE')
        sql = sql.replace(f'create table','CREATE TEMPORARY TABLE')

    # Delete end - case insensitive - looks like this since set target target_cohort_table="",target_database_schema="", and target_cohort_id = 0 in sql render
    sql = re.split(re.escape('delete from . where cohort_definition_id = 0;'), sql, maxsplit=1, flags=re.IGNORECASE)[0]

    # Use temp observation period table instead
    sql = sql.replace(f'{DB_SCHEMA}.observation_period','observation_period')

    # add select * from final cohort 
    sql = sql + 'SELECT * FROM final_cohort;'

    # add temp observation_period table query
    sql = obs_period_query + sql

    # add begin and end for bigquery
    if DB_TYPE == 'bigquery':
        sql = 'BEGIN \n' + sql
        sql = sql + 'END;'
    return sql
    

