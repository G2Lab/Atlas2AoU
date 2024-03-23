
from sqlalchemy import create_engine
import configparser
import pandas as pd
def create_df():
                config = configparser.ConfigParser()
                config.read('/gpfs/commons/home/anewbury/config.ini')  # Provide the path to your config.ini file
                # Database configuration
                username = config.get('postgres', 'username')
                password = config.get('postgres', 'password')
                host = config.get('postgres', 'host')
                database = config.get('postgres', 'database')
                # Create the database engine using the configuration
                engine = create_engine(f'postgresql://{username}:{password}@{host}/{database}')
                sql = f"""
    CREATE TEMPORARY TABLE observation_period (
      observation_period_id INT not null,
      person_id INT not null,
      observation_period_start_date DATE not null,
      observation_period_end_date DATE not null,
      period_type_concept_id INT not null
    )
    ; insert into observation_period (observation_period_id, person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)
    --- look at min and max recorded dates for patient
    SELECT 1 AS observation_period_id, person_id, MIN(event_date) AS observation_period_start_date, MAX(event_date) AS observation_period_end_date, 1 AS  period_type_concept_id
    FROM
    (SELECT person_id,specimen_date AS event_date FROM ukbb.specimen
    UNION ALL
    SELECT person_id, death_date AS event_date FROM ukbb.death
    UNION ALL 
    SELECT person_id, visit_start_date AS event_date FROM ukbb.visit_occurrence
    UNION ALL 
    SELECT person_id, visit_end_date AS event_date FROM ukbb.visit_occurrence
    UNION ALL
    SELECT person_id, procedure_date AS event_date FROM ukbb.procedure_occurrence
    UNION ALL
    SELECT person_id, drug_exposure_start_date AS event_date FROM ukbb.drug_exposure
    UNION ALL 
    SELECT person_id, drug_exposure_end_date AS event_date FROM ukbb.drug_exposure
    UNION ALL 
    SELECT person_id, device_exposure_start_date AS event_date FROM ukbb.device_exposure
    UNION ALL 
    SELECT person_id, device_exposure_end_date AS event_date FROM ukbb.device_exposure
    UNION ALL
    SELECT person_id, condition_start_date AS event_date FROM ukbb.condition_occurrence
    UNION ALL 
    SELECT person_id, condition_end_date AS event_date FROM ukbb.condition_occurrence
    UNION ALL 
    SELECT person_id, measurement_date AS event_date FROM ukbb.measurement
    UNION ALL 
    SELECT person_id, observation_date AS event_date FROM ukbb.observation
    ) AS combined_events
    GROUP BY 
    person_id;
    
    CREATE TEMP TABLE Codesets  (codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;
INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (40481901)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40481901)
  and c.invalid_reason is null
) I
) C
;
CREATE TEMP TABLE qualified_events
AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
  ) E
	JOIN observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE (OP.OBSERVATION_PERIOD_START_DATE + 0*INTERVAL'1 day') <= E.START_DATE AND (E.START_DATE + 0*INTERVAL'1 day') <= OP.OBSERVATION_PERIOD_END_DATE
) P
-- End Primary Events
) pe
) QE
;
ANALYZE qualified_events
;
--- Inclusion Rule Inserts
CREATE TEMP TABLE inclusion_events  (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);
CREATE TEMP TABLE included_events
AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date
FROM
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
) Results
;
ANALYZE included_events
;
-- date offset strategy
CREATE TEMP TABLE strategy_ends
AS
SELECT
event_id, person_id, 
  case when (end_date + 0*INTERVAL'1 day') > op_end_date then op_end_date else (end_date + 0*INTERVAL'1 day') end as end_date
FROM
included_events;
ANALYZE strategy_ends
;
-- generate cohort periods into #final_cohort
CREATE TEMP TABLE cohort_rows
AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
	  from included_events I
	  join ( -- cohort_ends
-- cohort exit dates
-- End Date Strategy
SELECT event_id, person_id, end_date from strategy_ends
    ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
) FE;
ANALYZE cohort_rows
;
CREATE TEMP TABLE final_cohort
AS
SELECT
person_id, min(start_date) as start_date, end_date
FROM
( --cteEnds
	SELECT
		 c.person_id
		, c.start_date
		, MIN(ed.end_date) AS end_date
	FROM cohort_rows c
	JOIN ( -- cteEndDates
    SELECT
      person_id
      , (event_date + -1 * 1*INTERVAL'1 day')  as end_date
    FROM
    (
      SELECT
        person_id
        , event_date
        , event_type
        , SUM(event_type) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS interval_status
      FROM
      (
        SELECT
          person_id
          , start_date AS event_date
          , -1 AS event_type
        FROM cohort_rows
        UNION ALL
        SELECT
          person_id
          , (end_date + 1*INTERVAL'1 day') as end_date
          , 1 AS event_type
        FROM cohort_rows
      ) RAWDATA
    ) e
    WHERE interval_status = 0
  ) ed ON c.person_id = ed.person_id AND ed.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
) e
group by person_id, end_date
;
ANALYZE final_cohort
;
SELECT * FROM final_cohort;"""
                ohdsi_query_df = pd.read_sql(sql, con=engine)
                return ohdsi_query_df
        
