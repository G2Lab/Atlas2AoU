
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
  select concept_id from ukbb.CONCEPT where concept_id in (201254,435216,40484648)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (201254,435216,40484648)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (4297650,4334806,46273442,2107572,2107561,4058299,4107913,2108721,4102493,80809,2107560,2107558,2107559,4083556,4035611)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4297650,4334806,46273442,2107572,2107561,4058299,4107913,2108721,4102493,80809,2107560,2107558,2107559,4083556,4035611)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (75614,140168)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (75614,140168)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (81931)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (81931)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 5 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (374919)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (374919)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 6 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (46273369,4145240,257628)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (46273369,4145240,257628)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 7 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (443394)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (443394)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 8 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (4232076)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4232076)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 9 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (254443)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (254443)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 10 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (135215)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (135215)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (76685,432893)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (76685,432893)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 12 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (4137275)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4137275)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 13 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (432295)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (432295)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 14 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (194992)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (194992)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 15 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (46272236,4331739,441928,255304,4105026,46270482,134442,4063582)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (46272236,4331739,441928,255304,4105026,46270482,134442,4063582)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 16 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (438688,4262578,45772123,4105005)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (438688,4262578,45772123,4105005)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 17 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (46273477,81893,46273478,46274082,46269952,4116142)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (46273477,81893,46273478,46274082,46269952,4116142)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 18 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (201606,46269889,46269999)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (201606,46269889,46269999)
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
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, (C.observation_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.observation_date as sort_date
from 
(
  select o.* 
  FROM ukbb.OBSERVATION o
JOIN Codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
-- End Observation Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 3)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 4)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 6)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 8)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 9)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 10)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 11)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 12)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 13)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 14)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 15)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 16)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 17)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, (C.observation_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.observation_date as sort_date
from 
(
  select o.* 
  FROM ukbb.OBSERVATION o
JOIN Codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 17)
) C
-- End Observation Criteria
UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 18)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, (C.observation_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.observation_date as sort_date
from 
(
  select o.* 
  FROM ukbb.OBSERVATION o
JOIN Codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 18)
) C
-- End Observation Criteria
  ) E
	JOIN observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE (OP.OBSERVATION_PERIOD_START_DATE + 0*INTERVAL'1 day') <= E.START_DATE AND (E.START_DATE + 0*INTERVAL'1 day') <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
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
WHERE Results.ordinal = 1
;
ANALYZE included_events
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
-- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from included_events
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
      , (event_date + -1 * 0*INTERVAL'1 day')  as end_date
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
          , (end_date + 0*INTERVAL'1 day') as end_date
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
        
