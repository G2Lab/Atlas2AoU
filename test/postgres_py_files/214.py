
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
  select concept_id from ukbb.CONCEPT where concept_id in (36715585,320073,42872951,435224,440689,45766061,4119158)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (36715585,320073,42872951,440689,45766061,4119158)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (193448,4289321,35622701,434895)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (193448,4289321,35622701,434895)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (37393856,4148615,3017732,3013650,3017501)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3017732,3013650,3017501)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (4306106)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4306106)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 14 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (432881,4131124)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (432881,4131124)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 15 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (2617149,36716029,2617148,4029669,4120449,35624756,4028718,37017165,4144746,2617150,44831063,4105643,37398911,438869,4183718,37395652,4125630,4217370,4267432,36680584,37018722,4295183,4028717,44783626,4159748,4051681,37017132,4029670,4006467,40478891,36715584)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2617149,36716029,2617148,4029669,4120449,35624756,4028718,37017165,4144746,2617150,4105643,37398911,438869,4183718,37395652,4125630,4217370,4267432,36680584,37018722,4295183,4028717,44783626,4159748,4051681,37017132,4029670,4006467,40478891,36715584)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 16 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (3000963,3027484)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3000963,3027484)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 17 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (3009542,3034976,3028813,3019909,3050746,42869584,3023314,42869588,4016241,4151358,4097621,2212641,2212642)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3009542,3034976,3028813,3019909,3050746,42869584,3023314,42869588,4016241,4151358,4097621,2212641,2212642)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 18 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (432870,37397537,42872952)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (432870,37397537,42872952)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 19 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (4267147,3031586,3050583,3007461,37393863)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4267147,3031586,3050583,3007461,37393863)
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
-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
WHERE (C.value_as_number >= 0.0100 and C.value_as_number <= 1.4990)
AND C.unit_concept_id in (9444,8848,8816,8961,44777588)
-- End Measurement Criteria
UNION ALL
-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
WHERE (C.value_as_number >= 10.0000 and C.value_as_number <= 1499.0000)
AND C.unit_concept_id in (8784,8647)
-- End Measurement Criteria
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
CREATE TEMP TABLE Inclusion_0
AS
SELECT
0 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, (C.condition_start_date + 1*INTERVAL'1 day')) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM ukbb.CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) C
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id  AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_0
;
CREATE TEMP TABLE Inclusion_1
AS
SELECT
1 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
WHERE (C.value_as_number >= 4.0000 and C.value_as_number <= 8.2500)
AND C.unit_concept_id in (9444,8848,8816,8961,44777588)
-- End Measurement Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + 0*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 0*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
WHERE (C.value_as_number >= 4000.0000 and C.value_as_number <= 8250.0000)
AND C.unit_concept_id in (8784,8647)
-- End Measurement Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + 0*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 0*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_1
;
CREATE TEMP TABLE Inclusion_2
AS
SELECT
2 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
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
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + 0*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 0*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_2
;
CREATE TEMP TABLE Inclusion_3
AS
SELECT
3 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
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
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_3
;
CREATE TEMP TABLE Inclusion_4
AS
SELECT
4 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
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
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, (C.observation_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.observation_date as sort_date
from 
(
  select o.* 
  FROM ukbb.OBSERVATION o
JOIN Codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 15)
) C
-- End Observation Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_4
;
CREATE TEMP TABLE Inclusion_5
AS
SELECT
5 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 16)
) C
WHERE (C.value_as_number >= 4.0000 and C.value_as_number <= 11.0000)
AND C.unit_concept_id in (4121395,8713,8950)
-- End Measurement Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_5
;
CREATE TEMP TABLE Inclusion_6
AS
SELECT
6 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 17)
) C
WHERE (C.value_as_number >= 5.0000 and C.value_as_number <= 30.0000)
AND C.unit_concept_id in (8554)
-- End Measurement Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_6
;
CREATE TEMP TABLE Inclusion_7
AS
SELECT
7 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
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
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
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
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_7
;
CREATE TEMP TABLE Inclusion_8
AS
SELECT
8 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM qualified_events P
JOIN (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 19)
) C
WHERE (C.value_as_number >= 10.0000 and C.value_as_number <= 149.0000)
AND C.unit_concept_id in (8848,8961,9444,8816)
-- End Measurement Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -7*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + 7*INTERVAL'1 day') ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
ANALYZE Inclusion_8
;
CREATE TEMP TABLE inclusion_events
AS
SELECT
inclusion_rule_id, person_id, event_id
FROM
(select inclusion_rule_id, person_id, event_id from Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_1
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_2
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_3
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_4
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_5
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_6
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_7
UNION ALL
select inclusion_rule_id, person_id, event_id from Inclusion_8) I;
ANALYZE inclusion_events
;
TRUNCATE TABLE Inclusion_0;
DROP TABLE Inclusion_0;
TRUNCATE TABLE Inclusion_1;
DROP TABLE Inclusion_1;
TRUNCATE TABLE Inclusion_2;
DROP TABLE Inclusion_2;
TRUNCATE TABLE Inclusion_3;
DROP TABLE Inclusion_3;
TRUNCATE TABLE Inclusion_4;
DROP TABLE Inclusion_4;
TRUNCATE TABLE Inclusion_5;
DROP TABLE Inclusion_5;
TRUNCATE TABLE Inclusion_6;
DROP TABLE Inclusion_6;
TRUNCATE TABLE Inclusion_7;
DROP TABLE Inclusion_7;
TRUNCATE TABLE Inclusion_8;
DROP TABLE Inclusion_8;
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
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),9)-1)
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
UNION ALL
-- Censor Events
select i.event_id, i.person_id, MIN(c.start_date) as end_date
FROM included_events i
JOIN
(
-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
WHERE (C.value_as_number >= 4.0000 and C.value_as_number <= 8.2500)
AND C.unit_concept_id in (9444,8848,8816,8961,44777588)
-- End Measurement Criteria
) C on C.person_id = I.person_id and C.start_date >= I.start_date and C.START_DATE <= I.op_end_date
GROUP BY i.event_id, i.person_id
UNION ALL
select i.event_id, i.person_id, MIN(c.start_date) as end_date
FROM included_events i
JOIN
(
-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, (C.measurement_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM ukbb.MEASUREMENT m
JOIN Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
WHERE (C.value_as_number >= 4000.0000 and C.value_as_number <= 8250.0000)
AND C.unit_concept_id in (8784,8647)
-- End Measurement Criteria
) C on C.person_id = I.person_id and C.start_date >= I.start_date and C.START_DATE <= I.op_end_date
GROUP BY i.event_id, i.person_id
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
        