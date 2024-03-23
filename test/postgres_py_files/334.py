
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
  select concept_id from ukbb.CONCEPT where concept_id in (317009,4235703,4279553)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (317009,4235703,4279553)
  and c.invalid_reason is null
) I
) C UNION ALL 
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from ukbb.CONCEPT where concept_id in (42483138,36812530,40727741,1356123,40142665,46234463,1356111,1356108,40142703,21603292,41143395,36421291,40745353,36787954,21158944,36894458,21090035,36883710,35130061,40142784,42479684,35133500,783228,40142910,36787269,1356143,35135829,1356140,44120754,41205832,40924271,40986621,40830666,41205663,40861866,43274335,43263324,43621601,1356154,1356147,40143105,41080205,41174344,41111516,1356173,1356180,40144087,43134418,40727834,36811735,40861768,40727839,35150375,44081619,1356217,1356215,1356101,42800913,40143326,36894464,44817882,40143337,42941603,35146684,35160199,44029688,42481922,36813480,36812414,1356244,40143708,21150787,44082127,41206083,1356138,1356136,40152662,40156382,41048760,40754973,21089505,40144020,36882733,42482744,40144024,37592046,43532281,43291091,41267401,40144035,35147990,35149212,40144037,40223712,42629522,42480849,1356191,1356187,44107471,43145868,43259954,1356196,35145836,42479568,1356211,44055847,44081467,1356120,40182262,40746100,21174574,45775117,41205378,40152687,40167702,35158799,783089,1356189,35152712)
UNION  select c.concept_id
  from ukbb.CONCEPT c
  join ukbb.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (42483138,36812530,40727741,1356123,40142665,46234463,1356111,1356108,40142703,21603292,41143395,36421291,40745353,36787954,21158944,36894458,21090035,36883710,35130061,40142784,42479684,35133500,783228,40142910,36787269,1356143,35135829,1356140,44120754,41205832,40924271,40986621,40830666,41205663,40861866,43274335,43263324,43621601,1356154,1356147,40143105,41080205,41174344,41111516,1356173,1356180,40144087,43134418,40727834,36811735,40861768,40727839,35150375,44081619,1356217,1356215,1356101,42800913,40143326,36894464,44817882,40143337,42941603,35146684,35160199,44029688,42481922,36813480,36812414,1356244,40143708,21150787,44082127,41206083,1356138,1356136,40152662,40156382,41048760,40754973,21089505,40144020,36882733,42482744,40144024,37592046,43532281,43291091,41267401,40144035,35147990,35149212,40144037,40223712,42629522,42480849,1356191,1356187,44107471,43145868,43259954,1356196,35145836,42479568,1356211,44055847,44081467,1356120,40182262,40746100,21174574,45775117,41205378,40152687,40167702,35158799,783089,1356189,35152712)
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
  select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.visit_occurrence_id, PE.sort_date FROM (
-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.DRUG_EXPOSURE_END_DATE, (DRUG_EXPOSURE_START_DATE + C.DAYS_SUPPLY*INTERVAL'1 day'), (C.DRUG_EXPOSURE_START_DATE + 1*INTERVAL'1 day')) as end_date,
       C.visit_occurrence_id,C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM ukbb.DRUG_EXPOSURE de
JOIN Codesets cs on (de.drug_concept_id = cs.concept_id and cs.codeset_id = 4)
) C
JOIN ukbb.PERSON P on C.person_id = P.person_id
WHERE EXTRACT(YEAR FROM C.drug_exposure_start_date) - P.year_of_birth < 55
-- End Drug Exposure Criteria
) PE
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.DRUG_EXPOSURE_END_DATE, (DRUG_EXPOSURE_START_DATE + C.DAYS_SUPPLY*INTERVAL'1 day'), (C.DRUG_EXPOSURE_START_DATE + 1*INTERVAL'1 day')) as end_date,
       C.visit_occurrence_id,C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM ukbb.DRUG_EXPOSURE de
JOIN Codesets cs on (de.drug_concept_id = cs.concept_id and cs.codeset_id = 4)
) C
JOIN ukbb.PERSON P on C.person_id = P.person_id
WHERE EXTRACT(YEAR FROM C.drug_exposure_start_date) - P.year_of_birth < 55
-- End Drug Exposure Criteria
) Q
JOIN ukbb.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id 
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.DRUG_EXPOSURE_END_DATE, (DRUG_EXPOSURE_START_DATE + C.DAYS_SUPPLY*INTERVAL'1 day'), (C.DRUG_EXPOSURE_START_DATE + 1*INTERVAL'1 day')) as end_date,
       C.visit_occurrence_id,C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM ukbb.DRUG_EXPOSURE de
JOIN Codesets cs on (de.drug_concept_id = cs.concept_id and cs.codeset_id = 4)
) C
JOIN ukbb.PERSON P on C.person_id = P.person_id
WHERE EXTRACT(YEAR FROM C.drug_exposure_start_date) - P.year_of_birth < 55
-- End Drug Exposure Criteria
) Q
JOIN ukbb.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id 
  and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
JOIN (
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.DRUG_EXPOSURE_END_DATE, (DRUG_EXPOSURE_START_DATE + C.DAYS_SUPPLY*INTERVAL'1 day'), (C.DRUG_EXPOSURE_START_DATE + 1*INTERVAL'1 day')) as end_date,
       C.visit_occurrence_id,C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM ukbb.DRUG_EXPOSURE de
JOIN Codesets cs on (de.drug_concept_id = cs.concept_id and cs.codeset_id = 4)
) C
-- End Drug Exposure Criteria
) A on A.person_id = P.person_id  AND A.START_DATE >= (P.START_DATE + -365*INTERVAL'1 day') AND A.START_DATE <= (P.START_DATE + -180*INTERVAL'1 day') ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id
UNION ALL
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
-- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, (C.observation_date + 1*INTERVAL'1 day') as END_DATE,
       C.visit_occurrence_id, C.observation_date as sort_date
from 
(
  select o.* 
  FROM ukbb.OBSERVATION o
JOIN Codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Observation Criteria
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
        
