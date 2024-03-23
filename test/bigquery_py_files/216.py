
import os
from google.cloud import bigquery
def create_df():
        sql = f"""BEGIN 

    CREATE TEMPORARY TABLE observation_period (
      observation_period_id INT64 not null,
      person_id INT64 not null,
      observation_period_start_date DATE not null,
      observation_period_end_date DATE not null,
      period_type_concept_id INT64 not null
    )
    ; insert into observation_period (observation_period_id, person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)
    --- look at min and max recorded dates for patient
    SELECT 1 AS observation_period_id, person_id, MIN(event_date) AS observation_period_start_date, MAX(event_date) AS observation_period_end_date, 1 AS  period_type_concept_id
    FROM
    (SELECT person_id,specimen_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.specimen
    UNION ALL
    SELECT person_id, death_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.death
    UNION ALL 
    SELECT person_id, visit_start_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.visit_occurrence
    UNION ALL 
    SELECT person_id, visit_end_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.visit_occurrence
    UNION ALL
    SELECT person_id, procedure_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.procedure_occurrence
    UNION ALL
    SELECT person_id, drug_exposure_start_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.drug_exposure
    UNION ALL 
    SELECT person_id, drug_exposure_end_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.drug_exposure
    UNION ALL 
    SELECT person_id, device_exposure_start_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.device_exposure
    UNION ALL 
    SELECT person_id, device_exposure_end_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.device_exposure
    UNION ALL
    SELECT person_id, condition_start_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.condition_occurrence
    UNION ALL 
    SELECT person_id, condition_end_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.condition_occurrence
    UNION ALL 
    SELECT person_id, measurement_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.measurement
    UNION ALL 
    SELECT person_id, observation_date AS event_date FROM {os.environ["WORKSPACE_CDR"]}.observation
    ) AS combined_events
    GROUP BY 
    person_id;
    
    CREATE TEMPORARY TABLE codesets (
  codeset_id INT64 not null,
  concept_id INT64 not null
)
;
insert into codesets (codeset_id, concept_id)
select 7 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4267147,3031586,3050583,3007461,37393863)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4267147,3031586,3050583,3007461,37393863)
  and c.invalid_reason is null
) i
) c union all 
select 9 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (37397537,4121131,4006469,42537688,437242)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (37397537,4121131,4006469,42537688,437242)
  and c.invalid_reason is null
) i
) c union all 
select 10 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4280071,36715584,45766614)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4280071,36715584,45766614)
  and c.invalid_reason is null
) i
) c union all 
select 12 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4098292)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4098292)
  and c.invalid_reason is null
) i
) c union all 
select 13 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (192298,200527,4064314,4272928)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (192298,200527,4064314,4272928)
  and c.invalid_reason is null
) i
) c union all 
select 14 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (75580,76685,80809,81893,140168,194992,201606,254443,257628,315763,374919,375260,761978,765565,3654567,3655583,3656106,4009501,4028118,4044056,4045732,4060405,4077943,4102503,4107913,4116142,4132809,4132810,4137275,4140462,4145240,4211939,4219859,4219879,4225691,4232076,4253901,4269448,4297650,4300204,4319305,4330635,4334806,37016279,37110681,37116301,37117740,42539357,44784474,45757061,45757069,45766237,46269952,46273369,46273442,46273477,46273478,46274082)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (75580,76685,80809,81893,140168,194992,201606,254443,257628,315763,374919,375260,761978,765565,3654567,3655583,3656106,4009501,4028118,4044056,4045732,4060405,4077943,4102503,4107913,4116142,4132809,4132810,4137275,4140462,4145240,4211939,4219859,4219879,4225691,4232076,4253901,4269448,4297650,4300204,4319305,4330635,4334806,37016279,37110681,37116301,37117740,42539357,44784474,45757061,45757069,45766237,46269952,46273369,46273442,46273477,46273478,46274082)
  and c.invalid_reason is null
) i
) c union all 
select 15 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4212540)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4212540)
  and c.invalid_reason is null
) i
) c union all 
select 16 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (435227,4181204,35622258,4101286)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (435227,4181204,35622258,4101286)
  and c.invalid_reason is null
) i
left join
(
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (434622,4280354,436070,436658,4146209)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (434622,4280354,436070,436658,4146209)
  and c.invalid_reason is null
) e on i.concept_id = e.concept_id
where e.concept_id is null
) c union all 
select 18 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4024671,40481388)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4024671,40481388)
  and c.invalid_reason is null
) i
) c union all 
select 19 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (197253,4159966,36674324)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (197253,4159966,36674324)
  and c.invalid_reason is null
) i
) c union all 
select 20 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (313800,4198102)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (313800,4198102)
  and c.invalid_reason is null
) i
) c union all 
select 21 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4316372)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4316372)
  and c.invalid_reason is null
) i
) c union all 
select 23 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4030264,4295651,4043444,4057594,37016161,4056992,4043445,4323972,36402787,432571,4161665,4041104,36402625,4156433,4241295,4219140,317510,4215820,4224628,4347562,4161138,4058706,37206728,37204530,45767656,4264693,4043713,4301780,4301659,4221907,437233,4131124,36402821,4161560,37110403,4161020,4164159)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4030264,4295651,4043444,4057594,37016161,4056992,4043445,4323972,36402787,432571,4161665,4041104,36402625,4156433,4241295,4219140,317510,4215820,4224628,4347562,4161138,4058706,37206728,37204530,45767656,4264693,4043713,4301780,4301659,4221907,437233,4131124,36402821,4161560,37110403,4161020,4164159)
  and c.invalid_reason is null
) i
) c union all 
select 24 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (432881,4131124)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (432881,4131124)
  and c.invalid_reason is null
) i
) c union all 
select 25 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (36715585,320073,42872951,435224,440689,45766061)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (36715585,320073,42872951,435224,440689,45766061)
  and c.invalid_reason is null
) i
) c union all 
select 26 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (37393856,4148615,3017732,3013650,3017501)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3017732,3013650,3017501)
  and c.invalid_reason is null
) i
) c union all 
select 27 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (2617149,36716029,2617148,4029669,4120449,35624756,4028718,37017165,4144746,2617150,44831063,4105643,37398911,438869,4183718,37395652,4125630,4217370,4267432,36680584,37018722,4295183,4028717,44783626,4159748,4051681,37017132,4029670,4006467,40478891,36715584)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (2617149,36716029,2617148,4029669,4120449,35624756,4028718,37017165,4144746,2617150,4105643,37398911,438869,4183718,37395652,4125630,4217370,4267432,36680584,37018722,4295183,4028717,44783626,4159748,4051681,37017132,4029670,4006467,40478891,36715584)
  and c.invalid_reason is null
) i
) c union all 
select 28 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (3000963,3027484)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3000963,3027484)
  and c.invalid_reason is null
) i
) c union all 
select 29 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (3009542,3034976,3028813,3019909,3050746,42869584,3023314,42869588,4016241,4151358,4097621,2212641,2212642)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3009542,3034976,3028813,3019909,3050746,42869584,3023314,42869588,4016241,4151358,4097621,2212641,2212642)
  and c.invalid_reason is null
) i
) c union all 
select 30 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4103532,4119134,40479556)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4103532,4119134,40479556)
  and c.invalid_reason is null
) i
) c
;
CREATE TEMPORARY TABLE qualified_events
 AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date asc) as ordinal, cast(pe.visit_occurrence_id  as int64) as visit_occurrence_id
  from (-- Begin Primary Events
select p.ordinal as event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id  as int64) as visit_occurrence_id
from
(
  select e.person_id, e.start_date, e.end_date,
         row_number() over (partition by e.person_id order by e.sort_date asc, e.event_id) ordinal,
         op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date, cast(e.visit_occurrence_id  as int64) as visit_occurrence_id
  from 
  (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 30)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) pe
) qe
;
--- Inclusion Rule Inserts
CREATE TEMPORARY TABLE inclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 30)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -365 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -1 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_1
 AS
SELECT
1 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 9)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_2
 AS
SELECT
2 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
where (c.value_as_number >= 101.0000 and c.value_as_number <= 450.0000)
and c.unit_concept_id in (8848,8961,9444,8816,44777588)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_3
 AS
SELECT
3 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 10)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_4
 AS
SELECT
4 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 13)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_5
 AS
SELECT
5 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 12)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_6
 AS
SELECT
6 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 18)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_7
 AS
SELECT
7 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 19)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_8
 AS
SELECT
8 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 20)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_9
 AS
SELECT
9 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 14)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_10
 AS
SELECT
10 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 15)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_11
 AS
SELECT
11 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 21)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -90 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_12
 AS
SELECT
12 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 23)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_13
 AS
SELECT
13 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 16)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_14
 AS
SELECT
14 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 24)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_15
 AS
SELECT
15 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 25)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_16
 AS
SELECT
16 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 26)
) c
where (c.value_as_number >= 0.0100 and c.value_as_number <= 1.4990)
and c.unit_concept_id in (9444,8848,8816,8961,44777588)
and (c.range_low >= 1.5000 and c.range_low <= 4.0000)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 26)
) c
where (c.value_as_number >= 10.0000 and c.value_as_number <= 1500.0000)
and c.unit_concept_id in (8784,8647)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 2
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_17
 AS
SELECT
17 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 27)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 1 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 27)
) c
-- End Observation Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 2
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_18
 AS
SELECT
18 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 28)
) c
where (c.value_as_number >= 4.0000 and c.value_as_number <= 11.0000)
and c.unit_concept_id in (4121395,8713,8950)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_19
 AS
SELECT
19 as inclusion_rule_id, person_id, event_id
FROM
(
  select pe.person_id, pe.event_id
  from qualified_events pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from qualified_events e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, p.person_id, p.event_id
  from qualified_events p
left join (
select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 29)
) c
where c.unit_concept_id in (8554)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -7 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 7 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 1
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
) results
;
CREATE TEMPORARY TABLE inclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id
FROM
(select inclusion_rule_id, person_id, event_id from inclusion_0
union all
select inclusion_rule_id, person_id, event_id from inclusion_1
union all
select inclusion_rule_id, person_id, event_id from inclusion_2
union all
select inclusion_rule_id, person_id, event_id from inclusion_3
union all
select inclusion_rule_id, person_id, event_id from inclusion_4
union all
select inclusion_rule_id, person_id, event_id from inclusion_5
union all
select inclusion_rule_id, person_id, event_id from inclusion_6
union all
select inclusion_rule_id, person_id, event_id from inclusion_7
union all
select inclusion_rule_id, person_id, event_id from inclusion_8
union all
select inclusion_rule_id, person_id, event_id from inclusion_9
union all
select inclusion_rule_id, person_id, event_id from inclusion_10
union all
select inclusion_rule_id, person_id, event_id from inclusion_11
union all
select inclusion_rule_id, person_id, event_id from inclusion_12
union all
select inclusion_rule_id, person_id, event_id from inclusion_13
union all
select inclusion_rule_id, person_id, event_id from inclusion_14
union all
select inclusion_rule_id, person_id, event_id from inclusion_15
union all
select inclusion_rule_id, person_id, event_id from inclusion_16
union all
select inclusion_rule_id, person_id, event_id from inclusion_17
union all
select inclusion_rule_id, person_id, event_id from inclusion_18
union all
select inclusion_rule_id, person_id, event_id from inclusion_19) i;
DELETE FROM inclusion_0 WHERE True;
drop table inclusion_0;
DELETE FROM inclusion_1 WHERE True;
drop table inclusion_1;
DELETE FROM inclusion_2 WHERE True;
drop table inclusion_2;
DELETE FROM inclusion_3 WHERE True;
drop table inclusion_3;
DELETE FROM inclusion_4 WHERE True;
drop table inclusion_4;
DELETE FROM inclusion_5 WHERE True;
drop table inclusion_5;
DELETE FROM inclusion_6 WHERE True;
drop table inclusion_6;
DELETE FROM inclusion_7 WHERE True;
drop table inclusion_7;
DELETE FROM inclusion_8 WHERE True;
drop table inclusion_8;
DELETE FROM inclusion_9 WHERE True;
drop table inclusion_9;
DELETE FROM inclusion_10 WHERE True;
drop table inclusion_10;
DELETE FROM inclusion_11 WHERE True;
drop table inclusion_11;
DELETE FROM inclusion_12 WHERE True;
drop table inclusion_12;
DELETE FROM inclusion_13 WHERE True;
drop table inclusion_13;
DELETE FROM inclusion_14 WHERE True;
drop table inclusion_14;
DELETE FROM inclusion_15 WHERE True;
drop table inclusion_15;
DELETE FROM inclusion_16 WHERE True;
drop table inclusion_16;
DELETE FROM inclusion_17 WHERE True;
drop table inclusion_17;
DELETE FROM inclusion_18 WHERE True;
drop table inclusion_18;
DELETE FROM inclusion_19 WHERE True;
drop table inclusion_19;
CREATE TEMPORARY TABLE included_events
 AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date
FROM
(
  select event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date asc) as ordinal
  from
  (
     select q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(cast(power(cast(2  as int64), i.inclusion_rule_id) as int64), 0)) as inclusion_rule_mask
     from qualified_events q
    left join inclusion_events i on i.person_id = q.person_id and i.event_id = q.event_id
     group by  q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
   ) mg -- matching groups
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  where (mg.inclusion_rule_mask = power(cast(2  as int64),20)-1)
) results
;
-- date offset strategy
CREATE TEMPORARY TABLE strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(IF(SAFE_CAST(end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(end_date  AS STRING)),SAFE_CAST(end_date  AS DATE)), INTERVAL 0 DAY) > op_end_date then op_end_date else DATE_ADD(IF(SAFE_CAST(end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(end_date  AS STRING)),SAFE_CAST(end_date  AS DATE)), INTERVAL 0 DAY) end as end_date
FROM
included_events;
-- generate cohort periods into #final_cohort
CREATE TEMPORARY TABLE cohort_rows
 AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
	select f.person_id, f.start_date, f.end_date
	from (
	  select i.event_id, i.person_id, i.start_date, ce.end_date, row_number() over (partition by i.person_id, i.event_id order by ce.end_date) as ordinal
	  from included_events i
	  join ( -- cohort_ends
-- cohort exit dates
-- End Date Strategy
 select event_id, person_id, end_date  from strategy_ends
union all
-- Censor Events
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from included_events i
join
(
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
where (c.value_as_number >= 150.0000 and c.value_as_number <= 450.0000)
and c.unit_concept_id in (8848,8961,9444,8816,44777588)
-- End Measurement Criteria
) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
  group by  i.event_id, i.person_id
union all
 select i.event_id, i.person_id, min(c.start_date) as end_date
 from included_events i
join
(
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 10)
) c
-- End Condition Occurrence Criteria
) c on c.person_id = i.person_id and c.start_date >= i.start_date and c.start_date <= i.op_end_date
 group by  i.event_id, i.person_id
       ) ce on i.event_id = ce.event_id and i.person_id = ce.person_id and ce.end_date >= i.start_date
	) f
	where f.ordinal = 1
) fe;
 CREATE TEMPORARY TABLE final_cohort
  AS
SELECT
person_id, min(start_date) as start_date, end_date
FROM
( --cteEnds
	 select c.person_id
		, c.start_date
		, min(ed.end_date) as end_date
	 from cohort_rows c
	join ( -- cteEndDates
    select
      person_id
      , DATE_ADD(IF(SAFE_CAST(event_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(event_date  AS STRING)),SAFE_CAST(event_date  AS DATE)), INTERVAL -1 * 0 DAY)  as end_date
    from
    (
      select
        person_id
        , event_date
        , event_type
        , sum(event_type) over (partition by person_id order by event_date, event_type rows unbounded preceding) as interval_status
      from
      (
        select
          person_id
          , start_date as event_date
          , -1 as event_type
        from cohort_rows
        union all
        select
          person_id
          , DATE_ADD(IF(SAFE_CAST(end_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(end_date  AS STRING)),SAFE_CAST(end_date  AS DATE)), INTERVAL 0 DAY) as end_date
          , 1 as event_type
        from cohort_rows
      ) rawdata
    ) e
    where interval_status = 0
  ) ed on c.person_id = ed.person_id and ed.end_date >= c.start_date
	 group by  c.person_id, c.start_date
 ) e
 group by  1, 3 ;
SELECT * FROM final_cohort;END;"""
        # Construct a BigQuery client object.
        client = bigquery.Client()
        query_job = client.query(sql)
        results = query_job.result()

        # get results
        ohdsi_query_df = results.to_dataframe()
        return ohdsi_query_df
        
