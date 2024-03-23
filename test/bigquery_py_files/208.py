
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
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (444413,437663,4141062,4152360,43530637)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (444413,437663,4141062,4152360,43530637)
  and c.invalid_reason is null
) i
left join
(
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4346179,4170869,4226022,37205085,4318555,37016869,443908,4093997,4347651,440285,4087628,4143214,4039793,37397178,40493465,4199309,44784428,44784427,44784429,44782483,43530646,4086668,3197956,4094003,43530637,37017455,4087629,4150518,4184347,4326408,4200980,4229442,4239624,4300533,4308214,4099900,4094000,4087017,438963,4243806)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4346179,4170869,4226022,37205085,4318555,37016869,443908,4093997,4347651,440285,4087628,4143214,4039793,37397178,40493465,4199309,44784428,44784427,44784429,44782483,43530646,4086668,3197956,4094003,43530637,37017455,4087629,4150518,4184347,4326408,4200980,4229442,4239624,4300533,4308214,4099900,4094000,4087017,438963,4243806)
  and c.invalid_reason is null
) e on i.concept_id = e.concept_id
where e.concept_id is null
) c union all 
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (3025085,21490870,21490688,3020891,3025926,3025704,3017614,3011783,3004750,3008557,3007846,3016117,3006749,3016715,3018145,3015039,3009553,4329518,4174894,21490907,21490588,4212763,44809208,21490906,21490590,4039793,4077057,4039791,4151775,3006322,3022060,4265708,3025163)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3025085,21490870,21490688,3020891,3025926,3025704,3017614,3011783,3004750,3008557,3007846,3016117,3006749,3016715,3018145,3015039,3009553,4329518,4174894,21490907,21490588,4212763,44809208,21490906,21490590,4039793,4077057,4039791,4151775,3006322,3022060,4265708,3025163)
  and c.invalid_reason is null
) i
left join
(
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4189949,4040476,4039792,4040104,4164378,4039796,4039795,4040106,4039794,4038778,45769775,4267945)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4189949,4040476,4039792,4040104,4164378,4039796,4039795,4040106,4039794,4038778,45769775,4267945)
  and c.invalid_reason is null
) e on i.concept_id = e.concept_id
where e.concept_id is null
) c union all 
select 4 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (36715585,320073,42872951,435224,440689,45766061)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (36715585,320073,42872951,435224,440689,45766061)
  and c.invalid_reason is null
) i
) c union all 
select 5 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4250734)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4250734)
  and c.invalid_reason is null
) i
) c union all 
select 6 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (37393856,4148615,3017732,3013650,3017501)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (3017732,3013650,3017501)
  and c.invalid_reason is null
) i
) c union all 
select 7 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4311853,4170143,435742,4000938,4161193,4309542,132736,72410,435613)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4311853,4170143,435742,4000938,4161193,4309542,132736,72410,435613)
  and c.invalid_reason is null
) i
) c union all 
select 8 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (262,9203,9201)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (262,9203,9201)
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) p
join (
  -- Begin Visit Occurrence Criteria
select c.person_id, c.visit_occurrence_id as event_id, c.visit_start_date as start_date, c.visit_end_date as end_date,
       c.visit_occurrence_id, c.visit_start_date as sort_date
from 
(
  select vo.* 
  from {os.environ["WORKSPACE_CDR"]}.visit_occurrence vo
join codesets cs on (vo.visit_concept_id = cs.concept_id and cs.codeset_id = 8)
) c
-- End Visit Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.end_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.end_date <= p.op_end_date ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
union all
-- Begin Criteria Group
select 1 as index_id, 2, event_id
from
(
    select e.person_id, e.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 1 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 1 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 4)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -1 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 1 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 2 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) c
where (c.value_as_number >= 0.0100 and c.value_as_number <= 1.4990)
and c.unit_concept_id in (9444,8848,8816,8961,44777588)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -1 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 1 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 3 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Condition Occurrence Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) c
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Measurement Criteria
union all
-- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Measurement Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 38.0000 and c.value_as_number <= 42.0000)
and c.unit_concept_id in (586323)
-- End Observation Criteria
union all
-- Begin Observation Criteria
select c.person_id, c.observation_id as event_id, c.observation_date as start_date, DATE_ADD(IF(SAFE_CAST(c.observation_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.observation_date  AS STRING)),SAFE_CAST(c.observation_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.observation_date as sort_date
from 
(
  select o.* 
  from {os.environ["WORKSPACE_CDR"]}.observation o
join codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
where (c.value_as_number >= 100.4000 and c.value_as_number <= 120.0000)
and c.unit_concept_id in (9289)
-- End Observation Criteria
union all
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 7)
) c
-- End Condition Occurrence Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) c
where (c.value_as_number >= 10.0000 and c.value_as_number <= 1500.0000)
and c.unit_concept_id in (8784,8647)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL -1 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 1 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
      ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) > 0
 ) g
-- End Criteria Group
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) = 2
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
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
  select 0 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Visit Occurrence Criteria
select c.person_id, c.visit_occurrence_id as event_id, c.visit_start_date as start_date, c.visit_end_date as end_date,
       c.visit_occurrence_id, c.visit_start_date as sort_date
from 
(
  select vo.* 
  from {os.environ["WORKSPACE_CDR"]}.visit_occurrence vo
join codesets cs on (vo.visit_concept_id = cs.concept_id and cs.codeset_id = 8)
) c
-- End Visit Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= p.op_start_date and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.end_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.end_date <= p.op_end_date ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
   ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) > 0
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
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) c
where (c.value_as_number >= 4.0000 and c.value_as_number <= 8.2500)
and c.unit_concept_id in (9444,8848,8816,8961)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
  group by  p.person_id, p.event_id
 having count(cc.event_id) = 0
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 1 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
from qualified_events p
join (
  -- Begin Measurement Criteria
select c.person_id, c.measurement_id as event_id, c.measurement_date as start_date, DATE_ADD(IF(SAFE_CAST(c.measurement_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.measurement_date  AS STRING)),SAFE_CAST(c.measurement_date  AS DATE)), INTERVAL 1 DAY) as end_date,
       c.visit_occurrence_id, c.measurement_date as sort_date
from 
(
  select m.* 
  from {os.environ["WORKSPACE_CDR"]}.measurement m
join codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) c
where (c.value_as_number >= 4000.0000 and c.value_as_number <= 8250.0000)
and c.unit_concept_id in (8784,8647)
and (c.range_low >= 1500.0000 and c.range_low <= 4000.0000)
-- End Measurement Criteria
) a on a.person_id = p.person_id  and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) > 0
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
select inclusion_rule_id, person_id, event_id from inclusion_1) i;
DELETE FROM inclusion_0 WHERE True;
drop table inclusion_0;
DELETE FROM inclusion_1 WHERE True;
drop table inclusion_1;
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
  where (mg.inclusion_rule_mask = power(cast(2  as int64),2)-1)
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
select event_id, person_id, end_date from strategy_ends
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
        
