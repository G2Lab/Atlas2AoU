
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
select 0 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4146943,46274061,4266367,42537960,4112824,4299935,46269706,763011)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4146943,46274061,4266367,42537960,4112824,4299935,46269706,763011)
  and c.invalid_reason is null
) i
left join
(
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4042202)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4042202)
  and c.invalid_reason is null
) e on i.concept_id = e.concept_id
where e.concept_id is null
) c union all 
select 1 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (45757528,4262075)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (45757528,4262075)
  and c.invalid_reason is null
) i
) c union all 
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (44789502,37392948,37394408,36676235,3001703,3016424,3038149,3016676,3017256,3020370,2213094,2213181,2213063,2213062,40757077,40756898,4039357,37392223,44806470,37394297,4044987,4039358,37392224,44807860,37394300,4044988,37392761,40653953,40655680,40655681,40655682,40655683,40655684,40655685,40655686,37080282,40655687,40655688,40655689,40655690,4047171,40655691,40484560,3002988,3002646,3003682)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (44789502,37392948,37394408,36676235,3001703,3016424,3038149,3016676,3017256,3020370,2213094,2213181,2213063,2213062,40757077,40756898,4039357,37392223,44806470,37394297,4044987,4039358,37392224,44807860,37394300,4044988,37392761,40653953,40655680,40655681,40655682,40655683,40655684,40655685,40655686,37080282,40655687,40655688,40655689,40655690,4047171,40655691,40484560,3002988,3002646,3003682)
  and c.invalid_reason is null
) i
left join
(
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (37066972,3002696,3000619,3023674,3001047,3007341,37398130,4201049,37393224,44805899,45757528,37393225,4196405,44805900,4262075,40760327,37048815,37058506,37055728,37040765,37023170,37075082,37041511,37066453,37057346,37054344,37022661,37067997,37028300,37054453,37036578,37078464,37067504,37070652,37048012,37071077,37042025,37024679,37042070,37052164,37037245,37072518,37044556,37025175,37063376,37068553,37045806,37036252,37064420,37037941,37062169,37076139,37034233,37051887,37075960,37022372,37031117,37073627,37057853,37042870,37075259,37030627,37059249,37037961,37047321,37075847,37073492,37056311,37029363,37023424,37073273,37076384,37069805,37026917,37048913,37074405,37040736,37076803,37075491,37072743,40758391,42869813,40758149,3043198,4017924,37050279,3026510,3027889)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (37066972,3002696,3000619,3023674,3001047,3007341,37398130,4201049,37393224,44805899,45757528,37393225,4196405,44805900,4262075,40760327,37048815,37058506,37055728,37040765,37023170,37075082,37041511,37066453,37057346,37054344,37022661,37067997,37028300,37054453,37036578,37078464,37067504,37070652,37048012,37071077,37042025,37024679,37042070,37052164,37037245,37072518,37044556,37025175,37063376,37068553,37045806,37036252,37064420,37037941,37062169,37076139,37034233,37051887,37075960,37022372,37031117,37073627,37057853,37042870,37075259,37030627,37059249,37037961,37047321,37075847,37073492,37056311,37029363,37023424,37073273,37076384,37069805,37026917,37048913,37074405,37040736,37076803,37075491,37072743,40758391,42869813,40758149,3043198,4017924,37050279,3026510,3027889)
  and c.invalid_reason is null
) e on i.concept_id = e.concept_id
where e.concept_id is null
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
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
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
where c.value_as_concept_id in (4126681,45877985,9191,45884084,4181412,45879438)
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
where c.value_as_concept_id in (4126681,45877985,9191,45884084,4181412,45879438)
-- End Observation Criteria
  ) e
	join observation_period op on e.person_id = op.person_id and e.start_date >=  op.observation_period_start_date and e.start_date <= op.observation_period_end_date
  where DATE_ADD(IF(SAFE_CAST(op.observation_period_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(op.observation_period_start_date  AS STRING)),SAFE_CAST(op.observation_period_start_date  AS DATE)), INTERVAL 0 DAY) <= e.start_date and DATE_ADD(IF(SAFE_CAST(e.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(e.start_date  AS STRING)),SAFE_CAST(e.start_date  AS DATE)), INTERVAL 0 DAY) <= op.observation_period_end_date
) p
-- End Primary Events
) pe
) qe
;
--- Inclusion Rule Inserts
CREATE TEMPORARY TABLE inclusion_events (inclusion_rule_id INT64,
	person_id INT64,
	event_id INT64
);
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
        
