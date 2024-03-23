
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
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (135618)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (135618)
  and c.invalid_reason is null
) i
) c union all 
select 2 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (4169287)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4169287)
  and c.invalid_reason is null
) i
) c union all 
select 3 as codeset_id, c.concept_id from (select distinct i.concept_id from
( 
  select concept_id from {os.environ["WORKSPACE_CDR"]}.concept where concept_id in (140214)
union distinct select c.concept_id
  from {os.environ["WORKSPACE_CDR"]}.concept c
  join {os.environ["WORKSPACE_CDR"]}.concept_ancestor ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (140214)
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
  select pe.person_id, pe.event_id, pe.start_date, pe.end_date, pe.visit_occurrence_id, pe.sort_date from (
-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
-- End Condition Occurrence Criteria
) pe
join (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
from
(
    select e.person_id, e.event_id 
    from (select q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date
from (-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
-- End Condition Occurrence Criteria
) q
join observation_period op on q.person_id = op.person_id 
  and op.observation_period_start_date <= q.start_date and op.observation_period_end_date >= q.start_date
) e
  inner join
  (
    -- Begin Correlated Criteria
  select 0 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
from (select q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date
from (-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
-- End Condition Occurrence Criteria
) q
join observation_period op on q.person_id = op.person_id 
  and op.observation_period_start_date <= q.start_date and op.observation_period_end_date >= q.start_date
) p
join (
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
) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
union all
-- Begin Correlated Criteria
  select 1 as index_id, cc.person_id, cc.event_id
  from (select p.person_id, p.event_id 
from (select q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date as op_start_date, op.observation_period_end_date as op_end_date
from (-- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) c
-- End Condition Occurrence Criteria
) q
join observation_period op on q.person_id = op.person_id 
  and op.observation_period_start_date <= q.start_date and op.observation_period_end_date >= q.start_date
) p
join (
  -- Begin Condition Occurrence Criteria
select c.person_id, c.condition_occurrence_id as event_id, c.condition_start_date as start_date, coalesce(c.condition_end_date, DATE_ADD(IF(SAFE_CAST(c.condition_start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(c.condition_start_date  AS STRING)),SAFE_CAST(c.condition_start_date  AS DATE)), INTERVAL 1 DAY)) as end_date,
  c.visit_occurrence_id, c.condition_start_date as sort_date
from 
(
  select co.* 
  from {os.environ["WORKSPACE_CDR"]}.condition_occurrence co
  join codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 3)
) c
-- End Condition Occurrence Criteria
) a on a.person_id = p.person_id  and a.start_date >= p.op_start_date and a.start_date <= p.op_end_date and a.start_date >= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) and a.start_date <= DATE_ADD(IF(SAFE_CAST(p.start_date  AS DATE) IS NULL,PARSE_DATE('%Y%m%d', cast(p.start_date  AS STRING)),SAFE_CAST(p.start_date  AS DATE)), INTERVAL 0 DAY) ) cc 
  group by  cc.person_id, cc.event_id
 having count(cc.event_id) >= 1
-- End Correlated Criteria
    ) cq on e.person_id = cq.person_id and e.event_id = cq.event_id
    group by  e.person_id, e.event_id
   having count(index_id) > 0
 ) g
-- End Criteria Group
) ac on ac.person_id = pe.person_id and ac.event_id = pe.event_id
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
        
