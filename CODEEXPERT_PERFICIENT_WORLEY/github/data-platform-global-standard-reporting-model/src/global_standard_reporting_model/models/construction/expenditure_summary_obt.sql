{{
     config(
         materialized = "table",
         tags=["construction"]
         )
}}

with iwp_main as (
select 
iwp.model_updated_date as iwp_updated_date,
iwp.iwpsid as iwp_iwpsid,
iwp.cwpsid as iwp_cwpsid,
iwp.projectid as iwp_project_id,
iwp.status as iwp_status,
iwp.disciplineid as iwp_disciplineid,
iwp.constructionworkareaid as  iwp_constructionworkareaid,
iwp.estimatedhours as iwp_estimatedhours,
cwp.cwpsdescription as cwp_cwpsdescription,
cwp.contract as cwp_contract,
cast(iwp.plannedstartdate as DATE) as iwp_plannedstartdate,
cast(iwp.plannedfinishdate as DATE) as iwp_plannedfinishdate

from
(select 
model_updated_date,
iwpsid,
cwpsid,
projectid,
status,
disciplineid,
constructionworkareaid,
estimatedhours,
plannedstartdate,
plannedfinishdate
from {{source('domain_integrated_model','transformed_iwps')}} 

union all
(select 
'2025-01-02' as model_updated_date,
'TEST1' as iwpsid,
'test' as cwpsid,
'41' as projectid, 
'In Progress' as status,
'1400' as disciplineid, 
'115672' as constructionworkareaid,
'350' as estimatedhours,
'2025-04-21' as plannedstartdate,
'2025-06-17' as plannedfinishdate)

union all
(select 
'2025-01-03' as model_updated_date,
'TEST2' as iwpsid,
'test' as cwpsid,
'41' as projectid, 
'In Progress' as status,
'1401' as disciplineid, 
'115727' as constructionworkareaid,
'500' as estimatedhours,
'2025-04-06' as plannedstartdate,
'2025-07-17' as plannedfinishdate)

union all
(select 
'2025-01-13' as model_updated_date,
'TEST3' as iwpsid,
'test' as cwpsid,
'41' as projectid, 
'In Progress' as status,
'1401' as disciplineid, 
'115727' as constructionworkareaid,
'400' as estimatedhours,
'2025-04-21' as plannedstartdate,
'2025-07-07' as plannedfinishdate)) iwp

inner join 
(select 
cwpsid,
cwpstatus,
cwpsdescription,
contract from {{source('domain_integrated_model','transformed_cwps')}} 
union all
(select
'test'as cwpsid,
'In Development' as cwpstatus,
'Manual test case' as cwpsdescription,
'WORLEY' as contract) ) cwp on iwp.cwpsid=cwp.cwpsid),

date_table as (
with date_range as (
select min(iwp_plannedstartdate) as min_startdate,
max(iwp_plannedfinishdate) as max_finishdate from iwp_main)

select dim_date.date_key as iwp_date
from  "construction"."dim_date_new1" dim_date
inner join date_range dr
on dim_date.date_key BETWEEN dr.min_startdate AND dr.max_finishdate
order by dim_date.date_key),

daylevel_iwp as (
select * from iwp_main iwp_day
inner join date_table dt on dt.iwp_date between iwp_day.iwp_plannedstartdate and iwp_day.iwp_plannedfinishdate),

daylevel_planned_hours as (
select *, 
(dl_iwp.iwp_plannedfinishdate - dl_iwp.iwp_plannedstartdate + 1) as datedifference,
ROUND(dl_iwp.iwp_estimatedhours / datedifference, 3) as day_estimated_hours,
CASE when EXTRACT(DOW FROM date (dl_iwp.iwp_date)) = 0 then  dl_iwp.iwp_date
else dl_iwp.iwp_date + 7 -1 * cast(DATE_PART(dayofweek, date (dl_iwp.iwp_date)) as INT) end as daylevel_weekend
from daylevel_iwp dl_iwp order by dl_iwp.iwp_date),

iwp_joined as ( 
select iwp_iwpsid,
daylevel_weekend,
cast(sum(day_estimated_hours) as numeric(38,2)) as weekly_planned_hours,
max(iwp_plannedstartdate) as iwp_plannedstartdate,
max(iwp_plannedfinishdate) as iwp_plannedfinishdate,
max(datedifference) as datedifference,

max(iwp_updated_date) as iwp_updated_date,
max(iwp_cwpsid) as iwp_cwpsid,
max(iwp_project_id) as iwp_project_id,
max(iwp_status) as iwp_status,
max(iwp_disciplineid) as iwp_disciplineid,
max(iwp_constructionworkareaid) as iwp_constructionworkareaid,
max(cwp_cwpsdescription) as cwp_cwpsdescription,
max(cwp_contract) as cwp_contract
from daylevel_planned_hours
group by iwp_iwpsid,daylevel_weekend
order by iwp_iwpsid,daylevel_weekend),

ch_table as (select 
    entityname,
    projectid,
    datecreated,
    newvalue
    from {{source('domain_integrated_model','transformed_change_history')}} where fieldname='Earned Hours'
    
    union all
    select 
    'TEST1' as entityname,
    '41' as projectid,
    '2025-04-17' as datecreated,
    '5' as newvalue
    
    union all
    select 
    'TEST1' as entityname,
    '41' as projectid,
    '2025-04-26' as datecreated,
    '10' as newvalue

    union all
    select 
    'TEST1' as entityname,
    '41' as projectid,
    '2025-04-28' as datecreated,
    '20' as newvalue

    union all
    select 
    'TEST1' as entityname,
    '41' as projectid,
    '2025-05-01' as datecreated,
    '60' as newvalue

    union all
    select 
    'TEST2' as entityname,
    '41' as projectid,
    '2025-04-19' as datecreated,
    '20' as newvalue
    
    union all
    select 
    'TEST2' as entityname,
    '41' as projectid,
    '2025-04-26' as datecreated,
    '110' as newvalue

    union all
    select 
    'TEST2' as entityname,
    '41' as projectid,
    '2025-05-02' as datecreated,
    '160' as newvalue

    union all
    select 
    'TEST1' as entityname,
    '41' as projectid,
    '2025-06-23' as datecreated,
    '160' as newvalue),

change_history as (
select
ch_cwpsid,
ch_project_id,
ch_disciplineid,
ch_cwaid,
ch_contract,
    
ch_entityname,
ch_projectid,
ch_datecreated,
ch_weekend,
ch_newvalue,
case when lag(ch_newvalue) over (partition by ch_entityname order by ch_datecreated) is null 
then ch_newvalue
else ch_newvalue - (lag(ch_newvalue) over (partition by ch_entityname order by ch_datecreated))
end as ch_actual_value
from (
    select 
    entityname as ch_entityname,
    projectid as ch_projectid,
    iwp_iwpsid,
    iwp_cwpsid as ch_cwpsid,
    iwp_project_id as ch_project_id,
    iwp_disciplineid as ch_disciplineid,
    iwp_constructionworkareaid as ch_cwaid,
    cwp_contract as ch_contract,
    cast(datecreated as DATE) as ch_datecreated,
    case when EXTRACT(DOW from date (ch_datecreated)) = 0 then  ch_datecreated
    else ch_datecreated + 7 -1 * cast(DATE_PART(dayofweek, date (ch_datecreated)) as INT) end as ch_weekend,
    CASE
      WHEN newvalue >=0 then cast(newvalue as decimal(8,2))
      ELSE NULL  
    END AS ch_newvalue,
    row_number() over (partition by ch_entityname, ch_weekend, ch_projectid
    order by ch_datecreated desc, ch_newvalue desc) as rn
    from 
    ( 
    ch_table
    left join ( select 
    iwp_iwpsid,
    iwp_cwpsid,
    iwp_project_id,
    iwp_disciplineid,
    iwp_constructionworkareaid,
    cwp_contract from iwp_main
    )iwp_ch_join on ch_table.entityname = iwp_ch_join.iwp_iwpsid
    )) where rn = 1 
    order by ch_entityname,ch_datecreated desc),

fts_table as (select iwpsid,
projectid,
weekendingdate,
spenthours,
overtimehours from {{source('domain_integrated_model','transformed_fts_timesheet')}}
    union all
    select 
    'TEST1' as iwpsid,
    '41' as projectid,
    '2025-04-20' as weekendingdate,
    '30' as spenthours,
    '20' as overtimehours
    
    union all
    select 
    'TEST1' as iwpsid,
    '41' as projectid,
    '2025-04-27' as weekendingdate,
    '20' as spenthours,
    '30' as overtimehours  

    union all
    select 
    'TEST1' as iwpsid,
    '41' as projectid,
    '2025-05-04' as weekendingdate,
    '25' as spenthours,
    '15' as overtimehours  

    union all
    select 
    'TEST1' as iwpsid,
    '41' as projectid,
    '2025-05-11' as weekendingdate,
    '30' as spenthours,
    '20' as overtimehours
    union all

    select 
    'TEST2' as iwpsid,
    '41' as projectid,
    '2025-04-20' as weekendingdate,
    '15' as spenthours,
    '45' as overtimehours

    union all
    select 
    'TEST2' as iwpsid,
    '41' as projectid,
    '2025-04-27' as weekendingdate,
    '20' as spenthours,
    '10' as overtimehours

    union all
    select 
    'TEST2' as iwpsid,
    '41' as projectid,
    '2025-05-18' as weekendingdate,
    '25' as spenthours,
    '15' as overtimehours

    union all
    select 
    'TEST2' as iwpsid,
    '41' as projectid,
    '2025-06-28' as weekendingdate,
    '25' as spenthours,
    '15' as overtimehours),

fts as (
select
iwpsid as fts_iwpsid,
max(iwp_project_id) as fts_projectid,
max(iwp_cwpsid) as fts_cwpsid,
max(iwp_disciplineid) as fts_disciplineid,
max(iwp_constructionworkareaid) as fts_cwaid,
max(cwp_contract) as fts_contract,

cast(weekendingdate as DATE) as fts_weekend,
sum(spenthours) as fts_spenthours_sum,
sum(overtimehours) as fts_overtimehours_sum,
(fts_spenthours_sum + fts_overtimehours_sum) as fts_St_Ot_sum
from 
(
    fts_table
    left join ( select 
    iwp_iwpsid,
    iwp_cwpsid,
    iwp_project_id,
    iwp_disciplineid,
    iwp_constructionworkareaid,
    cwp_contract from iwp_main
    )iwp_fts_join on fts_table.iwpsid = iwp_fts_join.iwp_iwpsid
)
group by fts_iwpsid,fts_weekend
order by fts_iwpsid,fts_weekend)

select 
iwp_updated_date,
iwp_plannedstartdate,
iwp_plannedfinishdate,
datedifference,

cwpsid,
iwpsid,
weekend,
cast(project_id as varchar(100)) as project_id,
discipline_id,
cwaid,
contract_filter,

ch_newvalue,
weekly_planned_hours,
ch_actual_value as weekly_earned_hours,
fts_St_Ot_sum as weekly_spent_hours,

disc.disciplinedescription as disc_disciplinedescription,
cwa.cwasname as cwa_constructionworkarea,
CONCAT(contract_filter,CONCAT(
 CASE when contract_filter is not null and disc_disciplinedescription is not null then ' ' else '' end, disc_disciplinedescription)) AS contract_discipline

from
(SELECT
*,
CASE
    WHEN iwpsid_ch IS NULL THEN fts_iwpsid
    ELSE iwpsid_ch
  END AS iwpsid,

  CASE
    WHEN weekend_ch IS NULL THEN fts_weekend
    ELSE weekend_ch
  END AS weekend,

  CASE
    WHEN cwpsid_ch IS NULL THEN fts_cwpsid
    ELSE cwpsid_ch
  END AS cwpsid,

  CASE
    WHEN project_id_ch IS NULL THEN fts_projectid
    ELSE project_id_ch
  END AS project_id,

  CASE
    WHEN discipline_id_ch IS NULL THEN fts_disciplineid
    ELSE discipline_id_ch
  END AS discipline_id,

  CASE
    WHEN cwaid_ch IS NULL THEN fts_cwaid
    ELSE cwaid_ch
  END AS cwaid,

  CASE
    WHEN contract_filter_ch IS NULL THEN fts_contract
    ELSE contract_filter_ch
  END AS contract_filter

from 
(select
iwp_updated_date,
iwp_cwpsid,
iwp_status,
iwp_disciplineid,
iwp_constructionworkareaid,

cwp_cwpsdescription,
cwp_contract,
iwp_plannedstartdate,
iwp_plannedfinishdate,
datedifference,

iwp_iwpsid,
ch_entityname,
ch_datecreated,
ch_weekend,
daylevel_weekend,
weekly_planned_hours,
ch_newvalue,
ch_actual_value,

CASE
    WHEN iwp_iwpsid IS NULL THEN ch_entityname
    ELSE iwp_iwpsid
  END AS iwpsid_ch,

  CASE
    WHEN daylevel_weekend IS NULL THEN ch_weekend
    ELSE daylevel_weekend
  END AS weekend_ch,

  CASE
    WHEN iwp_cwpsid IS NULL THEN ch_cwpsid
    ELSE iwp_cwpsid
  END AS cwpsid_ch,

  CASE
    WHEN iwp_project_id IS NULL THEN ch_project_id
    ELSE iwp_project_id
  END AS project_id_ch,

  CASE
    WHEN iwp_disciplineid IS NULL THEN ch_disciplineid
    ELSE iwp_disciplineid
  END AS discipline_id_ch,

  CASE
    WHEN iwp_constructionworkareaid IS NULL THEN ch_cwaid
    ELSE iwp_constructionworkareaid
  END AS cwaid_ch,

  CASE
    WHEN cwp_contract IS NULL THEN ch_contract
    ELSE cwp_contract
  END AS contract_filter_ch

from iwp_joined 
full join change_history on iwp_joined.iwp_iwpsid = change_history.ch_entityname and iwp_joined.daylevel_weekend = change_history.ch_weekend) ch_iwp_joined
full join fts on ch_iwp_joined.iwpsid_ch = fts.fts_iwpsid and ch_iwp_joined.weekend_ch = fts.fts_weekend) fts_iwp_joined

left join {{source('domain_integrated_model','transformed_cwas')}}  cwa on  fts_iwp_joined.cwaid=cwa.cwasid
left join {{source('domain_integrated_model','transformed_discipline')}} disc on  fts_iwp_joined.discipline_id=disc.disciplineid
where project_id = 41
