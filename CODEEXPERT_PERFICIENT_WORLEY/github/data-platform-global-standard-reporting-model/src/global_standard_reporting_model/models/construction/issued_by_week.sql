{{
     config(
         materialized = "table",
         tags=["construction"]
         )
}}

with Disciplines as 
( select disciplinecode,disciplineid,disciplinedescription from {{ source('domain_integrated_model', 'transformed_discipline') }}  ), 

CWA AS
(select * from {{ source('domain_integrated_model', 'transformed_cwas') }} ), 

fts_timesheet as (
select cwp.projectid,
cwp.cwpsid as cwp,
iwp.iwpsid as iwp,
disc.disciplinedescription,
cwa.cwasname,
CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
cast(fts.weekendingdate_new as date) as weekendingdate_new,
count (distinct employeenumber) as FTE_count,
sum(spenthours)+sum(overtimehours) as spenthours, 
'FTS' as Category
from {{ source('domain_integrated_model', 'transformed_cwps') }} cwp
inner join  {{ source('domain_integrated_model', 'transformed_iwps') }} iwp on  cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid  
inner join   
(SELECT *,
    case when iwpsid = 'PDN-043' then '1LU00UGC07-EW07I'
    when iwpsid = 'PDN-138' then '1TZ00ZGC07-CN036' else 
    iwpsid end as iwpsid_new,

    case when iwpsid = 'PDN-043' then '2025-04-10'
    when iwpsid = 'PDN-138' then '2024-05-23' else 
    weekendingdate end as weekendingdate_new
FROM {{ source('domain_integrated_model', 'transformed_fts_timesheet') }} ) fts on iwp.iwpsid=fts.iwpsid_new  
left join Disciplines disc on iwp.disciplineid=disc.disciplineid
left join cwa cwa on iwp.constructionworkareaid=cwa.cwasid
group by cwp.projectid,cwp.cwpsid,iwp.iwpsid,disc.disciplinedescription,cwa.cwasname,cwp.contract,weekendingdate_new), 

IWP as (
select cwp.projectid,cwp.cwpsid as cwp,
     iwp.iwpsid as iwp,
     disc.disciplinedescription,
     cwa.cwasname,
     iwp.iwp_status,
     iwp.iwp_actualstartdate,
     iwp.iwp_remaininghours ,
     CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
     'Actual_Start' as Category
from {{ source('domain_integrated_model', 'transformed_cwps') }} cwp 
inner join 
(
select *,
cast(case 
    when  iwp.iwpsid = '1EU00UGC07-CN032' then '2025-04-03'
    when  iwp.iwpsid = '1MZ04ZGM02-ME006' then '2025-04-04'
    when  iwp.iwpsid = '1MM27HPP01-PI001' then '2025-04-10'	
    when  iwp.iwpsid = '1EZ02ZVM07-ME200' then '2025-04-14'	
    when  iwp.iwpsid = '1LU00UGC07-CN08D' then '2025-04-16'	
    when  iwp.iwpsid = '1LZ11ZGC02-EW090' then '2025-04-20'	
    when  iwp.iwpsid = '1LZ10ZGC02-EW040' then '2025-04-21'	
    when  iwp.iwpsid = '1SZ02ZGC03-CN005' then '2025-04-25'	
    when  iwp.iwpsid = '1UZ02ZGC02-CN002' then '2025-04-12'
    when  iwp.iwpsid = '1UZ07ZGC02-CN004' then '2025-04-17'
 else iwp.actualstartdate  end  as date) as iwp_actualstartdate,

 case 
    when  iwp.iwpsid = '1EU00UGC07-CN032' then 'In Progress'
    when  iwp.iwpsid = '1MZ04ZGM02-ME006' then 'In Progress'
    when  iwp.iwpsid = '1MM27HPP01-PI001' then 'In Progress'	
    when  iwp.iwpsid = '1EZ02ZVM07-ME200' then 'In Progress'	
    when  iwp.iwpsid = '1LU00UGC07-CN08D' then 'Started'	
    when  iwp.iwpsid = '1LZ11ZGC02-EW090' then 'Started'	
    when  iwp.iwpsid = '1LZ10ZGC02-EW040' then 'In Progress'	
    when  iwp.iwpsid = '1SZ02ZGC03-CN005' then 'In Progress'	
    when  iwp.iwpsid = '1UZ02ZGC02-CN002' then 'Complete'
    when  iwp.iwpsid = '1UZ07ZGC02-CN004' then 'Complete'
    when  iwp.iwpsid = '1MM28HPP01-PI001' then 'Issued to Field'
    when  iwp.iwpsid = '1LZ02ZGC02-EW060' then 'Issued to Field'
 else iwp.status end as iwp_status,

case 
    when  iwp.iwpsid = '1MZ04ZGM02-ME006' then 20
    when  iwp.iwpsid = '1EZ02ZVM07-ME200' then 50
    when  iwp.iwpsid = '1LU00UGC07-CN08D' then 30	
    when  iwp.iwpsid = '1LZ10ZGC02-EW040' then 20	
    when  iwp.iwpsid = '1SZ02ZGC03-CN005' then 15	
    when  iwp.iwpsid = '1EU00UGC07-CN032' then 25	
    when  iwp.iwpsid = '1MM27HPP01-PI001' then 40	
    when  iwp.iwpsid = '1SZ02ZGC02-CN002' then 20	
 else iwp.remaininghours  end as iwp_remaininghours
from {{ source('domain_integrated_model', 'transformed_iwps') }} iwp) iwp on  cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid
left  join Disciplines disc on iwp.disciplineid=disc.disciplineid
left join cwa cwa on iwp.constructionworkareaid=cwa.cwasid),

IWP_actual_finish_date as (
select 
     cwp.projectid ,
     cwp.cwpsid as cwp,
     iwp.iwpsid as iwp,
     disc.disciplinedescription,
     cwa.cwasname,
     iwp.status,
     iwp.iwp_actualfinishdate,
     CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
     'Actual_Finish' as Category
from {{ source('domain_integrated_model', 'transformed_cwps') }} cwp 
inner join
(
select *,
cast(case 
    when  iwp.iwpsid = '1EU00UGC07-CN032' then '2025-04-13'
    when  iwp.iwpsid = '1MZ04ZGM02-ME006' then '2025-04-14'
    when  iwp.iwpsid = '1MM27HPP01-PI001' then '2025-04-20'	
    when  iwp.iwpsid = '1EZ02ZVM07-ME200' then '2025-05-10'	
    when  iwp.iwpsid = '1LU00UGC07-CN08D' then '2025-04-26'	
    when  iwp.iwpsid = '1LZ11ZGC02-EW090' then '2025-04-30'	
    when  iwp.iwpsid = '1LZ10ZGC02-EW040' then '2025-05-21'	
    when  iwp.iwpsid = '1SZ02ZGC03-CN005' then '2025-05-25'	
    when  iwp.iwpsid = '1UZ02ZGC02-CN002' then '2025-05-12'
    when  iwp.iwpsid = '1UZ07ZGC02-CN004' then '2025-05-17'
 else iwp.actualfinishdate  end  as date) as iwp_actualfinishdate

from {{ source('domain_integrated_model', 'transformed_iwps') }} iwp) iwp on  cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid
left  join Disciplines disc on iwp.disciplineid=disc.disciplineid
left join cwa cwa on   iwp.constructionworkareaid=cwa.cwasid
)
select cast(projectid as varchar(100)) as project_id,cwp,iwp,disciplinedescription,cwasname,scope_discipline,iwp_status as status,cast(iwp_actualstartdate as date) as Date_Key,cast(iwp_remaininghours as numeric(38,2)) value,0 as FTE_count,Category from IWP
union all
select cast(projectid as varchar(100)) as project_id,cwp,iwp,disciplinedescription,cwasname,scope_discipline,status,cast(iwp_actualfinishdate as date) as Date_Key,0.00 value,0 as FTE_count,Category from IWP_actual_finish_date
union all
select cast(projectid as varchar(100)) as project_id,cwp,iwp,disciplinedescription,cwasname,scope_discipline,'' status,cast(weekendingdate_new as date) as Date_Key,cast(spenthours as numeric(38,2)) value,FTE_count,Category from fts_timesheet
