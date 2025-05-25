{{
     config(
         materialized = "table",
         tags=["construction"]
         )
}}

with Disciplines as 
( select disciplinecode,disciplineid,disciplinedescription from {{ source('domain_integrated_model', 'o3_discipline') }}  ), 

CWA AS
(select * from {{ source('domain_integrated_model', 'o3_cwas') }} ), 

fts_timesheet as (
select cwp.projectid,
cwp.cwpsid as cwp,
iwp.iwpsid as iwp,
disc.disciplinedescription,
cwa.cwasname,
CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
fts.weekendingdate,
count (distinct employeenumber) as FTE_count,
sum(spenthours)+sum(overtimehours) as spenthours, 
'FTS' as Category
from {{ source('domain_integrated_model', 'o3_cwps') }} cwp
inner join  {{ source('domain_integrated_model', 'o3_iwps') }} iwp on  cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid  
inner join   
(SELECT *,
    case when iwpsid = 'PDN-016' then '1PZ07ZGC03-CN270'
    when iwpsid = 'PDN-043' then '1KU00UGC08-CN010' else 
    iwpsid end as iwpsid_new
FROM {{ source('domain_integrated_model', 'fts_timesheet') }} ) fts on iwp.iwpsid=fts.iwpsid_new  
left join Disciplines disc on iwp.disciplineid=disc.disciplineid
left join cwa cwa on iwp.constructionworkareaid=cwa.cwasid
group by cwp.projectid,cwp.cwpsid,iwp.iwpsid,disc.disciplinedescription,cwa.cwasname,cwp.contract,fts.weekendingdate), 

IWP as (
select cwp.projectid,cwp.cwpsid as cwp,
     iwp.iwpsid as iwp,
     disc.disciplinedescription,
     cwa.cwasname,
     iwp.status,
     iwp.iwp_actualstartdate,
     iwp.iwp_remaininghours ,
     CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
     'Actual_Start' as Category
from  {{ source('domain_integrated_model', 'o3_cwps') }} cwp 
inner join 
(
select *,
cast(case 
    when  iwp.iwpsid = '1PZ07ZGC03-CN270' then '2024-04-03'
    when  iwp.iwpsid = '1PZ07ZGC02-EW090' then '2024-04-04'
    when  iwp.iwpsid = '1KU00UGC08-CN010' then '2024-04-10'	
    when  iwp.iwpsid = '1PZ07ZGC03-CN260' then '2024-04-14'	
    when  iwp.iwpsid = '1PZ07ZGC03-CN190' then '2024-04-16'	
    when  iwp.iwpsid = '1UZ07ZGC02-CN011' then '2024-04-20'	
    when  iwp.iwpsid = '1UZ11ZGC03-CN009' then '2024-04-21'	
    when  iwp.iwpsid = '1SZ02ZGC02-CN002' then '2024-04-25'	
    when  iwp.iwpsid = '1UZ02ZGC02-CN002' then '2024-04-12'
    when  iwp.iwpsid = '1UZ07ZGC02-CN004' then '2024-04-17'
 else iwp.actualstartdate  end  as date) as iwp_actualstartdate,

case 
    when  iwp.iwpsid = '1PZ07ZGC03-CN270' then '20'
    when  iwp.iwpsid = '1PZ07ZGC02-EW090' then '50'
    when  iwp.iwpsid = '1KU00UGC08-CN010' then '30'	
    when  iwp.iwpsid = '1PZ07ZGC03-CN260' then '20'	
    when  iwp.iwpsid = '1PZ07ZGC03-CN190' then '15'	
    when  iwp.iwpsid = '1UZ07ZGC02-CN011' then '25'	
    when  iwp.iwpsid = '1UZ11ZGC03-CN009' then '40'	
    when  iwp.iwpsid = '1SZ02ZGC02-CN002' then '20'	
 else iwp.remaininghours  end as iwp_remaininghours
 from {{ source('domain_integrated_model', 'o3_iwps') }} iwp) iwp on  cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid
left  join Disciplines disc on iwp.disciplineid=disc.disciplineid
left join cwa cwa on iwp.constructionworkareaid=cwa.cwasid),

IWP_actual_finish_date as (
select cwp.projectid,
     cwp.cwpsid as cwp,
     iwp.iwpsid as iwp,
     disc.disciplinedescription,
     cwa.cwasname,
     iwp.status,
     iwp.actualfinishdate,
     CONCAT(cwp.contract,CONCAT(' ',disc.disciplinedescription)) AS scope_discipline,
     'Actual_Finish' as Category
from {{ source('domain_integrated_model', 'o3_cwps') }} cwp 
inner join {{ source('domain_integrated_model', 'o3_iwps') }} iwp on  cwp.projectid=iwp.projectid and cwp.cwpsid=iwp.cwpsid
left  join Disciplines disc on iwp.disciplineid=disc.disciplineid
left join cwa cwa on   iwp.constructionworkareaid=cwa.cwasid
)
select projectid,cwp,iwp,disciplinedescription,cwasname,scope_discipline,status,cast(iwp_actualstartdate as date) as Date_Key,cast(iwp_remaininghours as numeric(38,2)) value,NULL as FTE_count,Category from IWP
union all
select projectid,cwp,iwp,disciplinedescription,cwasname,scope_discipline,status,cast(actualfinishdate as date) as Date_Key,0.00 value,NULL as FTE_count,Category from IWP_actual_finish_date
union all
select projectid,cwp,iwp,disciplinedescription,cwasname,scope_discipline,'' status,cast(weekendingdate as date) as Date_Key,cast(spenthours as numeric(38,2)) value,FTE_count,Category from fts_timesheet
