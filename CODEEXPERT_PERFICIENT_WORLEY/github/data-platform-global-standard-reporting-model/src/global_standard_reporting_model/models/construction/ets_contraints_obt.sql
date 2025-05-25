{{
     config(
         materialized = "table",
         tags=["construction"]
         )
}}

select 
iwp.model_updated_date as iwp_updated_date,
cast(iwp.projectid as varchar(100)) as project_id,
cwp.cwpsid as cwp_cwpsid,
iwp.iwpsid as iwp_iwpsid,
iwp.status as iwp_status,
CONCAT(cwp.contract,CONCAT(' ',cwp.disciplinedescription)) AS contractor_discipline,
cwa.cwasname as cwa_constructionworkarea,

cast(iwp.plannedstartdate as DATE) as iwp_plannedstartdate, 
cast(iwp.plannedfinishdate as DATE) as iwp_plannedfinishdate,
cast(iwp.actualstartdate as DATE) as iwp_actualstartdate,  
cast(iwp.actualfinishdate as DATE) as iwp_actualfinishdate, 

cast(remaininghours as numeric(38,2)) as remaininghours,
cast(total_spenthours as numeric(38,2)) as total_spenthours,

constraintid,
constraintstatus,
constrainttypeid,
constrainttype,
constrainttypedescription,
hardconstraint 

from
(select 
model_updated_date,
iwpsid,
cwpsid,
projectid,
constructionworkareaid,
status,
remaininghours,
plannedstartdate,
plannedfinishdate,
actualstartdate,
actualfinishdate
from {{source('domain_integrated_model','transformed_iwps')}}

union all
(select 
'2025-01-02' as model_updated_date,
'TEST1' as iwpsid,
'test' as cwpsid,
41 as projectid, 
115608 as constructionworkareaid,
'In Progress' as status,
50 as remaininghours,
'2024-12-24' as plannedstartdate,
'2025-02-20' as plannedfinishdate,
'2024-12-26' as actualstartdate,
'2025-02-26' as actualfinishdate)

union all
(select 
'2025-03-12' as model_updated_date,
'TEST3' as iwpsid,
'test' as cwpsid,
41 as projectid,
115608 as constructionworkareaid, 
'Complete' as status,
30 as remaininghours,
'2025-02-14' as plannedstartdate,
'2025-03-25' as plannedfinishdate,
'2025-02-26' as actualstartdate,
'2025-03-26' as actualfinishdate)

union all
(select 
'2025-03-16' as model_updated_date,
'TEST4' as iwpsid,
'test' as cwpsid,
41 as projectid,
115608 as constructionworkareaid, 
'Complete' as status,
60 as remaininghours,
'2025-02-16' as plannedstartdate,
'2025-03-23' as plannedfinishdate,
'2025-02-17' as actualstartdate,
'2025-03-29' as actualfinishdate)

union all
(select 
'2025-01-03' as model_updated_date,
'TEST2' as iwpsid,
'test' as cwpsid,
41 as projectid, 
115608 as constructionworkareaid,
'In Progress' as status, 
60 as remaininghours,
'2025-01-06' as plannedstartdate,
'2025-03-13' as plannedfinishdate,
'2025-01-17' as actualstartdate,
'2025-03-16' as actualfinishdate)) iwp

inner join 
(select 
cwpsid,
cwpsdescription,
disciplinedescription,
contract from {{source('domain_integrated_model','transformed_cwps')}}
union all
(select
'test'as cwpsid,
'Manual test case' as cwpsdescription,
'Piping' as disciplinedescription,
'WORLEY' as contract)) cwp on iwp.cwpsid=cwp.cwpsid

left join   
(SELECT *,
    case when iwpsid = 'PDN-043' then '1LU00UGC07-EW07I'
    when iwpsid = 'PDN-138' then '1TZ00ZGC07-CN036' else 
    iwpsid end as iwpsid_new,

    cast (case when iwpsid = 'PDN-043' then '2024-04-10'
    when iwpsid = 'PDN-138' then '2024-04-13' else 
    weekendingdate end as date) as weekendingdate_new,

    case when iwpsid = 'PDN-043' then 4000
    when iwpsid = 'PDN-138' then 3000 else 
    spenthours end as spenthours_new,

    case when iwpsid = 'PDN-043' then 3000
    when iwpsid = 'PDN-138' then 4000 else 
    overtimehours end as overtimehours_new,

    spenthours_new + overtimehours_new as total_spenthours
FROM {{source('domain_integrated_model','transformed_fts_timesheet')}} ) fts on iwp.iwpsid=fts.iwpsid_new  

left join
(select 
    entityid,
    entitytypeid,
    entityname,
    projectid,
    constraintid,
    status as constraintstatus,
    constrainttypeid,
    constrainttype,
    constrainttypedescription,
    hardconstraint
    from {{source('domain_integrated_model', 'transformed_constraints')}} where entitytypeid = '4'
    and constraintstatus not in ('closed','Closed') and hardconstraint = true
    
    union all
    (select 
    600000 as entityid,
    4 as entitytypeid,
    'TEST1' as entityname,
    41 as projectid, 
    'DEF' as constraintid,
    'Open' as constraintstatus,
    NULL as constrainttypeid,
    'Test-C1' as constrainttype,
    '' as constrainttypedescription,
    true as hardconstraint)

    union all
    (select 
    600000 as entityid,
    4 as entitytypeid,
    'TEST1' as entityname,
    41 as projectid, 
    'ABC' as constraintid,
    'Open' as constraintstatus,
    NULL as constrainttypeid,
    'Test-C2' as constrainttype,
    '' as constrainttypedescription,
    true as hardconstraint)

    union all
    (select 
    600001 as entityid,
    4 as entitytypeid,
    'TEST2' as entityname,
    41 as projectid, 
    'ABC' as constraintid,
    'Open' as constraintstatus,
    NULL as constrainttypeid,
    'Test-C1' as constrainttype,
    '' as constrainttypedescription,
    true as hardconstraint) ) constraints on iwp.iwpsid = constraints.entityname

left join {{source('domain_integrated_model','transformed_cwas')}} cwa on  iwp.constructionworkareaid=cwa.cwasid
