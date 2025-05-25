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
cast(iwp.estimatedhours as numeric(38,2)) as iwp_estimatedhours,
cast(iwp.earnedhours as numeric(38,2)) as iwp_earnedhours,
cast(iwp.actualfinishdate as DATE) as iwp_actualfinishdate,
cast(iwp.plannedstartdate as DATE) as iwp_plannedstartdate, 
cast(iwp.percentcomplete as numeric(38,2)) as iwp_percentcomplete,

case when (iwp_actualfinishdate is not null) then
    case when (iwp_earnedhours < iwp_estimatedhours) OR 
    ((iwp_earnedhours < iwp_estimatedhours) and iwp_percentcomplete=100) OR
    (iwp_percentcomplete is null or iwp_percentcomplete=0)
    then true 
    else NULL
    end 
end as iwps_with_actualfinishdate_without_fullprogress,

constraintid,
constraintstatus,
cwpstatus,
constraintlongdescription, 
constraintdescription,

_constraint,
hardconstraint,
constrainttypeid,
constrainttype,
case when hardconstraint=true and (constrainttype is not null and constrainttype != '') then 1
else NULL end as constrainttype_count

from
(select 
model_updated_date,
iwpsid,
cwpsid,
projectid,
status,
actualfinishdate,
percentcomplete,
estimatedhours,
earnedhours,
actualduration,
superintendent,
plannedstartdate,
plannedfinishdate
from {{ source('domain_integrated_model', 'transformed_iwps') }}

union all
(select 
'2025-01-02' as model_updated_date,
'TEST1' as iwpsid,
'test' as cwpsid,
41 as projectid, 
'In Progress' as status,
'2025-01-16' as actualfinishdate,
100 as percentcomplete,
100 as estimatedhours,
10 as earnedhours,
'50' as actualduration,
'XYZ' as superintendent,
'2024-12-24' as plannedstartdate,
'2025-01-15' as plannedfinishdate)

union all
(select 
'2025-03-12' as model_updated_date,
'TEST3' as iwpsid,
'test' as cwpsid,
41 as projectid, 
'Complete' as status,
NULL as actualfinishdate,
100 as percentcomplete,
80 as estimatedhours,
30 as earnedhours,
'150' as actualduration,
'XYZ' as superintendent,
'2025-02-14' as plannedstartdate,
'2025-05-15' as plannedfinishdate)

union all
(select 
'2025-03-16' as model_updated_date,
'TEST4' as iwpsid,
'test' as cwpsid,
41 as projectid, 
'Complete' as status,
NULL as actualfinishdate,
100 as percentcomplete,
50 as estimatedhours,
40 as earnedhours,
'120' as actualduration,
'DEF' as superintendent,
'2025-02-16' as plannedstartdate,
'2025-05-13' as plannedfinishdate)

union all
(select 
'2025-01-03' as model_updated_date,
'TEST2' as iwpsid,
'test' as cwpsid,
41 as projectid, 
'In Progress' as status, 
'2025-02-03' as actualfinishdate,
70 as percentcomplete,
90 as estimatedhours,
5 as earnedhours,
'25' as actualduration,
'ABC' as superintendent,
'2025-01-06' as plannedstartdate,
'2025-02-05' as plannedfinishdate)) iwp

inner join 
(select 
cwpsid,
cwpsdescription,
contract from {{ source('domain_integrated_model', 'transformed_cwps') }}
union all
(select
'test'as cwpsid,
'Manual test case' as cwpsdescription,
'WORLEY' as contract)) cwp on iwp.cwpsid=cwp.cwpsid

left join
(select 
    entityid,
    entitytypeid,
    entityname,
    projectid,
    
    constraintid,
    status as constraintstatus,
    cwpstatus,
    constraintlongdescription, 
    constraintdescription,

    constrainttypeid,
    constrainttype,
    constrainttypedescription,
    _constraint,
    hardconstraint
    from {{ source('domain_integrated_model', 'transformed_constraints') }} where entitytypeid = '4'
    union all
    (select 
    600000 as entityid,
    4 as entitytypeid,
    'TEST1' as entityname,
    41 as projectid, 
    'DEF' as constraintid,
    'Open' as constraintstatus,
    'In Development' as cwpstatus,
    'test-longdescription' as constraintlongdescription, 
    NULL as constraintdescription,
    NULL as constrainttypeid,
    'Test-C1' as constrainttype,
    '' as constrainttypedescription,
    'description' as _constraint,
    'true' as hardconstraint)

    union all
    (select 
    600000 as entityid,
    4 as entitytypeid,
    'TEST1' as entityname,
    41 as projectid, 
    'ABC' as constraintid,
    'Open' as constraintstatus,
    'In Development' as cwpstatus,
    'test-longdescription' as constraintlongdescription, 
    'test-description' as constraintdescription,
    NULL as constrainttypeid,
    'Test-C2' as constrainttype,
    '' as constrainttypedescription,
    'description' as _constraint,
    true as hardconstraint)

    union all
    (select 
    600001 as entityid,
    4 as entitytypeid,
    'TEST2' as entityname,
    41 as projectid, 
    'ABC' as constraintid,
    'Open' as constraintstatus,
    'In Development' as cwpstatus,
    'test-longdescription' as constraintlongdescription, 
    NULL as constraintdescription,
    NULL as constrainttypeid,
    'Test-C1' as constrainttype,
    '' as constrainttypedescription,
    'description' as _constraint,
    true as hardconstraint)

    ) constraints on iwp.iwpsid = constraints.entityname
