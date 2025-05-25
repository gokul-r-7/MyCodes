{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}

select 
iwp.updated_date as iwp_updated_date,
iwp.projectid as iwp_project_id,
cwp.cwpsid as cwp_cwpsid,
iwp.iwpsid as iwp_iwpsid,
iwp.status as iwp_status,
cast(iwp.estimatedhours as numeric) as iwp_estimatedhours,
cast(iwp.earnedhours as numeric) as iwp_earnedhours,
iwp.percentcomplete as iwp_percentcomplete,
iwp.actualfinishdate as iwp_actualfinishdate,
cast(iwp.plannedstartdate as DATE) as iwp_plannedstartdate, 

case when (iwp_actualfinishdate is not null and iwp_actualfinishdate!='') then
    case when (iwp_earnedhours < iwp_estimatedhours) OR 
    ((iwp_earnedhours < iwp_estimatedhours) and iwp_percentcomplete='100') OR
    (iwp_percentcomplete is null or iwp_percentcomplete='' or iwp_percentcomplete=0)
    then true 
    else NULL
    end 
end as iwps_with_actualfinishdate_without_fullprogress,

constraintid,
constrainttypedescription,
_constraint,
hardconstraint,
constrainttypeid,
constrainttype,
case when hardconstraint='true' and (constrainttype is not null and constrainttype != '') then 1
else NULL end as constrainttype_count

from
(select 
updated_date,
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
from {{ source('domain_integrated_model', 'o3_iwps') }}

union all
(select 
'2025-01-02' as updated_date,
'TEST1' as iwpsid,
'test' as cwpsid,
'41' as projectid, 
'In Progress' as status,
'2025-01-16' as actualfinishdate,
'100' as percentcomplete,
'100' as estimatedhours,
'10' as earnedhours,
'50' as actualduration,
'XYZ' as superintendent,
'2024-12-24' as plannedstartdate,
'2025-01-15' as plannedfinishdate)

union all
(select 
'2025-01-03' as updated_date,
'TEST2' as iwpsid,
'test' as cwpsid,
'41' as projectid, 
'In Progress' as status, 
'2025-02-03' as actualfinishdate,
'70' as percentcomplete,
'200' as estimatedhours,
'5' as earnedhours,
'25' as actualduration,
'ABC' as superintendent,
'2025-01-06' as plannedstartdate,
'2025-02-05' as plannedfinishdate)) iwp

inner join 
(select 
cwpsid,
cwpstatus,
cwpsdescription,
contract from {{ source('domain_integrated_model', 'o3_cwps') }}
union all
(select
'test'as cwpsid,
'In Development' as cwpstatus,
'Manual test case' as cwpsdescription,
'WORLEY' as contract)) cwp on iwp.cwpsid=cwp.cwpsid

left join
((select 
    entityid,
    entitytypeid,
    entityname,
    projectid,
    constraintid,
    constrainttypeid,
    constrainttype as constraints_constrainttype,
    constrainttypedescription,
    _constraint,
    hardconstraint
    from {{ source('domain_integrated_model', 'o3_constraints') }} where entitytypeid = '4') constraints
    left join 

    (select 
    constrainttypeid as new_typeid,
    constrainttype
    from {{ source('domain_integrated_model', 'o3_constraint_type') }}) constraint_type
    on constraints.constrainttypeid = constraint_type.new_typeid
) constraint_joined on iwp.iwpsid = constraint_joined.entityname
