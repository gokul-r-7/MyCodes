{{
     config(

         materialized = "table",
         tags=["construction"]
         )

}}


select  41 as Projectid,MIN(cast(plannedfinishdate as date))-14 As start_date,max(cast(plannedfinishdate as date))+14 End_Date
from {{ source('Project_control_domain_integrated_model', 'p6_activity') }}
where name like '%CWA%'
union ALL
select  42 as Projectid,MIN(cast(plannedfinishdate as date))-14 As start_date,max(cast(plannedfinishdate as date))+14 End_Date
from {{ source('Project_control_domain_integrated_model', 'p6_activity') }}
where name like '%CWA%'
