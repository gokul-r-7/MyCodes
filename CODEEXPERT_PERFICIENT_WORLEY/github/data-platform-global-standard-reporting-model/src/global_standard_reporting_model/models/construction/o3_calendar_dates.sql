{{

     config( 
        materialized = "table",
        tags=["construction"]
        )

}}

select cast(projectid as varchar(100)) as project_id,
min(cast(plannedstartdate as date)) as Min_Start, 
max(cast(plannedfinishdate as date)) as Max_Finish 
from {{source('domain_integrated_model', 'transformed_cwps')}} where projectid is not null group by projectid