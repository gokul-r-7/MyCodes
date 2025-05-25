{{

     config( 
        materialized = "table",
        tags=["construction"]
        )

}}

select 
'IWP' as source,
cast(projectid as varchar(100)) as project_id,
max(cast(execution_date as date))  as refresh_date
from {{source('domain_integrated_model', 'transformed_iwps')}} where projectid is not null group by projectid