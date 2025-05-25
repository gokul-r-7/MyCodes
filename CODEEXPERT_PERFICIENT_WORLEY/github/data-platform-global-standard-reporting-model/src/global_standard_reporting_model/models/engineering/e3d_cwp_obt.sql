{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}


    
select
    distinct cwp1 as cwp,
    cast(project_code as varchar(100)) AS project_id,
    coalesce(project_code, '') || '_' || coalesce(cwp1, '')  as cwpkey
from
    {{ source('engineering_dim', 'transformed_e3d_pipes') }} 
where
    extracted_date in (
        select
        top
            90 distinct extracted_date
        from
            {{ source('engineering_dim', 'transformed_e3d_pipes') }} 
        order by
            extracted_date desc
    )
