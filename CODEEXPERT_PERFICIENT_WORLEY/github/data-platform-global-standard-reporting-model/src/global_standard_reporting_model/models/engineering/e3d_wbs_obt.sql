{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select distinct 
cast(project_code as varchar(100)) AS project_id,
wbs, 
cwa, 
cwpzone,
coalesce(project_code, '') || '_' || coalesce(wbs, '') || '_' || coalesce(cwa, '')
|| '_' || coalesce(cwpzone, '') as wbskey
FROM   {{ source('engineering_dim', 'transformed_e3d_wbs_cwa_cwpzone') }}
