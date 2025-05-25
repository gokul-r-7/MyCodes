{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select distinct 
project_code AS project_id, 
wbs, 
cwa, 
cwpzone,
coalesce(project_code, '') || '_' || coalesce(wbs, '') || '_' || coalesce(cwa, '')
|| '_' || coalesce(cwpzone, '') as wbskey
FROM   {{ source('engineering_dim', 'e3d_wbs_cwa_cwpzone') }}
