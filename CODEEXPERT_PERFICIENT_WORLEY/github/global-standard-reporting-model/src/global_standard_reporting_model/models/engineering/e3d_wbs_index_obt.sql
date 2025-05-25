{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select DISTINCT wbs,
project_code as project_id,
project_code || '_' || wbs  as Project_WBS,
DENSE_RANK() OVER(order by project_code,wbs) AS Id
FROM  {{ source('engineering_dim', 'e3d_pipes') }}
where wbs <> ''
ORDER BY project_code,wbs