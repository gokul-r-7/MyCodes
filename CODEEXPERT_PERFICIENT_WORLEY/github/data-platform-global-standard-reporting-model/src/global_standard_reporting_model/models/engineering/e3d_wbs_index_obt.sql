{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

select DISTINCT wbs,
cast(project_code as varchar(100)) AS project_id,
project_code || '_' || wbs  as Project_WBS,
DENSE_RANK() OVER(order by project_code,wbs) AS Id
FROM  {{ source('engineering_dim', 'transformed_e3d_pipes') }}
where wbs <> ''
ORDER BY project_code,wbs