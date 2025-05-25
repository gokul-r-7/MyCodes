{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

SELECT *,
cast(project_code as varchar(100)) AS project_id
FROM {{ source('engineering_dim', 'transformed_dim_project') }}