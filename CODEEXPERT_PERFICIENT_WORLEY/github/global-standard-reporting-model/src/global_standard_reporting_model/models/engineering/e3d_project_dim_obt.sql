{{
     config(
         materialized = "table",
         tags=["engineering"]
     )

}}

SELECT *,
project_code AS project_id
FROM {{ source('engineering_dim', 'dim_project') }}