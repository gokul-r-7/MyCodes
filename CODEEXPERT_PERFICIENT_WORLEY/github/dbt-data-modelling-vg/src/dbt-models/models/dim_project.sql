{{
    config(
        materialized='incremental'
    )
}}

SELECT
    distinct
    id
    ,project_code
    ,projectname
    ,execution_date
FROM
   {{ source('curated_vg_e3d', 'curated_dim_project') }}

   {% if is_incremental() %}
     where execution_date >= (select coalesce(max(execution_date),'1900-01-01') from {{ this }} )
   {% endif %}