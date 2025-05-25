{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_req_doc_ctrl_discipline_dim/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}




WITH cte_req_doc_ctrl_discipline_dim AS (
    SELECT 
        CAST(NULL AS STRING) AS id,
        project_code,
        CONCAT(project_code, ' - ', discipline) AS discipline,
        discipline AS discipline_name,
        model_created_date AS created_date,
        model_updated_date AS last_updated_date,
        ROW_NUMBER() OVER (PARTITION BY project_code, discipline  
                           ORDER BY COALESCE(model_updated_date, CURRENT_TIMESTAMP) DESC) AS rank,
        {{ run_date }} AS model_created_date,
        {{ run_date }} AS model_updated_date,
        {{ generate_load_id(model) }} AS model_load_id
    FROM {{ ref('transformed_erm_requisition_doc_ctrl') }}
    WHERE discipline IS NOT NULL AND discipline <> ''
)

SELECT 
    id,
    project_code,
    discipline,
    discipline_name,
    created_date,
    last_updated_date 
FROM cte_req_doc_ctrl_discipline_dim 
WHERE rank = 1
