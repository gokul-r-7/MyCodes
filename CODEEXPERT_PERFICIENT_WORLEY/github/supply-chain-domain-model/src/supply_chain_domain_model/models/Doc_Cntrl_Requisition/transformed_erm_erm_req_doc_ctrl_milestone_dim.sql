{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_erm_req_doc_ctrl_milestone_dim/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



WITH cte_req_doc_ctrl_milestone_dim AS (
    SELECT 
        CAST(NULL AS STRING) AS id,  -- Explicitly cast NULL
        project_code,
        CONCAT(project_code, ' - ', milestone, ' - ', milestone_seq) AS milestone,  -- Using CONCAT()
        milestone_id AS milestone_code,
        milestone AS milestone_desc,
        model_created_date AS created_date,
        model_updated_date AS last_updated_date,
        milestone_seq,
        ROW_NUMBER() OVER (PARTITION BY project_code, milestone_id, milestone  
                           ORDER BY COALESCE(model_updated_date, CURRENT_TIMESTAMP) DESC) AS rank,
        {{ run_date }} AS model_created_date,
        {{ run_date }} AS model_updated_date,
        {{ generate_load_id(model) }} AS model_load_id
    FROM {{ ref('transformed_erm_requisition_doc_ctrl') }} 
    WHERE milestone_id IS NOT NULL AND milestone_id <> ''
)

SELECT 
    id,
    project_code,
    milestone,
    milestone_code,
    milestone_desc,
    created_date,
    last_updated_date,
    milestone_seq 
FROM cte_req_doc_ctrl_milestone_dim 
WHERE rank = 1;
