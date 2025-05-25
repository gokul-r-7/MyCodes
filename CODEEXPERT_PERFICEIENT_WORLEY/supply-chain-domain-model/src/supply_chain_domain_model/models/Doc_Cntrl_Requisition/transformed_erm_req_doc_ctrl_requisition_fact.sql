{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='false', 
        custom_location=target.location ~ 'transformed_erm_req_doc_ctrl_requisition_fact/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


WITH cte_req_doc_ctrl_requisition_fact AS (
    SELECT DISTINCT
        CAST(NULL AS STRING) AS id,
        project_code,
        subproject,
        current_revision,
        requisition_title,
        CONCAT(project_code, ' - ', discipline) AS discipline,
        po_number,
        CONCAT(project_code, ' - ', supplier) AS supplier,
        potential_supplier,
        CONCAT(project_code, ' - ', milestone, ' - ', milestone_seq) AS milestone,
        milestone_id,
        milestone_planned_date,
        milestone_forecast_date,
        milestone_actual_date,
        CAST(NULL AS DATE) AS ros_date,
        originator,
        buyer,
        expeditor,
        criticality,
        budget,
        CAST(NULL AS DECIMAL(18,2)) AS commitment_value,
        lead_time,
        milestone_float,
        milestone_duration,
        milestone_comment,
        requisition_no,
        jip33,
        milestone_seq,
        project_region,
        contract_holder,
        contract_owner,
        proj_id,
        cutoff_date,
        model_created_date AS created_date,
        model_updated_date AS last_updated_date,
        comment,
        float
    FROM {{ ref('transformed_erm_requisition_doc_ctrl') }} 
)

SELECT * FROM cte_req_doc_ctrl_requisition_fact