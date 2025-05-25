{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_sdr_to_document/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

/*

SQL Query Title: Global Standard Model - Aconex Tag To Document cross reference table
Domain: Document Management
Data Governance Confidentiality Level:
Project Name: Global Standard
   
*/

WITH 
merged AS
(
    SELECT
        trackingid
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,current
        ,attribute2_attributetypenames AS merged_source
        ,REPLACE(REPLACE(attribute4_attributetypenames,'{"AttributeTypeName":["',''),'"]}','') as merged_value
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,source_system_name
        ,primary_key
        ,is_current
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM  {{ source('curated_aconex', 'curated_docregister_standard') }}
    --FROM "document_control"."dbt_curated_aconex"."curated_docregister_standard"
    Where 
        attribute4_attributetypenames IS NOT NULL
        AND eff_end_date = '9999-12-31'
),

numbers AS
(
    SELECT
        CAST(n AS INTEGER)
    FROM
        (
            SELECT 
                row_number() over (ORDER BY true) AS n 
            FROM 
                merged
        )
    CROSS JOIN
        (
            SELECT 
                MAX(regexp_count(merged_source,'","')) AS max_num 
            FROM 
                merged
        )
    WHERE n <= max_num + 1
),

parsed AS
(

    SELECT 
        *
        ,SPLIT_PART(merged_value,'","',n) AS parsed_value
    FROM 
        merged
    CROSS JOIN
        numbers
    WHERE
        SPLIT_PART(merged_value,'","',n) IS NOT NULL
        AND SPLIT_PART(merged_value,'","',n) != '' 
)

SELECT 

    concat(source_system_name,'|', trackingid) AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,split_part(parsed_value,' - ',1) AS sdr_code
    ,split_part(parsed_value,' - ',2) AS sdr_name
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN current_date()
        ELSE eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM 
    parsed
