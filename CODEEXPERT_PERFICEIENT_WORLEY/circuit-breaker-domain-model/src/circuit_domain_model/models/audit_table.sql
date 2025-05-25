{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_audit_table/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["audit"]
    ) 
}}

WITH empty_table AS (
    SELECT
        CAST('test_load_id' AS STRING) AS load_id,
        CAST('test_invocation_id' AS STRING) AS invocation_id,
        CAST('test_database_name' AS STRING) AS database_name,
        CAST('test_schema_name' AS STRING) AS schema_name,
        CAST('test_model_name' AS STRING) AS name,
        CAST('test_resource_type' AS STRING) AS resource_type,
        CAST('test_status' AS STRING) AS status,
        CAST(12122012 AS FLOAT) AS execution_time,
        CAST(100 AS INT) AS rows_affected,
        CAST({{ run_date }} AS DATE) AS model_execution_date

)
  
SELECT * FROM empty_table
WHERE 1 = 0
  



