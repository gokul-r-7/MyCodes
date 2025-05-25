{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_custom_attribute/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

/*

SQL Query Title: Venture Global Custom Mapping to Worley Global Data Model
Domain: Document Management
Data Governance Confidentiality Level:
Project Name: Venture Global CP2

*/

WITH curated_table AS (
    Select *,
    MAX(REGEXP_COUNT(Customattributes,'"," ')) OVER () as max_splits
    FROM {{ source('curated_aconex', 'curated_docregister_custom') }}  
    --FROM "document_control"."dbt_curated_aconex"."curated_docregister_custom"
    WHERE customattributes != '{}' 
    AND eff_end_date = '9999-12-31'
),

numbers AS (
    WITH max_split AS (
        SELECT max_splits + 1 as max_count 
        FROM curated_table 
        LIMIT 1
    ),
    sequence AS (
        SELECT ROW_NUMBER() OVER (ORDER BY TRUE) as n
        FROM curated_table
    )
    SELECT n 
    FROM sequence, max_split
    WHERE n <= max_count
    LIMIT 100
),

custom_attributes as (   
    SELECT 
        documentid,
        CAST(projectid as VARCHAR(100)) AS projectid,
        cast(execution_date AS TIMESTAMP) AS execution_date,
        source_system_name,
        --REGEXP_REPLACE(REGEXP_REPLACE(Customattributes, '[{}[\]]', ''), '"', '') AS cust_attributes,
        REGEXP_REPLACE(REPLACE(
            REPLACE(
                REGEXP_REPLACE(Customattributes, '[{}]', ''), 
                '[', ''),
                    ']', ''),
                    '"', '') AS cust_attributes,
        --REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(Customattributes, '\\{|\\}', ''), '\\[|\\]', ''),'"', '') AS cust_attributes,
        is_current,
        CAST(eff_end_date AS TIMESTAMP) AS eff_end_date,
        concat(source_system_name, '|' , documentid) AS version_sid
    FROM curated_table
),

parsed_attributes AS (
    SELECT 
        ca.*,
        TRIM(SPLIT_PART(ca.cust_attributes, '","', n)) as custom_attribute
    FROM custom_attributes ca
    CROSS JOIN numbers n
    WHERE n <= REGEXP_COUNT(ca.cust_attributes, '","') + 1
    AND cust_attributes IS NOT NULL
    AND TRIM(SPLIT_PART(ca.cust_attributes, '","', n)) != ''
),

attributes AS (
    SELECT 
        version_sid,
        custom_attribute as attribute,
        SPLIT_PART(custom_attribute, ':', 2) as attribute_values,
        projectid,
        is_current,
        execution_date,
        eff_end_date
    FROM parsed_attributes
)

SELECT

    version_sid,
    trimmed_attribute,
    CASE 
        WHEN trimmed_attribute in ('WbsCode') THEN 'Work Brakedown Structure'
        WHEN trimmed_attribute in ('CwpNumber','CwpNo','CwpNumber') THEN 'Construction Work Package'
        WHEN trimmed_attribute in ('Ewp','EwpNo','EwpNumber') THEN 'Engineering Work Package'         
    ELSE NULL END AS normalized_attribute,
    REPLACE(TRIM(SPLIT_PART(attribute_values,',',n)),'"','') AS attribute_value,

--metadata fields
    --DIM
    projectid AS dim_project_rls_key,
    current_timestamp() AS dim_snapshot_date,
    --DBT
    concat(version_sid , '|' , attribute) AS dbt_version_key, 
    CAST(execution_date as TIMESTAMP) AS dbt_ingestion_date,
    {{run_date}} as dbt_created_date,
    {{run_date}} as dbt_updated_date,
    {{ generate_load_id(model) }} as dbt_load_id
FROM (
    SELECT 
        *,
        TRIM(SPLIT_PART(attribute,'_',1)) as trimmed_attribute
    FROM attributes
) a
CROSS JOIN numbers
WHERE NULLIF(SPLIT_PART(attribute_values,',',n), '') IS NOT NULL

