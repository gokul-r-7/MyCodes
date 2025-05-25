{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
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
        MAX(REGEXP_COUNT(Customattributes,'","')) OVER () as max_splits
    FROM {{ source('curated_aconex', 'curated_docregister_custom') }}  
    --FROM "document_control"."curated_aconex"."curated_docregister_custom"
    WHERE customattributes != '{}' 
    AND eff_end_date = '9999-12-31'
    and projectID = '1207979025'
),

numbers AS (
    WITH max_split AS (
        SELECT max_splits + 1 as max_count 
        FROM curated_table 
        LIMIT 1
    ),
    sequence AS (
        SELECT ROW_NUMBER() OVER (ORDER BY TRUE)::INT as n
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
        projectid::VARCHAR AS projectid,
        execution_date::TIMESTAMP AS execution_date,
        source_system_name,
        --REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(Customattributes, '[\{\}]', ''), '[\[\]]', ''), '\\"', '') AS cust_attributes,
        REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(Customattributes,'\\n',','), '[\{\}]', ''), '[[\\]', ''),'","','||'), '\\"', ''),']','') AS cust_attributes,
        is_current,
        CAST(eff_end_date AS TIMESTAMP) AS eff_end_date,
        source_system_name || '|' || documentid AS version_sid
    FROM curated_table
),

parsed_attributes AS (
    SELECT 
        ca.*,
        TRIM(SPLIT_PART(ca.cust_attributes::VARCHAR, '||'::VARCHAR, n::INT)) as custom_attribute
    FROM custom_attributes ca
    CROSS JOIN numbers n
    WHERE n <= REGEXP_COUNT(ca.cust_attributes, '||') + 1
    AND cust_attributes IS NOT NULL
    AND TRIM(SPLIT_PART(ca.cust_attributes::VARCHAR, '||'::VARCHAR, n::INT)) != ''
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

select
    version_sid || '|' || attribute AS version_key,
    version_sid,
    trimmed_attribute,
    CASE 
        WHEN trimmed_attribute in ('WbsCode') THEN 'Work Brakedown Structure'
        WHEN trimmed_attribute in ('CwpNumber') THEN 'Construction Work Package'
        WHEN trimmed_attribute in ('Ewp','EwpNumber') THEN 'Engineering Work Package'
        WHEN trimmed_attribute in ('CwpZone') THEN 'Construction Work Package Zone'         
    ELSE NULL END AS normalized_attribute,
    --Replace(TRIM(SPLIT_PART(attribute_values,',',n::INT)),'"','') AS attribute_value,
    attribute_values as attribute_value,
    is_current AS meta_journal_current, 
    projectid AS meta_project_rls_key,
    CAST(execution_date as TIMESTAMP) AS meta_ingestion_date,
    getdate() AS meta_snapshot_date,
    {{run_date}} as dbt_created_date,
    {{run_date}} as dbt_updated_date,
    {{ generate_load_id(model) }} as dbt_load_id
from (
    select 
        *,
        TRIM(SPLIT_PART(attribute,'_',1)) as trimmed_attribute
    from attributes
) a
-- CROSS JOIN numbers
-- WHERE NULLIF(SPLIT_PART(attribute_values,',',n::INT), '') IS NOT NULL