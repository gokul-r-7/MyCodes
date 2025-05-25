
{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}


/*

SQL Query Title: Global Standard Model - Aconex Tag To Document cross reference table
Domain: Document Management
Data Governance Confidentiality Level:
Project Name: Global Standard Snowflake
   
*/

WITH 
merged AS
(
    SELECT
        trackingid
        ,CAST(projectid AS VARCHAR) AS projectid
        ,current
        ,attribute4_attributetypenames AS merged_source
        ,REPLACE(REPLACE(attribute4_attributetypenames,'{"AttributeTypeName":["',''),'"]}','') as merged_value
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,source_system_name
        ,primary_key
        ,is_current
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM  {{ source('curated_aconex', 'curated_docregister_standard') }}
    --FROM "document_control"."curated_aconex"."curated_docregister_standard"
     
    Where 
        attribute4_attributetypenames IS NOT NULL
        AND eff_end_date = '9999-12-31'
        AND current = 'true'
        AND is_current = '1'
),

-- numbers AS
-- (
--     SELECT
--         n::int
--     FROM
--         (
--             SELECT 
--                 row_number() over (ORDER BY true) AS n 
--             FROM 
--                 merged
--         )
--     CROSS JOIN
--         (
--             SELECT 
--                 MAX(regexp_count(merged_source,'","')) AS max_num 
--             FROM 
--                 merged
--         )
--     WHERE n <= max_num + 1
-- ),

parsed AS
(

    SELECT 
        *
        --,SPLIT_PART(merged_value,'","',n) AS parsed_value
        ,replace(merged_value,'","',',') AS parsed_value
    FROM 
        merged
    -- CROSS JOIN
    --     numbers
    -- WHERE
    --     SPLIT_PART(merged_value,'","',n) IS NOT NULL
    --     AND SPLIT_PART(merged_value,'","',n) != '' 
)

select
    source_system_name + '|' + trackingid AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,parsed_value AS sdr_code
    -- ,split_part(parsed_value,' - ',1) AS sdr_code
    -- ,split_part(parsed_value,' - ',2) AS sdr_name
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN getdate()
        ELSE eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
from 
    parsed