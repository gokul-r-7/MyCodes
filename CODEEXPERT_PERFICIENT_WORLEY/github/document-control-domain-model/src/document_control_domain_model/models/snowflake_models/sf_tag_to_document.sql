

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
Project Name: Global Standard
   
*/

WITH 
merged AS
(
    SELECT
        trackingid
        ,CAST(projectid AS VARCHAR) AS projectid
        ,current
        ,attribute2_attributetypenames AS merged_source
        ,REPLACE(REPLACE(attribute2_attributetypenames,'{"AttributeTypeName":["',''),'"]}','') as merged_value
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,source_system_name
        ,primary_key
        ,is_current
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM
           {{ source('curated_aconex', 'curated_docregister_standard') }} 
    Where 
        attribute2_attributetypenames IS NOT NULL
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

SELECT 
     source_system_name + '|' + trackingid AS document_key
    ,source_system_name + '|' + trackingid AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,parsed_value AS tag_number
    ,current
-- Datahub standard Meta data fields
    ,is_current as meta_journal_current
    ,projectid AS meta_project_rls_key
    ,execution_date AS meta_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN getdate()
        ELSE eff_end_date
    END AS meta_snapshot_date
    ,{{run_date}} as created_date
    ,{{run_date}} as updated_date
    ,{{ generate_load_id(model) }} as load_id
FROM 
    parsed
