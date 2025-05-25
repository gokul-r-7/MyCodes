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

WITH custom_attribute AS
(
    SELECT
        version_sid
        ,trimmed_attribute
        ,attribute_value
        ,meta_project_rls_key
        ,CAST(meta_ingestion_date AS TIMESTAMP) AS meta_ingestion_date
        ,CAST(meta_snapshot_date AS TIMESTAMP) AS meta_snapshot_date
    FROM {{ ref('sf_custom_attribute') }}
    --FROM "document_control"."domain_integrated_model"."custom_attribute"
    WHERE   meta_project_rls_key = '1207979025' -- Aconex instance ID
            AND meta_journal_current = '1'     -- AWS data journalling current flag
            -- AND SPLIT_PART(normalized_attribute,'_',1) in 
            -- (
            --     'Construction Work Package Zone',
            --     'Engineering Work Package',
            --     'Construction Work Package'
            -- )
),

merged AS
(
    SELECT
        documentid
        ,CAST(projectid AS VARCHAR) AS projectid
        ,current
        ,attribute3_attributetypenames AS merged_source
        ,REPLACE(REPLACE(attribute3_attributetypenames,'{"AttributeTypeName":["',''),'"]}','') as merged_value
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,source_system_name
        ,primary_key
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_docregister_standard') }}
    --FROM "document_control"."curated_aconex"."curated_docregister_standard"
    Where 
        attribute3_attributetypenames IS NOT NULL 
        AND projectid in ('268456382','268452089')
        AND is_current = '1'
),

numbers AS
(
    SELECT
        n::int
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
    source_system_name + '|' + documentid AS version_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,NULL AS package_type
    ,parsed_value AS package_name
-- Datahub standard Meta data fields
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

UNION ALL

select
    version_sid
    ,trimmed_attribute as package_type
    -- ,CASE 
    --     WHEN trim(split_Part(normalized_attribute,'_',1)) = 'Construction Work Package' THEN 'CWP'
    --     WHEN trim(split_Part(normalized_attribute,'_',1)) = 'Engineering Work Package' THEN 'EWP'
    --     WHEN trim(split_Part(normalized_attribute,'_',1)) = 'Construction Work Package Zone' THEN 'CWPZone'
    -- END AS package_type
    ,attribute_value AS package_name
    ,meta_project_rls_key
    ,meta_ingestion_date
    ,meta_snapshot_date
    ,{{run_date}} as created_date
    ,{{run_date}} as updated_date
    ,{{ generate_load_id(model) }} as load_id 
from custom_attribute