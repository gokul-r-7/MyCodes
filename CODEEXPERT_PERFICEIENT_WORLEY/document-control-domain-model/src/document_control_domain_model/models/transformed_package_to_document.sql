{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_package_to_document/',
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

WITH custom_attribute AS
(
    SELECT
        version_sid
        ,normalized_attribute
        ,Replace(REPLACE(REPLACE(attribute_value,'[',''),']',''),'\\\\','') AS attribute_value
        ,dim_project_rls_key
        ,CAST(dbt_ingestion_date AS TIMESTAMP) AS dbt_ingestion_date
        ,CAST(dim_snapshot_date AS TIMESTAMP) AS dim_snapshot_date
    FROM {{ ref('transformed_custom_attribute') }}
    WHERE normalized_attribute IN ("Construction Work Package", "Engineering Work Package")
),

merged AS
(
    SELECT
        documentid
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,current
        ,attribute3_attributetypenames AS merged_source
        ,REPLACE(array_join(from_json(attribute3_attributetypenames, "struct<AttributeTypeName:array<string>>").AttributeTypeName,","),CHR(34), "") as merged_value
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,source_system_name
        ,primary_key
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_docregister_standard') }}
    WHERE attribute3_attributetypenames IS NOT NULL
),

numbers AS
(
    SELECT
        n
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
                MAX(regexp_count(merged_source, CONCAT(CHR(34), ",", CHR(34)))) AS max_num
            FROM 
                merged
        )
    WHERE n <= max_num + 1
),

parsed AS
(

    SELECT 
        *
        ,element_at(split(merged_value, ","), n) AS parsed_value
    FROM 
        merged
    CROSS JOIN
        numbers
    WHERE
        element_at(split(merged_value, ","), n) IS NOT NULL 
        AND element_at(split(merged_value, ","), n) != ""
)

SELECT 
    concat(source_system_name , "|" , documentid) AS version_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,"Requisition" AS package_type
    ,parsed_value AS package_name
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN eff_end_date = "9999-12-31" THEN current_date()
        ELSE eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id 
FROM 
    parsed

UNION ALL

SELECT
    version_sid
    ,normalized_attribute AS package_type
    ,attribute_value AS package_name
    ,dim_project_rls_key
    ,dbt_ingestion_date
    ,dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id 
FROM custom_attribute
