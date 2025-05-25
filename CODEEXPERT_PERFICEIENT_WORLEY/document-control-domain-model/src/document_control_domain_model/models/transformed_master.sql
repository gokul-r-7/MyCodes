{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_master/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

/*

SQL Query Title: Venture Global Custom Mapping to Worley Global Data Model
Domain: Document Control
Data Governance Confidentiality Level:
Project Name: Venture Global CP2
*/

WITH document AS 
(
    Select
        source_system_name
        ,trackingid
        ,documentnumber
        ,vendordocumentnumber
        ,title
        ,documenttype
        ,vdrcode
        ,Category
        ,selectlist1
        ,contractdeliverable
        ,discipline
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
        ,ROW_NUMBER() OVER (PARTITION BY projectid,trackingid ORDER BY CAST(versionnumber AS INT) DESC) AS row_num
    FROM    {{ source('curated_aconex', 'curated_docregister_standard') }}
    --FROM    "document_control"."dbt_curated_aconex"."curated_docregister_standard"
    --WHERE   projectid in ('1207979025','268456382','268452089','268447877','1879053267','268454994','1342181262','1342181257','1342181165','1342181268', '268456867', '268456802')  -- Aconex instance ID
    WHERE     current = 'true'     -- Aconex current flag
    AND     is_current = '1' -- AWS data journalling current flag
    AND     eff_end_date = '9999-12-31'      
),

project AS
(
    SELECT 
        CAST(project AS VARCHAR(100)) AS projectid
        ,projectname
    FROM {{ source('curated_aconex', 'curated_project') }}
    --FROM "document_control"."dbt_curated_aconex"."curated_project"
    WHERE is_current = '1'
),

hexagon AS 
(
    SELECT 
        source_system_name
        ,document_number
        ,document_title
        ,discipline
        ,document_type
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(1207979025 AS VARCHAR(100)) AS projectid

    FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }} 
    --FROM "document_control"."dbt_curated_hexagon"."curated_hexagon_ofe"
    WHERE 
        document_number != 'nan'
)

SELECT DISTINCT
    concat(source_system_name , '|' , trackingid) AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,documentnumber AS document_number --The unique identifier for the Document according to the Owner/Operator (Client) Document numbering scheme.  CFIHOS Compliant ID CFIHOS-10000154
    ,CASE
        WHEN vendordocumentnumber IS NULL 
        THEN documentnumber ELSE vendordocumentnumber
    END AS originator_number --The unique identifier for the Document according to the originator (Worley, Supplier, etc) document numbering scheme.  CFIHOS Compliant ID CFIHOS-10000160
    ,title --The title of the document as shown on the document
    ,documenttype AS category --Document Category (Drawing, Engineering and Design, Supplier Data, etc.)
    ,SPLIT_PART(vdrcode,' - ', 1) AS discipline_code --Discipline code as per project document standards
    ,SPLIT_PART(vdrcode,' - ', 2) AS discipline_name  --Discipline code as per project document standards
    ,SPLIT_PART(Category,' - ', 1) AS type_code  --Document type code as per project document standards
    ,SPLIT_PART(category,' - ', 2) AS type_name  --Document type as per project document standards
    ,SPLIT_PART(selectlist1,' - ', 1) AS asset_code  --Asset or area as defined by the project document standards
    ,SPLIT_PART(selectlist1,' - ', 2) AS asset_name  --Asset or area as defined by the project document standards
    ,CAST(NULL AS VARCHAR(100)) AS jip33  -- Field not defined in the VG Aconex configuration 
    ,contractdeliverable AS turnover_required
    ,CAST(source_system_name AS VARCHAR(100)) AS native_application


-- Datahub standard Meta data fields
    ,p.projectname AS dim_project_name
    ,d.projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN current_date()
        ELSE eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM 
    document D
LEFT JOIN project P ON d.projectid = p.projectid
WHERE d.projectid = '1207979025'

UNION ALL

SELECT DISTINCT

    concat(h.source_system_name , '|' , document_number) AS document_sid
    ,document_number AS document_number
    ,CAST(NULL AS VARCHAR(200)) AS originator_number
    ,document_title as title
    ,'Owner Furnished Equipment' AS category
    ,h.discipline as discipline_code
    ,CAST(NULL AS VARCHAR(200)) AS discipline_name
    ,document_type as type_code
    ,CAST(NULL AS VARCHAR(200)) AS type_name
    ,CAST(NULL AS VARCHAR(200)) ASasset_code
    ,CAST(NULL AS VARCHAR(200)) AS asset_name
    ,CAST(NULL AS VARCHAR(200)) AS jip33
    ,false AS turnover_required
    ,CAST(h.source_system_name AS VARCHAR(100)) native_application

-- Datahub standard Meta data fields
    ,p.projectname AS dim_project_name
    ,h.projectid AS dim_project_rls_key
    ,H.execution_date AS dbt_ingestion_date
    ,H.execution_date AS dim_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM hexagon H
LEFT JOIN project P ON h.projectid = p.projectid
LEFT JOIN document D ON h.document_number = d.documentnumber AND D.projectid = '1207979025'
WHERE d.trackingid IS NULL

UNION ALL


SELECT DISTINCT

    concat(source_system_name , '|' , trackingid) AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,documentnumber AS document_number --The unique identifier for the Document according to the Owner/Operator (Client) Document numbering scheme.  CFIHOS Compliant ID CFIHOS-10000154
    ,CASE
        WHEN vendordocumentnumber IS NULL 
        THEN documentnumber ELSE vendordocumentnumber
    END AS originator_number --The unique identifier for the Document according to the originator (Worley, Supplier, etc) document numbering scheme.  CFIHOS Compliant ID CFIHOS-10000160
    ,title --The title of the document as shown on the document
    ,documenttype AS category --Document Category (Drawing, Engineering and Design, Supplier Data, etc.)
    --,SPLIT_PART(vdrcode,' - ', 1) AS discipline_code --Discipline code as per project document standards 
    --,SPLIT_PART(vdrcode,' - ', 2) AS discipline_name  --Discipline code as per project document standards
    ,SPLIT_PART(discipline,' - ', 1) AS discipline_code --Discipline code as per new aconex project config for non vg
    ,SPLIT_PART(discipline,' - ', 2) AS discipline_name  --Discipline code as per new aconex project config for non vg   
    ,SPLIT_PART(Category,' - ', 1) AS type_code  --Document type code as per project document standards
    ,SPLIT_PART(category,' - ', 2) AS type_name  --Document type as per project document standards
    --,SPLIT_PART(discipline,' - ', 1) AS asset_code  --Asset or area as defined by the project document standards
    --,SPLIT_PART(discipline,' - ', 2) AS asset_name  --Asset or area as defined by the project document standards
    ,SPLIT_PART(vdrcode,' - ', 1) AS asset_code  --Asset or area as defined as per new aconex project config for non vg
    ,SPLIT_PART(vdrcode,' - ', 2) AS asset_name  --Asset or area as defined as per new aconex project config for non vg   
    ,CAST(NULL AS VARCHAR(100)) AS jip33  -- Field not defined in the VG Aconex configuration 
    ,contractdeliverable AS turnover_required
    ,CAST(source_system_name AS VARCHAR(100)) AS native_application


-- Datahub standard Meta data fields
    ,p.projectname AS dim_project_name
    ,d.projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN current_date()
        ELSE eff_end_date
    END AS dim_snapshot_date    
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM 
    document D
LEFT JOIN project P ON d.projectid = p.projectid
--WHERE   d.projectid in ('268456382','268452089','268447877','1879053267','268454994','1342181262','1342181257','1342181165','1342181268', '268456867', '268456802')
WHERE D.projectid not in ('1207979025')
AND D.row_num = 1
