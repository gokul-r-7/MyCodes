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
        ,CAST(projectid AS VARCHAR) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM    {{ source('curated_aconex', 'curated_docregister_standard') }}
    --FROM    "document_control"."curated_aconex"."curated_docregister_standard"
    WHERE   --projectid in ('1207979025','268456382','268452089')  -- Aconex instance ID
            current = 'true'     -- Aconex current flag
    AND     is_current = '1' -- AWS data journalling current flag
    AND     eff_end_date = '9999-12-31'      
),

project AS
(
    SELECT 
        CAST(project AS VARCHAR) AS projectid
        ,projectname
    FROM {{ source('curated_aconex', 'curated_project') }}
    --FROM "document_control"."curated_aconex"."curated_project"
)

SELECT DISTINCT
     source_system_name + '|' + trackingid AS document_key
    ,source_system_name + '|' + trackingid AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
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
    ,NULL AS jip33  -- Field not defined in the VG Aconex configuration 
    ,contractdeliverable AS turnover_required
    ,NULL AS native_application
-- Datahub standard Meta data fields
    ,p.projectname AS meta_project_name
    ,d.projectid AS meta_project_rls_key
    ,execution_date AS meta_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN getdate()
        ELSE eff_end_date
    END AS meta_snapshot_date
    ,{{run_date}} as created_date
    ,{{run_date}} as updated_date
    ,{{ generate_load_id(model) }} as load_id
FROM 
    document D
LEFT JOIN project P ON d.projectid = p.projectid
WHERE d.projectid = '1207979025'

UNION ALL

SELECT DISTINCT
    source_system_name + '|' + trackingid AS document_key
    ,source_system_name + '|' + trackingid AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
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
    ,SPLIT_PART(discipline,' - ', 1) AS asset_code  --Asset or area as defined by the project document standards
    ,SPLIT_PART(discipline,' - ', 2) AS asset_name  --Asset or area as defined by the project document standards
    ,NULL AS jip33  -- Field not defined in the VG Aconex configuration 
    ,contractdeliverable AS turnover_required
    ,NULL AS native_application
-- Datahub standard Meta data fields
    ,p.projectname AS meta_project_name
    ,d.projectid AS meta_project_rls_key
    ,execution_date AS meta_ingestion_date
    ,CASE 
        WHEN eff_end_date = '9999-12-31' THEN getdate()
        ELSE eff_end_date
    END AS meta_snapshot_date    
    ,{{run_date}} as created_date
    ,{{run_date}} as updated_date
    ,{{ generate_load_id(model) }} as load_id
FROM 
    document D
LEFT JOIN project P ON d.projectid = p.projectid
WHERE   d.projectid <> '1207979025'
--in ('268456382','268452089')