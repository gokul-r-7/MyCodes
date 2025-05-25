{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transfomed_revision/',
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

WITH revision as 
(
    Select  
        concat(source_system_name, '|', trackingid) AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
        ,concat(source_system_name, '|', MAX(documentid) OVER (PARTITION BY trackingid, revision)) as revision_sid
        ,concat(source_system_name, '|', documentid) AS version_sid 
        ,current 
        ,versionnumber
        ,selectlist1
        ,selectlist2
        ,selectlist4
        ,selectlist5
        ,Selectlist6
        ,Selectlist7
        ,to_timestamp(substring(plannedsubmissiondate, 1, 19), "yyyy-MM-dd'T'HH:mm:ss") as planned_submission_date
        ,revision
        ,to_timestamp(substring(datecreated, 1, 19), "yyyy-MM-dd'T'HH:mm:ss") AS revisiondate
        ,documentstatus
        ,author
        ,reference
        ,CAST(datemodified AS TIMESTAMP) AS datemodified
        ,filename
        ,reviewstatus
        ,reviewsource
        ,modifiedby
        ,comments
        ,title
        ,documenttype    
        ,Category
        ,discipline
        ,vdrcode
        ,contractdeliverable    
        ,ROW_NUMBER() OVER (PARTITION BY projectid,trackingid ORDER BY CAST(versionnumber AS INT) DESC) AS row_num    
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,execution_date
        ,eff_end_date
        ,confidential
    FROM {{ source('curated_aconex', 'curated_docregister_standard') }} 
    --FROM "document_control"."dbt_curated_aconex"."curated_docregister_standard"
    WHERE   
        --projectid in ('1207979025','268456382','268452089','268447877','1879053267','268454994','1342181262','1342181257','1342181165','1342181268','268456867','268456802') -- Aconex instance ID    
        --AND 
        eff_end_date = '9999-12-31' -- AWS data journalling current flag. Need to check if this can be removed as we have is_current included
        AND is_current = '1'     -- AWS data journalling current flag
        
),

hexagon AS 
(
    SELECT 
        CASE WHEN D.trackingid IS NULL THEN concat(h.source_system_name, '|', document_number)
        ELSE concat(D.source_system_name , '|' , D.trackingid) END AS document_sid
        ,H.source_system_name
        ,H.document_number
        ,H.document_revision
        ,H.document_title
        ,H.discipline
        ,H.document_type    
        ,CAST(NULL AS TIMESTAMP) as revisiondate
        ,H.document_revision_state
        ,H.originating_organization
        ,CAST(H.transmittal_creation_date AS TIMESTAMP) AS transmittal_creation_date
        ,H.execution_date
    --FROM "document_control"."dbt_curated_hexagon"."curated_hexagon_ofe" H
    FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }}  H
    --LEFT JOIN "document_control"."dbt_curated_aconex"."curated_docregister_standard" D 
    LEFT JOIN {{ source('curated_aconex', 'curated_docregister_standard') }}  D 
    ON H.document_number = D.documentnumber AND D.projectid = '1207979025'
    WHERE document_number != 'nan'
),

document as 
(
    Select
        documentnumber
        ,revision
        ,trackingid
    --ROM "document_control"."dbt_curated_aconex"."curated_docregister_standard"    
    FROM {{ source('curated_aconex', 'curated_docregister_standard') }}
    WHERE   projectid = '1207979025' -- Aconex instance ID
        AND     eff_end_date = '9999-12-31'-- AWS data journalling current flag. Need to check if this can be removed as we have is_current included
        AND is_current = '1'     -- AWS data journalling current flag
)

SELECT DISTINCT

--Primary and Foreign key Information
    document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,revision_sid
    ,version_sid 

--General Information
    ,CASE WHEN row_num = 1 THEN TRUE ELSE FALSE END AS current --Is this verions flagged as the "Current" in the source system
    ,versionnumber AS version_number
    ,selectlist4 AS sub_project
    ,selectlist5 AS phase
    ,revision AS revision_code
    ,revisiondate AS revision_date --Revision date as per the drawing. Not to be confused with the issued date
    ,cast(next_day(Dateadd(day,-1,revisiondate),'friday') AS TIMESTAMP) AS revision_date_weekend
    ,SPLIT_PART(SPLIT_PART(documentstatus,' - ',1),'_',1) AS status_code
    ,SPLIT_PART(documentstatus,' - ',2) AS status_name
-- Changing fields to track changes changes made as per request on 07-April-2025 from GSR   
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
--Authoring Organization Informatin
    ,author AS originating_organization
    ,CAST(NULL AS  varchar(100)) AS originator_revision_code
    ,reference AS originator  -- The person (engineering/designer) that created or modified the drawing
    ,Selectlist7 AS supplier
    ,planned_submission_date
    ,cast(next_day(Dateadd(day,-1,planned_submission_date),'friday') AS TIMESTAMP) AS planned_submission_date_weekend

--Revision Issue information

    ,datemodified as issued_date
    ,CAST(next_day(Dateadd(day,-1,datemodified),'friday') AS TIMESTAMP ) AS issued_date_weekend
    ,selectlist2 as file_type --Is this the primary file or alternative like native or comments
    ,filename AS file_name
    ,CAST(NULL AS varchar(5000)) AS revision_comment
    ,reviewstatus as review_status
    ,reviewsource as review_source

-- Project Control Reference Information
    ,CAST(NULL AS VARCHAR(100)) AS control_account
    ,CAST(NULL AS VARCHAR(100)) AS deliverable_group
    ,CAST(NULL AS VARCHAR(100)) AS document_control_issue_request_form_number

-- Datahub standard Meta data fields
    ,confidential
    ,projectid AS dim_project_rls_key
    ,to_timestamp(revision.execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
    ,CASE WHEN revision.eff_end_date = '9999-12-31' THEN date_format(current_date(), 'yyyy-MM-dd') ELSE revision.eff_end_date END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM
    revision
Where projectID = '1207979025' 

UNION ALL

    SELECT DISTINCT
--Primary and Foreign key Information
    H.document_sid
    ,concat(H.source_system_name , '|' , H.document_number , '|' , H.document_revision) AS revision_sid
    ,concat(H.source_system_name , '|' , H.document_number , '|' , H.document_revision) AS version_sid
--General Information    
    ,true AS current  
    ,CAST(NULL as int) AS version_number
    ,CAST(NULL AS VARCHAR(100)) AS sub_project
    ,CAST(NULL AS VARCHAR(100)) AS phase
    ,document_revision as revision_code
    ,revisiondate AS revision_date
    ,cast(next_day(Dateadd(day,-1,revisiondate),'friday') AS TIMESTAMP) AS revision_date_weekend
    ,document_revision_state AS status_code
    ,CAST(NULL AS VARCHAR(100)) AS status_name
-- Changing fields to track changes changes made as per request on 07-April-2025 from GSR
    ,H.document_title as title
    ,'Owner Furnished Equipment' AS category
    ,H.discipline as discipline_code
    ,CAST(NULL AS VARCHAR(200)) AS discipline_name
    ,H.document_type as type_code
    ,CAST(NULL AS VARCHAR(200)) AS type_name
    ,CAST(NULL AS VARCHAR(200)) ASasset_code
    ,CAST(NULL AS VARCHAR(200)) AS asset_name
    ,CAST(NULL AS VARCHAR(200)) AS jip33
    ,false AS turnover_required    
--Authoring Organization Information
    ,originating_organization
    ,CAST(NULL AS VARCHAR(100)) AS originator_revision_code
    ,CAST(NULL AS VARCHAR(500)) AS originator
    ,originating_organization as supplier
    ,CAST(NULL as TIMESTAMP) AS planned_submission_date
    ,CAST(NULL as TIMESTAMP) AS planned_submission_date_weekend

--Revision Issue information
    ,transmittal_creation_date AS issued_date
    ,cast(next_day(Dateadd(day,-1,transmittal_creation_date),'friday') AS TIMESTAMP) AS issued_date_weekend
    ,'primary' AS file_type
    ,CAST(NULL AS VARCHAR(500)) as file_name
    ,CAST(NULL AS VARCHAR(5000)) as revision_comment
    ,'Review' as review_status
    ,'Hexagon' as review_source

-- Project Control Reference Information
    ,CAST(NULL AS VARCHAR(100)) AS control_account
    ,CAST(NULL AS VARCHAR(100)) AS deliverable_group
    ,CAST(NULL AS VARCHAR(100)) AS document_control_issue_request_form_number
    ,false as confidential
-- Datahub standard Meta data fields
    ,'1207979025' AS dim_project_rls_key
    ,to_timestamp(H.execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
    ,date_format(to_date(H.execution_date), 'yyyy-MM-dd') AS dim_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM hexagon H
LEFT JOIN document D ON H.document_number = D.documentnumber AND H.document_revision = D.revision
WHERE D.revision IS NULL

UNION ALL

SELECT DISTINCT

--Primary and Foreign key Information
    document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,revision_sid
    ,version_sid 

--General Information
    ,CASE WHEN row_num = 1 THEN TRUE ELSE FALSE END AS current --Is this verions flagged as the "Current" in the source system
    ,versionnumber AS version_number
    ,CAST(NULL AS VARCHAR(100)) AS sub_project
    ,selectlist5 AS phase
    ,revision AS revision_code
    ,revisiondate AS revision_date --Revision date as per the drawing. Not to be confused with the issued date
    ,cast(next_day(Dateadd(day,-1,revisiondate),'friday') AS TIMESTAMP) AS revision_date_weekend   
    ,SPLIT_PART(SPLIT_PART(documentstatus,' - ',1),'_',1) AS status_code
    ,SPLIT_PART(documentstatus,' - ',2) AS status_name
-- Changing fields to track changes changes made as per request on 07-April-2025 from GSR
    ,title --The title of the document as shown on the document
    ,documenttype AS category --Document Category (Drawing, Engineering and Design, Supplier Data, etc.)
    -- ,SPLIT_PART(vdrcode,' - ', 1) AS discipline_code --Discipline code as per project document standards
    -- ,SPLIT_PART(vdrcode,' - ', 2) AS discipline_name  --Discipline code as per project document standards
    ,SPLIT_PART(discipline,' - ', 1) AS discipline_code --Discipline code as per new project config
    ,SPLIT_PART(discipline,' - ', 2) AS discipline_name  --Discipline code as per new project config    
    ,SPLIT_PART(Category,' - ', 1) AS type_code  --Document type code as per project document standards
    ,SPLIT_PART(category,' - ', 2) AS type_name  --Document type as per project document standards
    -- ,SPLIT_PART(selectlist1,' - ', 1) AS asset_code  --Asset or area as defined by the project document standards
    -- ,SPLIT_PART(selectlist1,' - ', 2) AS asset_name  --Asset or area as defined by the project document standards
    ,SPLIT_PART(vdrcode,' - ', 1) AS asset_code  --Asset or area as defined as per new project config
    ,SPLIT_PART(vdrcode,' - ', 2) AS asset_name  --Asset or area as defined as per new project config
    ,CAST(NULL AS VARCHAR(100)) AS jip33  -- Field not defined in the VG Aconex configuration 
    ,contractdeliverable AS turnover_required    
--Authoring Organization Informatin
    ,author AS originating_organization
    ,CAST(NULL AS VARCHAR(100)) AS originator_revision_code
    ,reference AS originator  -- The person (engineering/designer) that created or modified the drawing
    ,selectlist7 as supplier
    ,planned_submission_date AS planned_submission_date
    ,cast(next_day(Dateadd(day,-1,planned_submission_date),'friday') AS TIMESTAMP) AS planned_submission_date_weekend

--Revision Issue information

    ,datemodified as issued_date
    ,cast(next_day(Dateadd(day,-1,datemodified),'friday') AS TIMESTAMP) AS issued_date_weekend

    ,selectlist2 as file_type --Is this the primary file or alternative like native or comments
    ,filename AS file_name
    ,comments AS revision_comment
    ,reviewstatus as review_status
    ,reviewsource as review_source

-- Project Control Reference Information
    ,Selectlist6 AS control_account
    ,CAST(NULL AS VARCHAR(100)) AS deliverable_group
    ,CAST(NULL AS VARCHAR(100)) AS document_control_issue_request_form_number
    ,confidential
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,to_timestamp(revision.execution_date, 'yyyy-MM-dd HH:mm:ss') AS dbt_ingestion_date
    ,CASE WHEN revision.eff_end_date = '9999-12-31' THEN date_format(current_date(), 'yyyy-MM-dd') ELSE revision.eff_end_date END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM
    revision
--WHERE projectid in ('268456382','268452089','268447877','1879053267','268454994','1342181262','1342181257','1342181165','1342181268','268456867','268456802')
WHERE projectid not in ('1207979025')
