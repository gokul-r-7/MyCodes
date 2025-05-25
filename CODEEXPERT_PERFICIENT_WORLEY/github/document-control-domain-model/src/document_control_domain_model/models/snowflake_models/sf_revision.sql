
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
WITH revision as 
(
    Select  
        source_system_name
        ,documentid
        ,trackingid
        ,CASE WHEN is_current = 1 then true else false end as current 
        ,versionnumber
        ,selectlist2
        ,selectlist4
        ,selectlist5
        ,Selectlist6
        ,revision
        --,CAST(datecreated AS TIMESTAMP) AS revisiondate
        ,cast(
            case
                when datecreated = '' then null
                else datecreated
            end as timestamp 
        ) as revisiondate
        ,documentstatus
        ,author
        ,reference
        ,CAST(datemodified AS TIMESTAMP) AS datemodified
        ,filename
        ,reviewstatus
        ,reviewsource
        ,modifiedby
        ,comments
        ,plannedsubmissiondate AS plannedsubmissiondate
        ,milestonedate AS milestonedate
        ,selectlist8 AS purchaseordernumber
        ,check1 AS check1
        ,CAST(projectid AS VARCHAR) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_docregister_standard') }} 
    --FROM "document_control"."curated_aconex"."curated_docregister_standard"
    WHERE eff_end_date = '9999-12-31' 
        --projectid in ('1207979025','268456382','268452089') -- Aconex instance ID    
        -- AWS data journalling current flag
)

-- hexagon AS 
-- (
--     SELECT 
--         CASE WHEN D.trackingid IS NULL THEN h.source_system_name + '|' + document_number
--         ELSE D.source_system_name + '|' + D.trackingid END AS document_sid
--         ,H.source_system_name
--         ,H.document_number
--         ,H.document_revision
--         ,NULL::TIMESTAMP as revisiondate
--         ,H.document_revision_state
--         ,H.originating_organization
--         ,CAST(H.transmittal_creation_date AS TIMESTAMP) AS transmittal_creation_date
--         ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
--     --FROM "document_control"."curated_hexagon"."curated_hexagon_ofe" H
--     FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }}  H
--     --LEFT JOIN "document_control"."curated_aconex"."curated_docregister_standard" D 
--     LEFT JOIN {{ source('curated_aconex', 'curated_docregister_standard') }}  D 
--     ON H.document_number = D.documentnumber AND D.projectid = '1207979025'
--     WHERE document_number != 'nan'
-- ),

-- document as 
-- (
--     Select
--         documentnumber
--         ,revision
--         ,trackingid
--     --FROM "document_control"."curated_aconex"."curated_docregister_standard"    
--     FROM {{ source('curated_aconex', 'curated_docregister_standard') }}
--     WHERE   projectid = '1207979025' -- Aconex instance ID
--         AND     eff_end_date = '9999-12-31'-- AWS data journalling current flag
-- )

SELECT DISTINCT
--Primary and Foreign key Information
     source_system_name + '|' + trackingid AS document_key
    ,source_system_name + '|'  + documentid AS version_sid
    ,source_system_name + '|' + trackingid AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
--General Information
    ,current --Is this verions flagged as the "Current" in the source system
    ,versionnumber AS version_number
    ,selectlist4 AS sub_project
    ,selectlist5 AS phase
    ,revision AS revision_code
    ,revisiondate AS revision_date --Revision date as per the drawing. Not to be confused with the issued date
    ,SPLIT_PART(SPLIT_PART(documentstatus,' - ',1),'_',1) AS status_code
    ,SPLIT_PART(documentstatus,' - ',2) AS status_name
--Authoring Organization Informatin
    ,author AS originating_organization
    ,NULL AS originator_revision_code
    ,reference AS originator  -- The person (engineering/designer) that created or modified the drawing
--Revision Issue information
    ,datemodified as issued_date
    ,Cast(
        next_day(Dateadd(day,-1,datemodified),'friday')
    as Date) as issued_minor_period  
    ,selectlist2 as file_type --Is this the primary file or alternative like native or comments
    ,filename AS file_name
    ,NULL AS revision_comment
    ,reviewstatus as review_status
    ,reviewsource as review_source
    ,plannedsubmissiondate AS planned_submission_date
    ,milestonedate AS milestone_date
    ,purchaseordernumber AS purchase_order_number
    ,check1 AS check1
-- Project Control Reference Information
    ,NULL AS control_account
    ,NULL AS deliverable_group
    ,NULL AS issue_request_form_number
-- Datahub standard Meta data fields
    ,modifiedby AS uploaded_by
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
    revision
Where projectID = '1207979025' 

-- UNION ALL

--     SELECT DISTINCT
--  --Primary and Foreign key Information
--      H.document_sid AS document_key
--     , H.source_system_name + '|' + H.document_number + '|' + H.document_revision AS version_sid
--     ,H.document_sid

--  --General Information

--     ,true AS current  
--     ,CAST(NULL as int) AS version_number
--     ,NULL AS sub_project
--     ,NULL AS phase
--     ,document_revision as revision_code
--     ,revisiondate AS revision_date
--     ,document_revision_state AS status_code
--     ,NULL AS status_name

-- --Authoring Organization Information
--     ,originating_organization
--     ,NULL AS originator_revision_code
--     ,NULL AS originator

-- --Revision Issue information
--     ,transmittal_creation_date AS issued_date
--     ,CAST(
--     next_day(Dateadd(day,-1,transmittal_creation_date),'friday') AS Date) as issued_minor_period
--     ,'primary' AS file_type
--     ,NULL as file_name
--     ,null as revision_comment
--     ,'Review' as review_status
--     ,'Hexagon' as review_source
--     ,NULL AS planned_submission_date
--     ,NULL AS milestone_date
--     ,NULL AS purchase_order_number
--     ,CAST('false' AS boolean) AS check1
-- -- Project Control Reference Information
--     ,NULL AS control_account
--     ,NULL AS deliverable_group
--     ,NULL AS issue_request_form_number
--     ,NULL AS uploaded_by

-- -- Datahub standard Meta data fields
--     ,'1207979025' AS meta_project_rls_key
--     ,H.execution_date AS meta_ingestion_date
--     ,H.execution_date AS meta_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
--     ,{{run_date}} as created_date
--     ,{{run_date}} as updated_date
--     ,{{ generate_load_id(model) }} as load_id
-- FROM hexagon H
-- LEFT JOIN document D ON H.document_number = D.documentnumber AND H.document_revision = D.revision
-- WHERE D.revision IS NULL

UNION ALL

SELECT DISTINCT
--Primary and Foreign key Information
     source_system_name + '|' + trackingid AS document_key
    ,source_system_name + '|'  + documentid AS version_sid
    ,source_system_name + '|' + trackingid AS document_sid -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
--General Information
    ,current --Is this verions flagged as the "Current" in the source system
    ,versionnumber AS version_number
    ,null AS sub_project
    ,selectlist5 AS phase
    ,revision AS revision_code
    ,revisiondate AS revision_date --Revision date as per the drawing. Not to be confused with the issued date
    ,SPLIT_PART(SPLIT_PART(documentstatus,' - ',1),'_',1) AS status_code
    ,SPLIT_PART(documentstatus,' - ',2) AS status_name
--Authoring Organization Informatin
    ,author AS originating_organization
    ,NULL AS originator_revision_code
    ,reference AS originator  -- The person (engineering/designer) that created or modified the drawing
--Revision Issue information
    ,datemodified as issued_date
    ,CAST(
        next_day(Dateadd(day,-1,datemodified),'friday')
     AS Date) as issued_minor_period 
    ,selectlist2 as file_type --Is this the primary file or alternative like native or comments
    ,filename AS file_name
    ,comments AS revision_comment
    ,reviewstatus as review_status
    ,reviewsource as review_source
    ,plannedsubmissiondate AS planned_submission_date
    ,milestonedate AS milestone_date
    ,purchaseordernumber AS purchase_order_number
    ,check1 AS check1
-- Project Control Reference Information
    ,Selectlist6 AS control_account
    ,NULL AS deliverable_group
    ,NULL AS issue_request_form_number
-- Datahub standard Meta data fields
    ,modifiedby AS uploaded_by
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
    revision
WHERE projectid <> '1207979025'
--in ('268456382','268452089')