{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"]
    )
}}

WITH Workflow as
(
    Select
        source_system_name
        ,documenttrackingid
        ,workflowid
        ,documentrevision
        ,workflownumber
        ,workflowname
        ,workflowstatus
        ,assignee_userid
        ,assignee_name
        ,assignee_organizationname
        ,datein
        ,dayslate
        ,duration
        ,stepoutcome
        ,CASE 
            WHEN lower(stepname) like 'consolidator%' then 'Consolidator'
            WHEN lower(stepname) like '%consolidator' then 'Consolidator'
            ELSE 'Reviewer' END as stepname
        ,stepstatus
        ,CAST(originalduedate AS TIMESTAMP) AS originalduedate
        ,CAST(datedue AS TIMESTAMP) AS datedue
        ,CAST(datecompleted AS TIMESTAMP) AS datecompleted
        ,reasonforissue
        ,CAST(projectid AS VARCHAR) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_workflow') }}
    --FROM "document_control"."curated_aconex"."curated_workflow" 
    WHERE   eff_end_date = '9999-12-31'
        --AND projectid in ('1207979025','268456382','268452089') -- Aconex instance ID
              -- AWS data journalling current flag
),
 
Actionee AS
(
    SELECT
        userid
        ,email
    FROM {{ source('curated_aconex', 'curated_userdirectory') }}
    --FROM    "document_control"."curated_aconex"."curated_userdirectory"
)
 
-- hexagon AS
-- (
--     SELECT
--         CASE 
--             WHEN D.trackingid IS NULL THEN h.source_system_name + '|' + document_number
--             ELSE D.source_system_name + '|' + D.trackingid 
--         END AS document_sid
--         ,H.source_system_name + '|' + h.transmittal_number as workflow_sid
--         ,H.document_revision
--         ,H.transmittal_number
--         ,H.recipient
--         ,H.recipient_e_mail_address
--         ,H.originating_organization
--         ,'Reviewer' as workflow_step
--         ,H.workflow_step_status
--         ,CAST('1207979025' AS VARCHAR) AS projectid
--         ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
--         ,CASE 
--             WHEN CAST(transmittal_suggested_due_date as char) = 'NaT' THEN CAST(NULL AS TIMESTAMP)
--             ELSE CAST(transmittal_suggested_due_date as TIMESTAMP) 
--         END AS transmittal_suggested_due_date
--     FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }} H
--     --FROM "document_control"."curated_hexagon"."curated_hexagon_ofe" H
--     LEFT JOIN {{ source('curated_aconex', 'curated_docregister_standard') }}
--     --LEFT JOIN "document_control"."curated_aconex"."curated_docregister_standard"
--          D ON H.document_number = D.documentnumber and D.projectid = '1207979025'
--     WHERE 
--         document_number != 'nan'
 
--     UNION ALL
 
--     SELECT
--         CASE 
--             WHEN D.trackingid IS NULL THEN h.source_system_name + '|' + document_number
--             ELSE D.source_system_name + '|' + D.trackingid 
--         END AS document_sid
--         ,H.source_system_name + '|' + h.transmittal_number as workflow_sid
--         ,H.document_revision
--         ,H.transmittal_number
--         ,H.recipient
--         ,H.recipient_e_mail_address
--         ,H.originating_organization
--         ,'Consolidator' as workflow_step
--         ,H.workflow_step_status
--         ,CAST('1207979025' AS VARCHAR) AS projectid
--         ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
--         ,CASE 
--             WHEN CAST(transmittal_suggested_due_date as char) = 'NaT' THEN CAST(NULL AS TIMESTAMP)
--             ELSE CAST(transmittal_suggested_due_date as TIMESTAMP) 
--         END AS transmittal_suggested_due_date
--     FROM {{ source('curated_hexagon', 'curated_hexagon_ofe_consolidate') }} H
--     --FROM "document_control"."curated_hexagon"."curated_hexagon_ofe_consolidate" H
--     LEFT JOIN {{ source('curated_aconex', 'curated_docregister_standard') }}
--     --LEFT JOIN "document_control"."curated_aconex"."curated_docregister_standard"
--          D ON H.document_number = D.documentnumber and D.projectid = '1207979025'
--     WHERE 
--         document_number != 'nan'
-- )
 
SELECT 
       source_system_name + '|' + documenttrackingid AS document_sid
      ,source_system_name + '|' + workflowid AS workflow_sid
      ,documentrevision AS document_revision_code
      ,workflownumber as workflow_number
      ,workflowname as workflow_title
      ,null as workflow_type  -- which field to use?
      ,workflowstatus as workflow_status
      ,assignee_name
      ,email as assignee_email
      ,assignee_organizationname AS assignee_organization 
      ,stepname as action_name
      ,stepstatus as action_status
      ,originalduedate as original_due_date
      ,datedue as current_due_date
      ,datecompleted as date_completed
      ,reasonforissue as reason_for_issue
      ,datein as date_in
      ,dayslate as days_late
      ,duration as duration
      ,stepoutcome as step_outcome
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
    Workflow
LEFT JOIN Actionee on workflow.assignee_userid = actionee.userid

-- UNION ALL
 
-- SELECT
--      H.document_sid
--     ,h.workflow_sid
--     ,H.document_revision AS document_revision_code
--     ,H.transmittal_number as workflow_number
--     ,NULL as workflow_title
--     ,NULL as workflow_type  -- which field to use?
--     ,'In Progress' as workflow_status
--     ,recipient as assignee_name
--     ,recipient_e_mail_address as assignee_email
--     ,originating_organization as assignee_organization
--     ,workflow_step as action_name
--     ,workflow_step_status as action_status
--     ,transmittal_suggested_due_date as original_due_date
--     ,transmittal_suggested_due_date as current_due_date
--     ,CAST(NULL as date) as date_completed
--     ,'OFE Document Review' as reason_for_issue
--     ,NULL as date_in
--     ,NULL as days_late
--     ,NULL as duration
--     ,NULL as step_outcome
-- -- Datahub standard Meta data fields
--     ,projectid AS meta_project_rls_key
--     ,CAST(execution_date as date) AS meta_ingestion_date
--     ,CAST(execution_date as date) AS meta_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
--     ,{{run_date}} as created_date
--     ,{{run_date}} as updated_date
--     ,{{ generate_load_id(model) }} as load_id
-- FROM hexagon H