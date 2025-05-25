{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_workflow_action/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
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
        ,duration
        ,stepoutcome
        ,dayslate
        ,CAST(datein as TIMESTAMP) as workflow_created_date
        ,dayslate
        ,CASE 
            WHEN lower(stepname) like 'consolidator%' then 'Consolidator'
            WHEN lower(stepname) like '%consolidator' then 'Consolidator'
            ELSE 'Reviewer' END as stepname
        ,stepstatus
        ,CAST(originalduedate AS TIMESTAMP) AS originalduedate
        ,CAST(datedue AS TIMESTAMP) AS datedue
        ,CAST(datecompleted AS TIMESTAMP) AS datecompleted
        ,reasonforissue
        ,CAST(projectid AS VARCHAR(100)) AS projectid
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM {{ source('curated_aconex', 'curated_workflow') }}
    --FROM "document_control"."dbt_curated_aconex"."curated_workflow" 
    WHERE   
        --projectid in ('268456382','268452089','268447877','1879053267','268454994','1342181262','1342181257','1342181165','1342181268','268456867','268456802') -- Aconex instance ID
        --AND 
        eff_end_date = '9999-12-31'     -- AWS data journalling current flag
),

Actionee AS
(
    SELECT
        userid
        ,email
    FROM {{ source('curated_aconex', 'curated_userdirectory') }}
    --FROM    "document_control"."dbt_curated_aconex"."curated_userdirectory"
),

hexagon AS
(
    SELECT
        CASE 
            WHEN D.trackingid IS NULL THEN concat(h.source_system_name, '|' , document_number)
            ELSE concat(D.source_system_name , '|' , D.trackingid)
        END AS document_sid
        ,concat(H.source_system_name , '|' , h.transmittal_number) as workflow_sid
        ,H.document_revision
        ,H.transmittal_number
        ,H.recipient
        ,H.recipient_e_mail_address
        ,H.originating_organization
        ,'Reviewer' as workflow_step
        ,H.workflow_step_status
        ,CAST('1207979025' AS VARCHAR(100)) AS projectid
        ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
        ,CASE 
            WHEN CAST(transmittal_suggested_due_date as STRING) = 'NaT' THEN CAST(NULL AS TIMESTAMP)
            ELSE CAST(transmittal_suggested_due_date as TIMESTAMP) 
        END AS transmittal_suggested_due_date
    FROM {{ source('curated_hexagon', 'curated_hexagon_ofe') }} H
    --FROM "document_control"."dbt_curated_hexagon"."curated_hexagon_ofe" H
    LEFT JOIN {{ source('curated_aconex', 'curated_docregister_standard') }}
    --LEFT JOIN "document_control"."dbt_curated_aconex"."curated_docregister_standard"
        D ON H.document_number = D.documentnumber and D.projectid = '1207979025'
    WHERE 
        document_number != 'nan'

    UNION ALL

    SELECT
        CASE 
            WHEN D.trackingid IS NULL THEN concat(h.source_system_name , '|' , document_number)
            ELSE concat(D.source_system_name , '|' , D.trackingid)
        END AS document_sid
        ,concat(H.source_system_name , '|' , h.transmittal_number) as workflow_sid
        ,H.document_revision
        ,H.transmittal_number
        ,H.recipient
        ,split_part(H.recipient_e_mail_address,'@',1) as recipient_e_mail_address
        ,H.originating_organization
        ,'Consolidator' as workflow_step
        ,H.workflow_step_status
        ,CAST('1207979025' AS VARCHAR(100)) AS projectid
        ,CAST(H.execution_date AS TIMESTAMP) AS execution_date
        ,CASE 
            WHEN CAST(transmittal_suggested_due_date as STRING) = 'NaT' THEN CAST(NULL AS TIMESTAMP)
            ELSE CAST(transmittal_suggested_due_date as TIMESTAMP) 
        END AS transmittal_suggested_due_date
    FROM {{ source('curated_hexagon', 'curated_hexagon_ofe_consolidate') }} H
    --FROM "document_control"."dbt_curated_hexagon"."curated_hexagon_ofe_consolidate" H
    LEFT JOIN {{ source('curated_aconex', 'curated_docregister_standard') }}
    --LEFT JOIN "document_control"."dbt_curated_aconex"."curated_docregister_standard"
        D ON H.document_number = D.documentnumber and D.projectid = '1207979025'
    WHERE 
        document_number != 'nan'
)

SELECT
    concat(source_system_name , '|' , documenttrackingid) AS document_sid
    ,concat(source_system_name , '|' , workflowid) AS workflow_sid
    ,documentrevision AS revision_code
    ,workflownumber as workflow_number
    ,workflowname as workflow_title
    ,cast(NULL AS VARCHAR(10)) as workflow_type  -- which field to use?
    ,workflowstatus as workflow_status
    ,assignee_name
    ,assignee_organizationname AS assignee_organization 
    ,stepname as action_name
    ,stepstatus as action_status
    ,workflow_created_date
    ,cast(next_day(Dateadd(day,-1,workflow_created_date),'friday') AS TIMESTAMP) as workflow_created_date_weekend
    ,originalduedate as original_due_date
    ,cast(next_day(Dateadd(day,-1,originalduedate),'friday') AS TIMESTAMP) as original_due_date_weekend
    ,datedue as current_due_date
    ,cast(next_day(Dateadd(day,-1,datedue),'friday') AS TIMESTAMP) as current_due_date_weekend
    ,datecompleted as date_completed
    ,cast(next_day(Dateadd(day,-1,datecompleted),'friday') AS TIMESTAMP) as date_completed_weekend
    ,reasonforissue as reason_for_issue
    ,CAST(dayslate AS INTEGER) as review_day_late
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,execution_date AS dbt_ingestion_date
    ,CASE
        WHEN eff_end_date = '9999-12-31' THEN current_date()
        ELSE eff_end_date
    END AS dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM
    Workflow
LEFT JOIN Actionee on workflow.assignee_userid = actionee.userid

UNION ALL

SELECT
    H.document_sid
    ,h.workflow_sid
    ,H.document_revision AS revision_code
    ,H.transmittal_number as workflow_number
    ,CAST(NULL AS VARCHAR(1000)) as workflow_title
    ,CAST(NULL AS VARCHAR(10)) as workflow_type  -- which field to use?
    ,'In Progress' as workflow_status
    ,recipient_e_mail_address as aassignee_name
    ,originating_organization as assignee_organization
    ,workflow_step as action_name
    ,workflow_step_status as action_status
    ,CAST(NULL as TIMESTAMP) as workflow_created_date
    ,CAST(NULL as TIMESTAMP) as workflow_created_date_weekend
    ,transmittal_suggested_due_date as original_due_date
    ,cast(next_day(Dateadd(day,-1,transmittal_suggested_due_date),'friday') AS TIMESTAMP) as original_due_date_weekend
    ,transmittal_suggested_due_date as current_due_date
    ,cast(next_day(Dateadd(day,-1,transmittal_suggested_due_date),'friday') AS TIMESTAMP) as current_due_date_weekend
    ,CAST(NULL as date) as date_completed
    ,CAST(NULL as TIMESTAMP)  as date_completed_weekend
    ,'OFE Document Review' as reason_for_issue
    ,CAST('0' AS INTEGER) as review_day_late
-- Datahub standard Meta data fields
    ,projectid AS dim_project_rls_key
    ,CAST(execution_date as date) AS dbt_ingestion_date
    ,CAST(execution_date as date) AS dim_snapshot_date  --in the absent of eff_end_date, execution_date will be duplicated as the snapshot date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{ generate_load_id(model) }} as dbt_load_id
FROM hexagon H
