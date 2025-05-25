{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_supplier_submission/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

WITH Document AS(
    SELECT DISTINCT 
        M.document_sid,
        R.revision_sid,
        R.version_sid,
        M.document_number,
        R.revision_code,
        R.review_source,
        R.review_status,
        R.originating_organization,
        R.planned_submission_date,
        R.Planned_submission_date_weekend,
        R.Supplier,
        R.dim_project_rls_key,
        R.dbt_ingestion_date,
        R.dim_snapshot_date
    FROM {{ ref('transformed_master') }}  M
    --FROM "document_control"."domain_integrated_model"."master" 
    LEFT JOIN {{ ref('transformed_revision') }} R ON
    --LEFT JOIN "document_control"."domain_integrated_model"."revision" R ON  
        M.document_sid = R.document_sid AND 
        M.dim_project_rls_key = R.dim_project_rls_key
    WHERE M.category = 'Supplier Data'
        AND R.file_type = 'Primary'
    ),

POD AS (
    SELECT
        document_sid
        ,FIRST_VALUE(review_source) OVER (PARTITION BY document_sid order by version_sid desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS purchase_order
    FROM Document
    WHERE review_source NOT ILIKE 'WF%' AND review_source != 'None'),

Workflow AS (
    SELECT DISTINCT
        revision_sid
        ,FIRST_VALUE(review_source) OVER (PARTITION BY revision_sid  order by version_sid desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS workflow_number
        ,FIRST_VALUE(review_status) OVER (PARTITION BY revision_sid  order by version_sid desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS review_status
    FROM Document
    WHERE review_source ILIKE 'WF%' AND review_status != 'Terminated'),

Consolidator AS(
    SELECT DISTINCT
        document_sid,
        revision_code,
        workflow_number,
        workflow_status,
        Min(workflow_created_date) AS workflow_created_date,
        Min(workflow_created_date_weekend) AS workflow_created_date_weekend,
        Max(current_due_date) AS current_due_date,
        Max(current_due_date_weekend) AS current_due_date_weekend,
        Max(date_completed) AS date_completed,
        Max(date_completed_weekend) AS date_completed_weekend,
        Max(review_day_late) AS review_day_late
        
    FROM {{ ref('transformed_workflow_action') }}  M
    --FROM "document_control"."domain_integrated_model"."workflow_action"
    WHERE workflow_status != 'Terminated'
    GROUP BY         
        document_sid,
        revision_code,
        workflow_number,
        workflow_status
    ),

Supplier_transmittal AS (
        SELECT DISTINCT
            T.transmittal_sid,
            D.document_sid,
            D.revision_sid,
            T.sent_date,
            T.sent_date_weekend
        FROM {{ ref('transformed_transmittal') }} T
        LEFT JOIN (
            Select *,
            FIRST_VALUE(transmittal_sid) OVER (PARTITION BY document_sid, revision_sid  order by transmittal_sid desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_transmittal
            FROM {{ ref('transformed_transmittal_to_document') }} ) D 
            ON t.transmittal_sid = d.transmittal_sid
        WHERE t.transmittal_type = 'Supplier Document Transmittal' 
            AND D.transmittal_sid = D.latest_transmittal
            -- AND revision_sid = 'aconex|1348828088866128824' 
            )



SELECT DISTINCT

-- Document Details
    D.document_sid,
    D.revision_sid,
    D.document_number,
    D.revision_code,

--Supplier Package Details
    D.supplier,
    p.purchase_order,
    CASE
        WHEN LOWER(D.review_status) LIKE 'cancelled' THEN 'Cancelled'
        WHEN D.revision_code LIKE '%-%' THEN 'Placeholder'
        WHEN T.sent_date IS NULL AND C.workflow_created_date IS NULL THEN 'Submission Required'
        WHEN c.date_completed IS NULL THEN 'In Progress'  
        WHEN w.review_status LIKE '%2%' OR w.review_status LIKE '%3%' THEN 'Resubmission Required'
        --WHEN w.review_status SIMILAR TO '%(2|3)%' THEN 'Resubmission Required' -- Add C back in 
        WHEN w.review_status LIKE '%1%' OR w.review_status LIKE '%4%' THEN 'Completed'
        --WHEN w.review_status SIMILAR TO '%(1|4)%' THEN 'Completed'  -- Add C back in 
        WHEN T.sent_date IS NOT NULL OR C.workflow_created_date IS NOT NULL THEN 'Submission Required'
    END AS submission_cycle_status,

-- Supplier Submission Details
    t.transmittal_sid as supplier_transmittal_sid,
    CASE WHEN t.sent_date IS NULL THEN 'Submission Required' ELSE 'Submitted' END AS submission_status,
    D.planned_submission_date AS submission_due_date,
    D.planned_submission_date_weekend AS submission_due_date_weekend,
    CASE WHEN T.sent_date IS NULL AND C.workflow_created_date is not NULL THEN C.workflow_created_date ELSE T.sent_date END AS submission_completed_date,
    CASE WHEN T.sent_date_weekend IS NULL AND C.workflow_created_date_weekend is not NULL THEN C.workflow_created_date_weekend ELSE T.sent_date_weekend END AS submission_completed_date_weekend,
CASE 
        WHEN T.sent_date IS NULL AND C.workflow_created_date is not NULL THEN
        ((DATEDIFF(day, D.planned_submission_date, C.workflow_created_date) + 1)
        - (DATEDIFF(week, D.planned_submission_date, C.workflow_created_date) * 2)
        - (CASE WHEN DATE_PART('dow', D.planned_submission_date) = 0 THEN 1 ELSE 0 END)
        - (CASE WHEN DATE_PART('dow', C.workflow_created_date) = 6 THEN 1 ELSE 0 END))
        WHEN T.sent_date IS NULL AND C.workflow_created_date is NULL THEN
        ((DATEDIFF(day, D.planned_submission_date, dbt_ingestion_date) + 1)
        - (DATEDIFF(week, D.planned_submission_date, dbt_ingestion_date) * 2)
        - (CASE WHEN DATE_PART('dow', D.planned_submission_date) = 0 THEN 1 ELSE 0 END)
        - (CASE WHEN DATE_PART('dow', dbt_ingestion_date) = 6 THEN 1 ELSE 0 END))
        WHEN T.sent_date IS NOT NULL THEN
        ((DATEDIFF(day, D.planned_submission_date,T.sent_date) + 1)
        - (DATEDIFF(week, D.planned_submission_date,T.sent_date) * 2)
        - (CASE WHEN DATE_PART('dow', D.planned_submission_date) = 0 THEN 1 ELSE 0 END)
        - (CASE WHEN DATE_PART('dow', T.sent_date) = 6 THEN 1 ELSE 0 END)) 
    END AS submission_day_late,

-- Supplier Document Review Details
    C.workflow_number AS review_workflow_number,
    C.workflow_status as review_status,
    C.current_due_date as review_due_date,
    C.current_due_date_weekend AS review_due_date_weekend,
    C.date_completed AS review_completed_date,
    C.date_completed_weekend AS review_completed_date_weekend,
    C.review_day_late AS review_day_late,


-- Metadata Information
    dim_project_rls_key,
    dbt_ingestion_date,
    dim_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{generate_load_id(model) }} as dbt_load_id
FROM Document D
LEFT OUTER JOIN POD P                   ON D.document_sid = P.document_sid
LEFT OUTER JOIN Workflow W              ON D.revision_sid = W.revision_sid
LEFT OUTER JOIN Supplier_transmittal t  ON D.revision_sid = t.revision_sid
LEFT OUTER JOIN Consolidator C          ON D.document_sid = C.document_sid 
AND W.workflow_number = c.workflow_number
