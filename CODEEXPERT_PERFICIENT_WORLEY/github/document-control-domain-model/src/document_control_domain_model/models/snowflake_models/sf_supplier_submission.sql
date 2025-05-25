
{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}

{{
    config(
        materialized = "table",
        tags=["snowflake_models"],
        dist = "revision_sid"
    )
}}

/*

SQL Query Title: Global Standard Model - Aconex Tag To Document cross reference table
Domain: Document Management
Data Governance Confidentiality Level:
Project Name: Global Standard
   
*/
WITH
Doc_register AS(
    SELECT DISTINCT 
        COALESCE(trackingid, '') + '|' + COALESCE(revision, '') AS JPK,
        documentid,
        trackingid,
        revision,
        documentnumber,
        documenttype,
        reviewsource,
        versionnumber,
        author,
        projectid,
        execution_date,
        Max(execution_date) over (PARTITION BY projectid) as meta_snapshot_date,
        eff_end_date
    FROM {{ source('curated_aconex', 'curated_docregister_standard') }} 
    --FROM "document_control"."curated_aconex"."curated_docregister_standard"
    WHERE eff_end_date = '9999-12-31'
    AND documenttype = 'Supplier Data'),
current_rev_version AS(
   SELECT 
        JPK,
        projectid,
        MAX(documentid) OVER (PARTITION BY trackingid, revision) as revision_sid,
        MAX(execution_date) OVER (PARTITION BY trackingid, revision) as execution_date,
        DocumentNumber,
        Revision,
        meta_snapshot_date
    FROM Doc_register),
Supplier AS(
    SELECT
        JPK
        ,FIRST_VALUE(author) OVER (PARTITION BY trackingid order by versionnumber desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS supplier
    FROM Doc_register A
    WHERE Author NOT ILIKE '%worley%'),
POD AS (
    SELECT
        JPK
    ,FIRST_VALUE(reviewsource) OVER (PARTITION BY trackingid, revision  order by versionnumber desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS purchase_order
    FROM Doc_register A
    WHERE reviewsource ILIKE '%POD%'),
Doc_Workflow AS (
    SELECT DISTINCT
        JPK,
        FIRST_VALUE(reviewsource) OVER (PARTITION BY trackingid, revision  order by versionnumber desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS workflow_number
    FROM Doc_register
    WHERE reviewsource ILIKE 'WF%'),
Consolidator AS(
    SELECT
        documenttrackingid::varchar + '|' + documentrevision AS jpk,
        workflowid,
        workflownumber,
        datecompleted::TIMESTAMP,
        stepoutcome
    FROM {{ source('curated_aconex', 'curated_workflow') }}
    --FROM "document_control"."curated_aconex"."curated_workflow" 
    WHERE stepname = 'Consolidator' and eff_end_date = '9999-12-31'),
Supplier_transmittal AS(
    SELECT DISTINCT
        COALESCE(d.documentid, d.registeredas)::varchar AS revision_sid,
        M.mailid AS transmittal_sid,
        M.sentdate::TIMESTAMP AS sentdate,
        DATEADD(day, 14, M.sentdate::TIMESTAMP) AS Review_Due_Date
    FROM {{ source('curated_aconex', 'curated_mailinbox') }} M
    --FROM "document_control"."curated_aconex"."curated_mailinbox" M
    LEFT JOIN {{ source('curated_aconex', 'curated_mail_document') }} D
    --LEFT JOIN "document_control"."curated_aconex"."curated_mail_document" D 
        ON M.mailid = D.mailid
    WHERE correspondencetype = 'Supplier Document Transmittal' and m.eff_end_date = '9999-12-31' )

SELECT DISTINCT
    r.revision_sid AS dbt_key,
    r.revision_sid,
    r.documentnumber,
    r.revision,
    s.supplier,
    p.purchase_order,
    t.transmittal_sid,

-- Columns related to the suppliers submission
    t.sentdate AS Submission_Completed_Date,
    Cast('9999-12-31' AS timestamp) AS submission_due_date,
    Cast('9999-12-31' AS timestamp) AS submission_due_period,
    DATEADD(day, 
        (7 - EXTRACT(DOW FROM t.sentdate))::int, 
        DATEADD(day, -1, t.sentdate)
    ) AS Submission_Completed_period,
    0 as Submission_day_late,

-- Columns related to Supplier Document Review
    c.workflowid::Varchar,
    t.Review_Due_Date,
    DATEADD(day, 
        (7 - EXTRACT(DOW FROM t.Review_Due_Date))::int, 
        DATEADD(day, -1, t.Review_Due_Date)
    ) AS Review_Due_period,
    c.datecompleted AS review_completed_date,
    DATEADD(day, 
        (7 - EXTRACT(DOW FROM c.datecompleted))::int, 
        DATEADD(day, -1, c.datecompleted)
    ) AS review_completed_period,

    CAST(DATEDIFF(day, t.Review_Due_Date, c.datecompleted) + 1
            - (DATEDIFF(week, t.Review_Due_Date, c.datecompleted) * 2)
            - CASE WHEN EXTRACT(DOW FROM t.Review_Due_Date) = 6 THEN 1 ELSE 0 END
            - CASE WHEN EXTRACT(DOW FROM c.datecompleted) = 0 THEN 1 ELSE 0 END AS int)
     AS review_days_Late,
    c.stepoutcome as review_status
    ,r.projectid AS meta_project_rls_key
    ,CAST(r.execution_date as Timestamp) AS meta_ingestion_date
    ,CAST(r.meta_snapshot_date as Timestamp) as meta_snapshot_date
    ,{{run_date}} as dbt_created_date
    ,{{run_date}} as dbt_updated_date
    ,{{generate_load_id(model) }} as dbt_load_id
FROM current_rev_version r
LEFT OUTER JOIN Supplier s ON r.jpk = s.jpk
LEFT OUTER JOIN POD P ON r.jpk = p.jpk
LEFT OUTER JOIN Doc_Workflow W ON r.jpk = w.jpk
LEFT OUTER JOIN Consolidator C ON w.jpk = c.jpk AND w.workflow_number = c.workflownumber
LEFT OUTER JOIN Supplier_transmittal t ON r.revision_sid = t.revision_sid
/*
WITH 
merged AS
(
    SELECT
        trackingid
        ,projectid
        ,source_system_name + '|' + trackingid AS document_sid
        ,current
        ,attribute4_attributetypenames AS merged_source
        ,REPLACE(REPLACE(attribute4_attributetypenames,'{"AttributeTypeName":["',''),'"]}','') as merged_value
        ,CAST(execution_date AS TIMESTAMP) AS execution_date
        ,source_system_name
        ,primary_key
        ,is_current
        ,CAST(eff_end_date AS TIMESTAMP) AS eff_end_date
    FROM
        {{ source('curated_aconex', 'curated_docregister_standard') }} 
    Where 
        attribute2_attributetypenames IS NOT NULL
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
     source_system_name + '|' + trackingid AS document_key  -- Tracking Id represent a unique document in Aconex. The DocumentId repersent a unique version.
    ,SPLIT_PART(parsed_value,' - ',1) AS sdr_code
    ,SPLIT_PART(parsed_value,' - ',2) AS sdr_name 
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
*/