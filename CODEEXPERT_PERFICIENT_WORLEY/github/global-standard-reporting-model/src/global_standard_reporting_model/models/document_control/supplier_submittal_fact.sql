{{
     config(
         materialized = "table",
         tags=["document_control"]
     )
}}

WITH Master as
(
    Select  
      document_sid
     ,title as document_title
     ,discipline_code
     ,discipline_name
     ,dim_project_name
     ,jip33
    --FROM     "document_control"."domain_integrated_model"."master"
    FROM {{ source('document_control_domain_integrated_model', 'master') }}
    Where category = 'Supplier Data' or type_code like '%SDC%'
)

, Revision as
( 
    Select 
    document_sid
    ,version_sid
    ,revision_sid
     ,status_code
     ,status_name
     ,current
    FROM {{ source('document_control_domain_integrated_model', 'revision') }}
    --FROM  "document_control"."domain_integrated_model"."revision"
    --where current = 'true' 
    --and status_code not in ('SUP','VOD','No Longer In Use', 'Rejected', 'Working Document')
)
,Resubmission AS (
    SELECT 
        document_sid,
        COUNT(DISTINCT revision_code) as no_of_resubmission
    FROM {{ source('document_control_domain_integrated_model', 'supplier_submission') }}
    GROUP BY document_sid
)

, Supplier_submission as
(
     Select
    document_sid
    ,revision_sid
     ,document_number as document_number
     ,submission_status
     ,Submission_cycle_status
     ,supplier
    ,purchase_order
    ,'' as sdr_code
    ,revision_code
    ,submission_day_late
    ,review_day_late
    ,submission_completed_date as date_submitted
    ,review_completed_date as date_returned
    ,submission_due_date as planned_submission_date
    ,review_completed_date_weekend as date_returned_weekend
    ,submission_completed_date_weekend as date_submitted_weekend
    ,submission_due_date_weekend as planned_submission_date_weekend
    ,dim_project_rls_key
    ,dbt_ingestion_date
    --FROM   "document_control"."domain_integrated_model"."supplier_submission"
     FROM {{ source('document_control_domain_integrated_model', 'supplier_submission') }}
          
)

SELECT
    Supplier_submission.document_number
    ,Supplier_submission.document_sid
    ,Case   when submission_status = 'Submision Required' then 'Total Pending Submission'
            When submission_status = 'Submitted' and resubmission.no_of_resubmission = 1 Then 'First Submittal'
            When submission_status = 'Submitted' and resubmission.no_of_resubmission > 1 Then 'Total Resubmitted'
            When Submission_cycle_status = 'Submision Required' and submission_day_late = 0 and resubmission.no_of_resubmission = 1  then 'Pending First Submission'
            When Submission_cycle_status = 'Resubmision Required' and submission_day_late = 0 and resubmission.no_of_resubmission > 1 then 'Pending Resubmission'
            When Submission_cycle_status = 'Submision Required' and submission_day_late <> 0 and resubmission.no_of_resubmission = 1 then 'Overdue First Submission'
            When Submission_cycle_status = 'Resubmision Required' and submission_day_late <> 0 and resubmission.no_of_resubmission > 1 then 'Overdue Resubmission'
            When Submission_status = 'Submitted' Then 'Total Submitted'
            When  Submission_cycle_status = 'Cancelled' and revision.current= 'true' then 'Total Canceled'
            When Submission_cycle_status = 'Completed' and revision.current= 'true' then 'Total Completed'
            else null
            End as Submittal_Status
    ,Supplier_submission.supplier
    ,Supplier_submission.purchase_order
    ,Supplier_submission.sdr_code
    ,revision_code
    ,Supplier_submission.submission_day_late
    ,Supplier_submission.review_day_late
    ,Supplier_submission.date_submitted
    ,Supplier_submission.date_returned
    ,Supplier_submission.planned_submission_date
    ,Supplier_submission.date_submitted_weekend
    ,Supplier_submission.date_returned_weekend
    ,Supplier_submission.planned_submission_date_weekend
    ,Revision.status_code
    ,Revision.status_name
    ,Revision.current
    ,Master.document_title
    ,Master.discipline_code
    ,Master.discipline_name
    ,Master.jip33
    ,dim_project_name + ' - ' + 'Supplier Submission Summary' as report_title
    ,dim_project_rls_key as project_id
    ,dim_project_name as project_name
    ,dbt_ingestion_date as snapshot_date
FROM
    Supplier_submission
    Left join  revision on Supplier_submission.document_sid = Revision.document_sid and Supplier_submission.revision_sid = Revision.revision_sid
    Left Join Resubmission on Supplier_submission.document_sid = Resubmission.document_sid
    Left join  Master on Supplier_submission.document_sid = Master.document_sid 
    WHERE Supplier_submission.document_sid IS NOT NULL
