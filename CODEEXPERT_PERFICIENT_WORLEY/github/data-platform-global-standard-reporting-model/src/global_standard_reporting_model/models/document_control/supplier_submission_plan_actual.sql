{{
     config(
         materialized = "table",
         tags=["document_control"]
     )

}}

SELECT distinct
    new_submision_status,document_number,submission_status,snapshot_date,project_id,dim_project_rls_name,
    'Planned' as attribute,
    --lanned_submission_date as date_value,
    due_in as date_value,
    case when new_submision_status='Pending First Submission' or new_submision_status='Pending Resubmission' or new_submision_status='Overdue First Submission' or new_submision_status='Overdue Resubmission' or new_submision_status='Pending Submission-Other'
    then 'Pending submission'
    when new_submision_status='First Submitted' or new_submision_status='Resubmitted' or new_submision_status='Submitted-Other'
    then 'Submitted' else new_submision_status end as  overall_status,CURRENT_WEEK
     --case when submission_status='Submission Required' then planned_submission_date else null end as Value

from {{ ref('supplier_submission_fact') }}
   -- "global_standard_reporting"."document_control"."supplier_submission_fact"
    where planned_submission_date is not null and current_flag=true 

union ALL

SELECT distinct
    new_submision_status,document_number,submission_status,snapshot_date,project_id,dim_project_rls_name,
    'Actual' as attribute,actual_in as date_value,
    case when new_submision_status='Pending First Submission' or new_submision_status='Pending Resubmission' or new_submision_status='Overdue First Submission' or new_submision_status='Overdue Resubmission' or new_submision_status='Pending Submission-Other'
    then 'Pending submission'
    when new_submision_status='First Submitted' or new_submision_status='Resubmitted' or new_submision_status='Submitted-Other'
    then 'Submitted' else new_submision_status end as  overall_status,CURRENT_WEEK
    --case when submission_status='Marked as Submitted' then actual_in when submission_status='Submitted' then actual_in else null end as Value
From {{ ref('supplier_submission_fact') }}
    --"global_standard_reporting"."document_control"."supplier_submission_fact"
    where actual_in is not null and current_flag=true