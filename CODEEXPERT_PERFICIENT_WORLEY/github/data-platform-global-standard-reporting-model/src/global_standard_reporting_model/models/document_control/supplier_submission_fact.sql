{{
     config(
         materialized = "table",
         tags=["document_control"]
     )

}}

   select * ,  case When actual_in BETWEEN Week_Start_date
    and Week_End_Date then 'In_Week'
    When actual_in not BETWEEN Week_Start_date
    and Week_End_Date then 'Out_Week'
    else 'No' end as CURRENT_WEEK from
    (select 
     CASE
    WHEN submission_status = 'Submission Required'
    AND (
        review_status = 'In Progress'
        OR review_status IS NULL
    )
    AND cast(days_late as int) = 0 THEN 'Pending First Submission'
    WHEN submission_status = 'Submission Required'
    AND (
        review_status like 'C2%'
        OR review_status like 'C3%'
    )
    AND cast(days_late as int) = 0 THEN 'Pending Resubmission'
    WHEN submission_status = 'Submission Required'
    AND (
        review_status = 'In Progress'
        OR review_status IS NULL
    )
    AND cast(days_late as int) <> 0 THEN 'Overdue First Submission'
    WHEN submission_status = 'Submission Required'
    AND (
        review_status like 'C2%'
        OR review_status like 'C3%'
    )
    AND cast(days_late as int) <> 0 THEN 'Overdue Resubmission'
    WHEN (
        submission_status = 'Marked As Submitted'
        OR submission_status = 'Submitted'
    )
    AND (
        review_status = 'In Progress'
        OR review_status IS NULL
    ) THEN 'First Submitted'
    WHEN (
        submission_status = 'Marked As Submitted'
        OR submission_status = 'Submitted'
    )
    AND (
        review_status like 'C2%'
        OR review_status like 'C3%'
    ) THEN 'Resubmitted'
    WHEN submission_status = 'Canceled' THEN 'Canceled'
    WHEN submission_status = 'Completed' THEN 'Completed'
    WHEN submission_status = 'Submission Required'  THEN 'Pending Submission-Other'
    WHEN submission_status = 'Marked As Submitted'
    OR submission_status = 'Submitted' THEN 'Submitted-Other'
    ELSE submission_status END AS new_submision_status,
     package_number,
    document_number,
    title,
    revision,
    submission_status,
    review_status,
    assigned_to_org,
    due_in,
    due_out,
    milestone_date,
    actual_in,
    actual_out,
    as_built_required,
    assigned_to,
    sdr_codes,
    document_identifier,
    days_late,
    description,
    file_name,
    planned_submission_date,
    required_by,
    review_source,
    file_classification,
    supplier,
    purchase_order_number,
    jip33,
    status,
    submission_sequence,
    supplied_by,
    document_type_name,
    discipline,
    version,
    is_current as current_flag,
    s.dim_project_rls_key AS project_id,
    p.projectname as dim_project_rls_name,
    p.projectname + ' - ' + 'Supplier Submission Summary' AS report_title,
    cast(s.dim_snapshot_date as timestamp) AS snapshot_date,
    dateadd(day, -2, date_trunc('Week', CURRENT_DATE)) as Week_Start_Date,
    dateadd(day, 6,dateadd(day, -2, date_trunc('Week', CURRENT_DATE))) as Week_End_Date
from
    {{ source('document_control_domain_integrated_model', 'transformed_supplier_bot') }} s
left join {{ source('document_control_domain_integrated_model', 'transformed_projects') }} p on  p.dim_project_rls_key = s.dim_project_rls_key
)tbl