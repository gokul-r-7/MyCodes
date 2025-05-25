{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    projet_no as project_no,
    project as project_id,
    sub_project,
    tmr_number,
    purchase_order_no as po_number,
    latest_revision as latest_rev,
    customer_po_no as customer_po,
    client_reference as client_reference_no,
    description,
    supplier,
    buyer,
    expediter,
    null as inspector,
    po_first_issue_date as po_1st_issue,
    issue_date as latest_issue_date,
    total_line_items,
    last_contact_date,
    next_contact_date,
    earliest_contract_dlv_date as earliest_contract_delivery_date,
    earliest_forecast_dlv_date as earliest_forecast_delivery_date,
    earliest_ros,
    earliest_forecast_arrival_date as earliest_forecast_arrival_date,
    incoterm,
    trans_terms,
    expediting_level,
    inspection_level,
    contractual_dlv_date as contract_delivery_date,
    expedite_comment as expediting_comments,
    ros_date as ros_date,
    items_not_released,
    released_items as total_items_released,
    milestones as milestone,
    milestone_seq,
    baseline as baseline_date,
    forecast_start as milestone_forecast_start_date,
    forecast_complete as milestone_forecast_complete_date,
    actual_date as milestone_actual_date,
    deadline_float as milestone_float,
    duration_before_milestone,
    milestone_duration,
    remark as remarks,
    template_no,
    template,
    template_name,
    etl_load_date as dataset_refershed_date
from
    {{ source('supply_chain_dim', 'po_hdr_details_cext_w_exp_sch_api_nv') }}
