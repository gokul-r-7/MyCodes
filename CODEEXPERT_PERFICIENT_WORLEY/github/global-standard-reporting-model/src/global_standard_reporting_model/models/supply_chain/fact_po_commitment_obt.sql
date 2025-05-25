{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    proj_id as project_id,
    po_id as po_number,
    title as description,
    ver as highest_issued_rev,
    sub_project,
    suppl_name as supplier,
    client_po_no as customer_po_number,
    issue_date,
    contract_delivery_date,
    ros_date,
    approve_date,
    incoterm,
    delivery_location,
    merc_hand_usr_id as buyer,
    net_po_price as total_po_commitment,
    cur_id as commited_currency,
    po_close_date,
    received_total,
    pp_inv_amt as progress_payment,
    inv_tot_against_commit as invoice_total_against_commitment,
    inv_tax_fright as invoice_total_not_booked_against_commitment,
    inv_tot_against_commit + inv_tax_fright as invoice_total,
    net_po_price - received_total as commitment_remaining,
    pct_po_complete as "%po_complete",
    received_total - (inv_tot_against_commit + inv_tax_fright) as accrual,
    customer,
    entity,
    bg_expiry,
    bg_needed,
    bg_description,
    bg_arrival,
    user_accepting_bg,
    etl_load_date as dataset_refershed_date
from
    {{ source('supply_chain_dim', 'erm_po_commitment') }}
WHERE
    inv_stat_flg = 0
