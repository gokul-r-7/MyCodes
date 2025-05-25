{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    proj_id as project_id,
    po_id as po_number,
    description,
    cast(highest_issued_rev as integer),
    sub_project,
    supplier_no,
    supplier,
    cust_po_no as customer_po_number,
    tax_status,
    issue_date,
    buyer,
    po_commitment_value as total_po_commitment,
    tot_invoice_to_commitment as total_invoices_applied_to_commitment,
    tot_inv_tax_and_freight as total_taxes_and_freight,
    total_invoices_applied,
    commited_currency,
    invoice_number,
    invoice_date,
    received as received_date,
    approve_date,
    approved_by,
    acc_supp_and_loc as accounting_supplier_and_location,
    payment_terms,
    payment_due_date,
    inv_applied_to_commitment as invoice_applied_to_commitment,
    tax_and_freight,
    invoice_amount,
    payment_amount,
    payment_date,
    payment_number,
    status,
    etl_load_date as dataset_refreshed_date
from
    {{ source('supply_chain_dim', 'transformed_erm_invoices_data') }}
