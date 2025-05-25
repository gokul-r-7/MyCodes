{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    proj_id as project_id,
    po_id as PO_Number,
    description,
    highest_issued_rev,
    sub_project,
    supplier_no,
    supplier,
    cust_po_no as Customer_PO_Number,
    tax_status,
    issue_date,
    buyer,
    po_commitment_value as Total_PO_Commitment,
    tot_invoice_to_commitment as Total_Invoices_Applied_to_Commitment,
    tot_inv_tax_and_freight as Total_Taxes_and_Freight,
    total_invoices_applied,
    commited_currency,
    invoice_number,
    invoice_date,
    received as Received_Date,
    approve_date,
    approved_by,
    acc_supp_and_loc as Accounting_Supplier_and_Location,
    payment_terms,
    payment_due_date,
    inv_applied_to_commitment as Invoice_Applied_to_Commitment,
    tax_and_freight,
    invoice_amount,
    payment_amount,
    payment_date,
    payment_number,
    status,
    etl_load_date as dataset_refershed_date
from
    {{ source('supply_chain_dim', 'erm_invoices_data') }}
