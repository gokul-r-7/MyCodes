{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}
SELECT
    *,
    CASE
    WHEN status_with_closed <> 'Closed' then 'Open'
    ELSE status_with_closed end as "open_or_close",
    CASE
    WHEN status <> 'Paid' AND payment_due_date < CURRENT_DATE AND payment_due_date IS NOT NULL THEN
            CASE
                WHEN (CURRENT_DATE - payment_due_date) <= 30 THEN '0-30 days overdue'
                WHEN (CURRENT_DATE - payment_due_date) <= 60 THEN '30-60 days overdue'
                WHEN (CURRENT_DATE - payment_due_date) <= 90 THEN '60-90 days overdue'
                WHEN (CURRENT_DATE - payment_due_date) > 90 THEN '>90 days overdue'
                ELSE status
            END
        ELSE status
    END AS status_with_past_due
FROM(
    SELECT DISTINCT
        cast(proj.project_code as varchar(100)) as project_id,
        inv.payment_due_date,
        inv.invoice_amount,
        inv.invoice_number,
        INITCAP(inv.status) as status,
        com.po_number,
        com.description,
        com.supplier,
        com.issue_date,
        com.ros_date,
        com.total_po_commitment,
        com.po_close_date,
        com.received_total,
        com.invoice_total_against_commitment,
        com.invoice_total_not_booked_against_commitment,
        com.invoice_total,
        com.po_complete_pct,
        com.customer,
        com.entity,
        sup.name_1 as supplier_name,
        ebu.business_units,
        CASE
        when com.po_complete_pct > 100 then 'Over Paid'
        else 'Compliant' end as overpaid_or_complaint,
        CASE
        when com.po_close_date is null THEN (
            CASE
            when com.po_complete_pct is null then 'In Progress'
            when com.po_complete_pct = 100 then 'Paid/Delivered Ready To Close'
            when com.po_complete_pct > 100 then 'Overpaid PO'
            else 'In Progress' end
        )
        else 'Closed' end as status_with_closed,
        EXTRACT(year FROM inv.issue_date) as issue_date_year,
        EXTRACT(month FROM inv.issue_date) as issue_date_month,
        'Qtr '|| EXTRACT(quarter FROM inv.issue_date) as issue_date_quarter,
        to_char(inv.issue_date , 'Month') as issue_date_month_name,
        EXTRACT(year FROM inv.payment_due_date) as payment_due_date_year,
        EXTRACT(month FROM inv.payment_due_date) as payment_due_date_month,
        to_char(inv.payment_due_date , 'Mon') as payment_due_date_month_name,
        inv.dataset_refreshed_date
    FROM
        {{ ref('fact_po_invoices_obt') }} inv
        LEFT JOIN {{ ref('fact_po_commitment_obt') }} com ON inv.po_number = com.po_number
        LEFT JOIN {{ source('supply_chain_dim', 'transformed_erm_suppl') }} sup ON inv.supplier_no = sup.suppl_id
        LEFT JOIN {{ source('supply_chain_dim', 'transformed_erm_project_dim') }} proj ON com.project_id = proj.project_code
        LEFT JOIN {{ source('supply_chain_dim', 'transformed_erm_business_unit') }} ebu ON com.project_id = ebu.project
)