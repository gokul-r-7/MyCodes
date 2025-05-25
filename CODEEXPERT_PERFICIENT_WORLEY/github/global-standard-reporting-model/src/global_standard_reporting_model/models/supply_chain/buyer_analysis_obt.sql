{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT 
    DISTINCT
        project_id,
        po_number as "PO Number",
        po_line_number as "PO Line Item Number",
        title as "PO Line Description",
        buyer,
        item_description,
        supplier as supplier_name,
        req_creation_date,
        po_creation_date,
        EXTRACT(year FROM po_creation_date) as po_creation_date_year,
        EXTRACT(month FROM po_creation_date) as po_creation_date_month,
        'Qtr '|| EXTRACT(quarter FROM po_creation_date) as po_creation_date_quarter,
        to_char(po_creation_date , 'Month') as po_creation_date_month_name,
        po_issue_date,
        EXTRACT(year FROM po_issue_date) as po_issue_date_year,
        EXTRACT(month FROM po_issue_date) as po_issue_date_month,
        'Qtr '|| EXTRACT(quarter FROM po_issue_date) as po_issue_date_quarter,
        to_char(po_issue_date , 'Month') as po_issue_date_month_name,
        promised_date,
        req_processing_time,
        po_approval_time,
        polt_v1,
        code_of_account,
        incoterm_tax_terms,
        item_tag,
        usd_line_unit_conv as "Line Item Unit Price",
        po_line_quantity,
        usd_line_total_conv as "Line Value",
        received_quantity,
        received_quantity*line_item_unit_price*usd_conv as "Received Value",
        supplier_location,
        worley_project_region,
        project_sub_region,
        project_country,
        worley_project_office,
        sector,
        client,
        paper,
        group_l1,
        category_l2,
        sub_category_l3,
        family_l4,
        unspsc as "Worley Code L5",
        po_stts_flag,
        received_date,
        EXTRACT(year FROM received_date) as received_date_year,
        EXTRACT(month FROM received_date) as received_date_month,
        'Qtr '|| EXTRACT(quarter FROM received_date) as received_date_quarter,
        to_char(received_date , 'Month') as received_date_month_name,
        data_refreshed_date 
from
    {{ ref('erm_procurement_obt') }}
WHERE  po_stts_flag = 1