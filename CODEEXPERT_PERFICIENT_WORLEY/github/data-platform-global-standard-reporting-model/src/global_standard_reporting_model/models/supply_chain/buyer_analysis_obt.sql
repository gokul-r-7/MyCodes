{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT 
    DISTINCT
        project_id,
        po_number,
        po_line_number as po_line_item_number,
        title as po_line_description,
        buyer,
        item_description,
        catg_supplier as supplier_name,
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
        cast(req_processing_time as integer) as req_processing_time,
        cast(po_approval_time as integer) as po_approval_time,
        polt_v1,
        code_of_account,
        incoterm_tax_terms,
        item_tag,
        usd_line_unit_conv as line_item_unit_price,
        po_line_quantity,
        usd_line_total_conv as line_value,
        received_quantity,
        received_quantity*line_item_unit_price*usd_conv as received_value,
        supplier_location,
        worley_project_region,
        project_sub_region,
        erm_supplier_country as project_country ,
        worley_project_office,
        sector,
        client,
        paper,
        group_l1,
        category_l2,
        sub_category_l3,
        family_l4,
        unspsc as worley_code_l5,
        po_stts_flag,
        received_date,
        EXTRACT(year FROM received_date) as received_date_year,
        EXTRACT(month FROM received_date) as received_date_month,
        'Qtr '|| EXTRACT(quarter FROM received_date) as received_date_quarter,
        to_char(received_date , 'Month') as received_date_month_name,
        data_refreshed_date ,
        getdate() as dag_execution_date
from
    {{ ref('erm_procurement_obt') }}
