{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


 SELECT DISTINCT
    project_id,
    po_number,
    worley_office_origin,
    po_creation_date,
    client,
    catg_supplier as supplier,
    supplier_location,
    po_issue_date,
    cast(po_line_number as integer) as po_line_number,
    item_description,
    received_date,
    cast(polt_v1 as integer) as polt_v1 ,
    cast(polt_v2 as integer) as polt_v2,
    worley_project_region,
    project_sub_region,
    erm_supplier_country as project_country,
    sector,
    received_date_issue,
    EXTRACT(year FROM po_creation_date) as po_creation_date_year,
    EXTRACT(month FROM po_creation_date) as po_creation_date_month,
    'Qtr '|| EXTRACT(quarter FROM po_creation_date) as po_creation_date_quarter,
    to_char(po_creation_date , 'Month') as po_creation_date_month_name,
    EXTRACT(year FROM po_issue_date) as po_issue_date_year,
    EXTRACT(month FROM po_issue_date) as po_issue_date_month,
    'Qtr '|| EXTRACT(quarter FROM po_issue_date) as po_issue_date_quarter,
    to_char(po_issue_date , 'Month') as po_issue_date_month_name,
    EXTRACT(year FROM received_date) as received_date_year,
    EXTRACT(month FROM received_date) as received_date_month,
    'Qtr '|| EXTRACT(quarter FROM received_date) as received_date_quarter,
    to_char(received_date , 'Month') as received_date_month_name,
    po_stts_flag,
    po_total_conv,
    data_refreshed_date as refreshed_date
from {{ ref('erm_procurement_obt') }}