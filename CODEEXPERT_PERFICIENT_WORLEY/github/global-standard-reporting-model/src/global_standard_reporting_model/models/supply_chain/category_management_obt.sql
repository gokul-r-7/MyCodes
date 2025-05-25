{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT DISTINCT
    project_id,
    po_number,
    purchase_order_line_item_internal_id,
    po_creation_date,
    DATE_PART('year', po_creation_date) as po_creation_date_year,
    'Qtr '|| DATE_PART('quarter', po_creation_date) as po_creation_date_quarter,
    TO_CHAR(po_creation_date, 'Mon') as po_creation_month_name,
    po_line_number,
    CASE
        when unspsc is null or unspsc =' ' then '00 Not Classified'
        else unspsc end as worley_code,
    worley_project_region,
    catg_supplier as supplier,
    erm_supplier_country as country,
    ctg_category_l2 as category,
    ctg_sub_category_l3 as sub_category,
    ctg_family_l4 as family,
    ctg_commodity_l5,
    usd_line_total_conv as line_value,
    data_refreshed_date as refreshed_date
from
    {{ ref('erm_procurement_obt') }}
