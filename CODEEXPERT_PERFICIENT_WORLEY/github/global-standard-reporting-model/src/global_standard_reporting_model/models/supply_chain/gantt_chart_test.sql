{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    t.project_id,
    t.po_number,
    t.po_number|| '-'||t.po_line_number as po_line,
    t.received_date,
    t.promised_date,
    d.date_key,
    CASE
    when d.date_key between t.received_date
    and t.promised_date then 1
    else 0 end as duration
FROM
    {{ ref('erm_procurement_obt') }} t
    LEFT JOIN {{ ref('dim_date_new') }} d ON 1 = 1
WHERE
    d.date_key between t.received_date
    and t.promised_date
