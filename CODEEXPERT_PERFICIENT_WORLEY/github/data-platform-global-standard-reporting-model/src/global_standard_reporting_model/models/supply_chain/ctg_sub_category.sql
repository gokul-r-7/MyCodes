{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    DISTINCT sub_category,
    sub_category_lv
FROM
    {{ ref('category_running_total') }}
where
    sub_category is not null