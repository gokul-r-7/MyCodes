{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
    DISTINCT category,
    category_lv
FROM
    {{ ref('category_running_total') }}
where
    category is not null