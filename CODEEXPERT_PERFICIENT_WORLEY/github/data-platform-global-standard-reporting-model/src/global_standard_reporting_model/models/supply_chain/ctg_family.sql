{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    DISTINCT family,
    family_lv
FROM
    {{ ref('category_running_total') }}
where
    family is not null