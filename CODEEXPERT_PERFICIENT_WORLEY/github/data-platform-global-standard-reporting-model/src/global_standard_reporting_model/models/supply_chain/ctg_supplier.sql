{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    supplier,
    SUM(supplier_cumulative) OVER (
        ORDER BY
            supplier_cumulative DESC ROWS BETWEEN UNBOUNDED PRECEDING
            AND CURRENT ROW
    ) as supplier_cumulative
FROM
    (
        SELECT
            supplier,
            SUM(line_value) as supplier_cumulative
        FROM
            {{ ref('category_management_obt') }}
        GROUP by
            supplier
    )