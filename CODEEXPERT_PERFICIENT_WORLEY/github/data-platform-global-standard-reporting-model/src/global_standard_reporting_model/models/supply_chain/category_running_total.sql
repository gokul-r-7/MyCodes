{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    DISTINCT L.worley_code,
    L.family,
    L.category,
    L.sub_category,
    L5.worley_code_lv,
    L4.family_lv,
    L3.sub_category_lv,
    L2.category_lv
FROM
    {{ ref('category_management_obt') }} L
    LEFT JOIN (
        SELECT
            worley_code,
            SUM(worley_code_lv) OVER (
                ORDER BY
                    worley_code_lv DESC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) as worley_code_lv
        FROM
(
                SELECT
                    worley_code,
                    SUM(line_value) as worley_code_lv
                FROM
                    {{ ref('category_management_obt') }}
                GROUP by
                    worley_code
            )
    ) L5 ON L.worley_code = L5.worley_code
    LEFT JOIN (
        SELECT
            family,
            SUM(family_lv) OVER (
                ORDER BY
                    family_lv DESC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) as family_lv
        FROM
(
                SELECT
                    family,
                    SUM(line_value) as family_lv
                FROM
                    {{ ref('category_management_obt') }}
                GROUP by
                    family
            )
    ) L4 ON L.family = L4.family
    left JOIN (
        SELECT
            category,
            SUM(category_lv) OVER (
                ORDER BY
                    category_lv DESC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) as category_lv
        FROM
(
                SELECT
                    category,
                    SUM(line_value) as category_lv
                FROM
                    {{ ref('category_management_obt') }}
                GROUP by
                    category
            )
    ) L2 ON L.category = L2.category
    left JOIN(
        SELECT
            sub_category,
            SUM(sub_category_lv) OVER (
                ORDER BY
                    sub_category_lv DESC ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) as sub_category_lv
        FROM
(
                SELECT
                    sub_category,
                    SUM(line_value) as sub_category_lv
                FROM
                    {{ ref('category_management_obt') }}
                GROUP by
                    sub_category
            )
    ) L3 ON L.sub_category = L3.sub_category