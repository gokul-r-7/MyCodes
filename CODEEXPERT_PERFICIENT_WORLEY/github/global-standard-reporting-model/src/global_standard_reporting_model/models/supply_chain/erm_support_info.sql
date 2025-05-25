{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
    *
FROM
     {{ source('supply_chain_dim', 'erm_support_info_criteria_calculations') }}
ORDER by
    sr_no