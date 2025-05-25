{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}

SELECT
    DISTINCT milestone_name,
    max(milestone_sequence) as milestoneseq,
    DENSE_RANK() OVER (
        ORDER BY
            milestoneseq ASC
    ) AS sort_key
FROM
    {{ ref('fact_requisition_data_obt') }}
where
    milestone_name is not null
GROUP BY
    milestone_name