{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )
}}

SELECT
    DISTINCT milestones,
    cast(max(milestone_seq) as integer) as milestoneseq,
    DENSE_RANK() OVER (
        ORDER BY
            milestoneseq ASC
    ) AS sort_key
FROM
    {{ source('supply_chain_dim', 'transformed_po_hdr_details_cext_w_exp_sch_api_nv') }}
where
    milestones is not null
GROUP BY
    milestones
order by sort_key