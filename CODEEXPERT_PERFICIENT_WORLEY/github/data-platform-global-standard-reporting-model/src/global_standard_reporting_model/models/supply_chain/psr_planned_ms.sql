{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
        project_id,
        requisition_no,
        milestone_name,
        milestone_sequence,
        milestone_planned_date
FROM
    {{ ref('fact_requisition_data_obt') }}