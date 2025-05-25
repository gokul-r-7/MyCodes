{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


SELECT
    DISTINCT project_id
From
    {{ ref('fact_requisition_data_obt') }}