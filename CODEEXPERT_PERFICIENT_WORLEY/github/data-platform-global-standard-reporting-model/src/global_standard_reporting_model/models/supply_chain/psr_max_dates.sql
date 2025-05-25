{{
     config(
         materialized = "table",
         tags=["supply_chain"]
     )

}}


WITH ActualDates AS (
    SELECT 
        MAX(milestone_actual_date) AS Max_Date
    FROM 
        {{ ref('fact_requisition_data_obt') }}
    WHERE 
        Commitment_Value IS NOT NULL
        AND Milestone_Sequence = 27000
),
PlannedDates AS (
    SELECT 
        MAX(milestone_planned_date) AS Max_Date
    FROM 
        {{ ref('fact_requisition_data_obt') }}
    WHERE 
        Budget IS NOT NULL
        AND Milestone_Sequence = 27000
)
SELECT 
    Max_Date
FROM 
    ActualDates

UNION

SELECT 
    Max_Date
FROM 
    PlannedDates
