{{
     config(
         materialized = "table",
         tags=["circuit_breaker"]
     )
}}

Select *
FROM {{ source('circuit_breaker_domain_integrated_model', 'gsr_openit_monthly_summary_by_location') }}