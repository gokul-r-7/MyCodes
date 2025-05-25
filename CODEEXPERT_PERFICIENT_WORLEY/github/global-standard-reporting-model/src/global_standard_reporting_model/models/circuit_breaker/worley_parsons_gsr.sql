{{
     config(
         materialized = "table",
         tags=["circuit_breaker"]
     )
}}

Select *
FROM {{ source('circuit_breaker_domain_integrated_model', 'worley_parsons_gsr') }}