
{{



     config(

        materialized = "table",
        tags=["construction"])

}}




 select * from {{ source('domain_integrated_model', 'o3_region') }}


