{{
    config(
    materialized = 'incremental',
    unique_key= ['project_id', 'user_name'],
    on_schema_change='fail',
    incremental_strategy='merge',
    tags=['supply_chain']
          )
}}
select Distinct
    cast(proj_id as varchar(100)) as project_id,
   'AAD:' || email as user_name
from
    {{ source('supply_chain_dim', 'transformed_erm_user_project_access') }}

