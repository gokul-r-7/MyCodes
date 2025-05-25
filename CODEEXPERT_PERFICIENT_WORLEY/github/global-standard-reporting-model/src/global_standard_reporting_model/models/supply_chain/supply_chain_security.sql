{{
    config(
        materialized = 'incremental',
        unique_key= ['project_id','user_name'],
        incremental_strategy='merge',
        tags=['supply_chain']
     )
}}

select
    distinct
   proj_id as project_id,
   concat('AWSIDC:',initcap(email)) as user_name
from
    {{ source('supply_chain_dim', 'erm_user_project_access') }}

