{{
 config(
    materialized='incremental',
    unique_key= ['project_id','user_name'],
    on_schema_change='fail',
    incremental_strategy='merge',
    tags=['document_control']
 )
}}
    select Distinct
    cast(dim_project_rls_key as varchar(100)) as project_id,
    'AAD:' || user_email as user_name
    from {{ source('document_control_domain_integrated_model', 'transformed_people') }}

--{% if is_incremental() %}

 ---   where meta_project_rls_key || '-' || 'AWSIDC:' || assignee_email not in (
   -- select meta_project_rls_key || '-' || 'AWSIDC:' || assignee_email from {{ this }}

--)

--{% endif %}