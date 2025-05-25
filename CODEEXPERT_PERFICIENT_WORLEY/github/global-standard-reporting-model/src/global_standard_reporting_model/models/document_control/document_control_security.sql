{{
 config(
    materialized='incremental',
    unique_key= ['project_id','user_name'],
    incremental_strategy='merge',
    tags=['document_control']
 )
}}
    select Distinct
    dim_project_rls_key as project_id,
    'AWSIDC:' || user_email as user_name
    from {{ source('document_control_domain_integrated_model', 'people') }}

--{% if is_incremental() %}

 ---   where meta_project_rls_key || '-' || 'AWSIDC:' || assignee_email not in (
   -- select meta_project_rls_key || '-' || 'AWSIDC:' || assignee_email from {{ this }}

--)

--{% endif %}