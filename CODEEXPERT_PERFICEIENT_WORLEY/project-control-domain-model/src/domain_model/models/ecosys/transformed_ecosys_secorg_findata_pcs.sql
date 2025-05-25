{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_secorg_findata_pcs/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

select
    findatapcs_projectinternalid as project_internal_id,
    secorg_id as secorgid,
    findatapcs_ownerorganizationid as owner_organizationid,
    findatapcs_costobjectid as cost_object_id,
    findatapcs_transactiondate as transaction_date,
    findatapcs_pcscomments as pcs_comments,
    findatapcs_pcsnumber as pcs_number,
    findatapcs_pcsstatus as pcs_status,
    cast(findatapcs_workingforecastunits as float) as working_forecast_units,
    findatapcs_pcspaymentterms as pcs_payment_term,
   	{{run_date}} as created_date,
	{{run_date}} as updated_date,
	{{ generate_load_id(model) }} as load_id
from
    {{ source('curated_ecosys', 'curated_ecosys_findatapcs') }}
where
    is_current = 1
