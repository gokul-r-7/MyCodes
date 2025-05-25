{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_engreg_td_index/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    cast(tdid as varchar(500)) as td_id,
    cast(title as varchar(500)) as td_proposal_number,
    -- missing column
    cast(rev as varchar(500)) as rev,
    cast(tdtitle as varchar(500)) as td_title,
    cast(originator as varchar(500)) as originator,
    cast(discipline as varchar(500)) as discipline,
    cast(td_recipient_name as varchar(500)) as td_recipient_name,
    cast(td_recipient_company as varchar(500)) as td_recipient_company,
    cast(date_issued as date) as date_issued,
    cast(due_date_for_response as date) as due_date_for_response,
    cast(date_for_response as date) as date_of_response,
    cast(pdn_no as varchar(500)) as pdn_no,
    cast(status as varchar(500)) as status,
    cast('' as varchar(100)) as error_code,
    -- missing column
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('VGCP2' as varchar(30)) as project_code,
	source_system_name
from
    {{ source('curated_engreg', 'curated_td_index') }}