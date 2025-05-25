{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_engreg_seal_decision/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select

    cast(id as varchar(500)) as decision_number,
    cast(decision_owner as varchar(500)) as decision_owner,
    cast(decision_title as varchar(500)) as decision_title,
    cast(studies_reviews_required as varchar(500)) as studies_reviews_required_to_inform_decision,
    cast(optionsconsidered as varchar(500)) as options_considered,
    cast(keysafetyimplications as varchar(500)) as key_safety_sustainability_implications,
    cast(decisionoutcome as varchar(500)) as decision_outcome,
    cast(approved_by_pm as date) as approved_by_pm,
    cast(approved_by_customer as varchar(500)) as approved_by_customer,
    cast('' as varchar(100)) as error_code,
    -- missing column
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('VGCP2' as varchar(100)) as project_code,
	source_system_name
from
    {{ source('curated_engreg', 'curated_seal_decisions') }}