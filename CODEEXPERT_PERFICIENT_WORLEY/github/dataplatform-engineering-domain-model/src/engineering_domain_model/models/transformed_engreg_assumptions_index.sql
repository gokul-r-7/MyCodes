{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_engreg_assumptions_index/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    cast(id as varchar(500)) as assumption_id,
    cast(basis_for_assumption as varchar(2000)) as basis_for_assumption,
    cast(wbsarea as varchar(500)) as wbs_area,
    cast(designdeliverable_drawingno as varchar(500)) as document_drawing_no,
    cast(
        designdeliverable_drawingdescription as varchar(500)
    ) as design_deliverable_drawing_description,
    cast(designdeliverablerev as varchar(500)) as design_deliverable_rev,
    cast(keydocumenttocloseassumption as varchar(500)) as key_document_to_close_assumption,
    cast(assumption_added_by as varchar(500)) as assumption_added_by,
    cast(responsible_engineer as varchar(500)) as responsible_engineer,
    cast(lead_discipline as varchar(500)) as lead_discipline,
    cast(date_assumption_confirmed as date) as date_assumption_listed,
    cast(assumption_confirmed as varchar(500)) as assumption_confirmed,
    cast(date_assumption_confirmed as date) as date_assumption_confirmed,
    cast(datedesigndeliverableupdatedandi as date) as date_design_deliverable_updated_and_issued,
    cast(updatetoassumptionifinvalid as varchar(500)) as update_to_assumption_if_invalid,
    cast(status_x0028_open_x002f_closed_x as varchar(500)) as status,
    cast(remarks as varchar(500)) as remarks,
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('' as varchar(100)) as error_code,
    -- missing column
    cast('VGCP2' as varchar(100)) as project_code,
	source_system_name
from
    {{ source('curated_engreg', 'curated_assumptions_index') }}
