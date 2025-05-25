{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_engreg_hold_index/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}

select
    cast(id as varchar(500)) as hold_id,
    cast(document_drawingno as varchar(500)) as document_drawing_no,
    cast(holddescription as varchar(2000)) as hold_description,
    cast(reasonforhold as varchar(500)) as reason_for_hold,
    cast(impact_risks_for_hold as varchar(500)) as impacts_risks_of_hold,
    cast(responsible_engineer as varchar(500)) as responsible_engineer,
    cast(lead_discipline as varchar(500)) as lead_discipline,
    cast(tagno as varchar(500)) as tag_no,
    cast(date_hold_placed as date) as date_hold_placed,
    cast(customer_approval as varchar(500)) as customer_approval,
    cast(plannedholdremovaldate as date) as planned_hold_removal_date,
    cast(actualholdremoved as date) as actual_hold_removed_date,
    cast(status as varchar(500)) as status,
    cast(remarks as varchar(500)) as remarks,
    cast(criticallevel as varchar(500)) as critical_level,
    cast(datelistedonconstrplan as date) as date_listed_on_constr_plan,
    cast(area as varchar(500)) as area,
    cast(arealead as varchar(500)) as area_lead,
    cast(notes as varchar(500)) as notes,
    cast('' as varchar(100)) as error_code,
    -- missing column
    cast(wbs as varchar(1000)) as wbs,
    cast(impacts_x002f_risksmitigation_x0 as varchar(1000)) as impacts_x002f_risksmitigation_x0,
    cast(forecastholdremovaldate as date) as forecast_hold_removal_date,
    cast(execution_date as date) as extracted_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('VGCP2' as varchar(100)) as project_code,
	source_system_name
from
    {{ source('curated_engreg', 'curated_holds_index') }}