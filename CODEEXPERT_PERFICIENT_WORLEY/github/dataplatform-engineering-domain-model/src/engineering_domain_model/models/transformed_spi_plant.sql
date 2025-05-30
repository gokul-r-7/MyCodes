{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_spi_plant/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    cast(plant_id as numeric(38, 0)),
    cast(proj_id as numeric(38, 0)),
    cast(site_id as numeric(38, 0)),
    cast(chg_num as numeric(38, 0)),
    user_name,
    chg_status,
    chg_date,
    plant_name,
    addr1,
    addr2,
    city,
    state,
    zip,
    country,
    plant_note,
    cast(owner_id as numeric(38, 0)),
    location,
    cast(hu_item_lib_id as numeric(38, 0)),
    browse_dw_name,
    plant_type_flg,
    wire_tag_flg,
    cast(routing_length_uom_id as numeric(38, 0)),
    cast(routing_width_uom_id as numeric(38, 0)),
    cast(routing_spare_length_percent as numeric(38, 0)),
    cast(routing_spare_length_fix as numeric(38, 0)),
    cast(extra_routing_length as numeric(38, 0)),
    generic_terminology,
    cast(parent_id as numeric(38, 0)),
    plant_udf_c01,
    plant_udf_c02,
    plant_udf_c03,
    plant_udf_c04,
    plant_udf_c05,
    plant_udf_c06,
    plant_udf_c07,
    plant_udf_c08,
    plant_udf_c09,
    plant_udf_c10,
    plant_udf_c11,
    plant_udf_c12,
    plant_udf_c13,
    plant_udf_c14,
    plant_udf_c15,
    plant_udf_c16,
    plant_udf_c17,
    plant_udf_c18,
    plant_udf_c19,
    plant_udf_c20,
    plant_flg,
    pipe_standard,
    'VGCP2' as project_code,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast(execution_date as date) as extracted_date,
    source_system_name
from
    {{ source('curated_spi', 'curated_plant') }}