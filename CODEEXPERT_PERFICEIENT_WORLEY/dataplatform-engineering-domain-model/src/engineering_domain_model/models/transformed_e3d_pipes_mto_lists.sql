{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_e3d_pipes_mto_lists/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}


select
    cast(pipe as varchar(500)) as pipe,
    cast(site as varchar(500)) as site,
    cast(zone as varchar(500)) as zone,
    cast(bore as varchar(500)) as bore,
    cast(pspec as varchar(500)) as pspec,
    cast(ispec_pipe as varchar(500)) as ispec,
    cast(tspec as varchar(500)) as tspec,
    cast(status as varchar(500)) as status,
    cast(duty as varchar(500)) as duty,
    cast(syscode as varchar(500)) as syscode,
    cast(lsno as varchar(500)) as lsno,
    cast(rev as varchar(500)) as rev,
    cast(lock as varchar(500)) as lock,
    cast(pid as varchar(500)) as pid,
    cast(cwa as varchar(500)) as cwa,
    cast(cwp as varchar(500)) as cwp,
    cast(isodwgno as varchar(500)) as isodwgno,
    cast(clientdocno as varchar(500)) as clientdocno,
    cast(execution_date as date) as extracted_date,
	source_system_name,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id,
    cast('VGCP2' as varchar(30)) as project_code
	
from
    {{ source('curated_e3d', 'curated_pipelistrep') }}