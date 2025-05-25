{{ config(materialized='table') }}

SELECT
    username , 
    NULLIF(wbs , 'nan') as wbs ,
    NULLIF(cwa , 'nan') as cwa ,
    NULLIF(cwpzone , 'nan') as cwpzone ,
    execution_date,
    source_system_name
FROM
{{ source('curated_vg_e3d', 'curated_dim_3dmodelusers') }}