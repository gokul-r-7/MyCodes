{{ config(materialized='table') }}

SELECT * 
FROM  
{{ source('curated_vg_ecosys', 'curated_ecosys_header') }}