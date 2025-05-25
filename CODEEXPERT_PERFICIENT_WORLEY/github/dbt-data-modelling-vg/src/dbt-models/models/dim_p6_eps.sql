{{ config(materialized='table') }}

SELECT * 
FROM  
{{ source('curated_vg_p6', 'curated_eps') }}