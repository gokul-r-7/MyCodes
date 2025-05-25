{{ config(materialized='table') }}
 
select * from
{{ source('curated_vg_ecosys', 'curated_ecosys_calendar') }}