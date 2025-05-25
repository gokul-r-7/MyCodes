{{ config(materialized='table') }}

select 
 *
 FROM
   {{ source('curated_vg_e3d', 'curated_pipecompwbsrep') }}