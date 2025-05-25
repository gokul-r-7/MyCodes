{{
    config(
        materialized='table'
    )
}}

select 
distinct 
right(site,3) wbs
,substring(zone,2,2) cwa
, substring(zone,2,4) Cwpzone
, right(site,3)|| substring(zone,2,2) || substring(zone,2,4) wbs_pk
FROM {{ source('curated_vg_e3d', 'curated_vg_e3d_vglpipes_sheet') }}
UNION
select 
distinct 
right(site,3) wbs
,substring(zone,2,2) cwa
, substring(zone,2,4) Cwpzone
, right(site,3)|| substring(zone,2,2) || substring(zone,2,4) wbs_pk
FROM {{ source('curated_vg_e3d', 'curated_vg_e3d_vglstruc_sheet') }}
UNION
select 
distinct 
right(site,3) wbs
,substring(zone,2,2) cwa
, substring(zone,2,4) Cwpzone
, right(site,3)|| substring(zone,2,2) || substring(zone,2,4) wbs_pk
FROM {{ source('curated_vg_e3d', 'curated_vg_e3d_vglequip_sheet') }}

 --  {% if is_incremental() %}
 --     where execution_date >= (select coalesce(max(execution_date),'1900-01-01') from {{ this }} )
 --  {% endif %}


