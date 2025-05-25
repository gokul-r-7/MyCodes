{{
    config(
        materialized='incremental'
    )
}}

--{{ config(materialized='table') }}

select 
 disc
,STATUS
,"PERCENT"
,CUMPERCENT
,PROJECT_CODE
,EXECUTION_DATE EXTRACTED_DATE
,DESCRIPTION
 FROM
   {{ source('curated_vg_e3d', 'curated_dim_memstatus') }}

   {% if is_incremental() %}
      where EXECUTION_DATE >= (select coalesce(max(EXECUTION_DATE),'1900-01-01') from {{ this }} )
   {% endif %}
