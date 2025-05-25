{{
    config(
        materialized='incremental'
    )
}}

select
   name,
   type,
   lock,
   owner,
   project,
   workpack_no,
   isometric,
   support_type,
   bore_inch,
   "tag",
   profile,
   length_mm,
   quantity,
   client_doc_num,
   drawing_no,
   pid_line_no,
   zone,
   site,
   execution_date,
   source_system_name
from dev.vg_e3d.curated_vg_e3d_vglpipesupport_sheet

   {%if is_incremental() %}
  where execution_date >= (select coalesce(max(execution_date),'1900-01-01') from {{ this }} )
{% endif %}