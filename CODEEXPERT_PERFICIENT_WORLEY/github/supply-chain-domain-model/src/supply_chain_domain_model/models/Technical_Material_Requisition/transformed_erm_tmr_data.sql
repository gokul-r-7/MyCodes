{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_tmr_data/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



SELECT proj.proj_id,
      th.tmr_id tmr_number
      ,dd.draw_discipline_id discipline_id
      ,dd.descr discipline
      ,cpv.sub_project
      ,th.title
      ,th.tech_hand_usr_id tmr_originator
      ,th.deadline ros_date
      ,inc.descr internal_status
      ,wp.workflow_purpose_id issue_purpose
      ,cvli.value origin
      ,th.tmr_no,
      cast(th.execution_date as date) as etl_load_date,
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
FROM {{ source('curated_erm', 'tmr_hdr') }} th
JOIN {{ source('curated_erm', 'proj') }} proj
    ON th.proj_no = proj.proj_no
    AND proj.is_current = 1
JOIN {{ source('curated_erm', 'draw_discipline') }} dd
    ON th.draw_discipline_no = dd.draw_discipline_no
    AND dd.is_current = 1
JOIN {{ source('curated_erm', 'proc_int_stat') }} inc
    ON th.proc_int_stat_no = inc.proc_int_stat_no
    AND inc.is_current = 1
JOIN {{ source('curated_erm', 'workflow_purpose') }} wp
    ON th.workflow_purpose_no = wp.workflow_purpose_no
    AND wp.is_current = 1
LEFT JOIN {{ source('curated_erm', 'tmr_hdr_cpv') }} cpv
    ON th.tmr_no = cpv.tmr_no
    AND cpv.is_current = 1
LEFT JOIN {{ source('curated_erm', 'cusp_value_list_item') }} cvli
    ON cpv.origin = cvli.value_list_item_no
    AND cvli.is_current = 1