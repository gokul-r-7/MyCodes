{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/transformed_ecosys_org_hierarchy/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

with secorg_cte as 
(
    select
        '' as id,
        secorgid as org_id,
        secorgname as org_name,
        secorgtype as org_type,
        parentsecorgid as org_parent_id,
        hierarchypathid as path,
        split_part(hierarchypathid,'.',1) as hq_id,
        split_part(hierarchypathid,'.',2) as region_id,
        split_part(hierarchypathid,'.',3) as sub_region_id,
        split_part(hierarchypathid,'.',4) as location_l1_id,
        split_part(hierarchypathid,'.',5) as location_l2_id,
        split_part(hierarchypathid,'.',6) as location_l3_id,
        split_part(hierarchypathid,'.',7) as location_l4_id,
        split_part(hierarchypathid,'.',8) as location_l5_id,
        split_part(hierarchypathid,'.',9) as location_l6_id,
        true as active_indicator,
        execution_date ,
        source_system_name
    from {{ source('curated_ecosys', 'curated_ecosys_secorglist') }}
    where is_current = 1
    and secorgid is not null
)
select
    s.*,
    s1.org_name as region,
    coalesce(s2.org_name , region || ' (No Detail)') as sub_region,
    coalesce(s3.org_name , coalesce(s2.org_name,s1.org_name) || ' (No Detail)') as location_l1,
    coalesce(s4.org_name , coalesce(s3.org_name,s2.org_name,s1.org_name) || ' (No Detail)') as location_l2,
    coalesce(s5.org_name , coalesce(s4.org_name,s3.org_name,s2.org_name,s1.org_name) || ' (No Detail)') as location_l3,
    coalesce(s6.org_name , coalesce(s5.org_name,s4.org_name,s3.org_name,s2.org_name,s1.org_name) || ' (No Detail)') as location_l4,
    coalesce(s7.org_name , coalesce(s6.org_name,s5.org_name,s4.org_name,s3.org_name,s2.org_name,s1.org_name) || ' (No Detail)')as location_l5,
    coalesce(s8.org_name , coalesce(s7.org_name,s6.org_name,s5.org_name,s4.org_name,s3.org_name,s2.org_name,s1.org_name) || ' (No Detail)') as location_l6,
    {{run_date}} as created_date,
    {{run_date}} as updated_date,
    {{ generate_load_id(model) }} as load_id
from secorg_cte s
left join secorg_cte s1 on s.region_id = s1.org_id
left join secorg_cte s2 on s.sub_region_id = s2.org_id
left join secorg_cte s3 on s.location_l1_id = s3.org_id
left join secorg_cte s4 on s.location_l2_id = s4.org_id
left join secorg_cte s5 on s.location_l3_id = s5.org_id
left join secorg_cte s6 on s.location_l4_id = s6.org_id
left join secorg_cte s7 on s.location_l5_id = s7.org_id
left join secorg_cte s8 on s.location_l6_id = s8.org_id