{%- set run_date = "CURRENT_TIMESTAMP()" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_erm_user_project_access/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true
        ) 
}}



        select distinct 
                p.proj_no, 
                p.proj_id, 
                p.descr AS projdescr,  
                u.usr_id,
                u.name AS username, 
                u.email,
                u.def_date, 
                u.upd_date,
                coalesce(c.name,cac.name) country,
                coalesce(p.home_office_addr_region,ca.addr_region) region,
                CVLI.VALUE worley_project_office,
                CVLI_R.VALUE sub_region,
                cast(p.execution_date as date) as etl_load_date,
                {{run_date}} as model_created_date,
                {{run_date}} as model_updated_date,
                {{ generate_load_id(model) }} as model_load_id
        FROM {{ source('curated_erm', 'usr') }} u 
            left join {{ source('curated_erm', 'ar_usr_grp_usr') }} augu on u.usr_id = augu.usr_id 
            right join {{ source('curated_erm', 'ar_usr_grp_role') }} augr on augr.ar_usr_grp_no = augu.ar_usr_grp_no 
            right join {{ source('curated_erm', 'ar_role') }} ar on ar.ar_role_no = augr.ar_role_no
            right join {{ source('curated_erm', 'proj') }} p on p.proj_no = nvl(augr.proj_no,p.proj_no)
            left join {{ source('curated_erm', 'country') }} c ON p.home_office_addr_country_id=c.country_id
            left join {{ source('curated_erm', 'cont_addr') }} ca ON p.client_addr_no=ca.cont_addr_no
            left join {{ source('curated_erm', 'country') }} cac ON ca.ADDR_country_id=cac.country_id
            join {{ ref('transformed_erm_psr_project_details') }} psh ON p.proj_no=psh.proj_no
            left join {{ source('curated_erm', 'proj_cpv') }} pc ON p.proj_no=pc.proj_no
            left join {{ source('curated_erm', 'cusp_value_list_item') }} cvli ON pc.proj_office=cvli.value_list_item_no
            left join {{ source('curated_erm', 'cusp_value_list_item') }} cvli_R ON pc.sub_region=cvli_R.value_list_item_no
        where u.usr_id is not null 
        and u.stat=1
        and u.name is not null
        order by p.proj_no