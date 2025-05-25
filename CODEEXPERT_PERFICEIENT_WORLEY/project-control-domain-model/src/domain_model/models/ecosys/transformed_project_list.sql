{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_DATE" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False',
        custom_location=target.location ~ 'ecosys/project_list/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["ecosys","v2"]
        )
}}

select
        secorg ,
        project ,
        project_internal_id ,
        systemdate ,
        latestsnapshotdate ,
        projectname ,
        projecttype ,
        client ,
        scope ,
        status ,
        portfolio_project_grouping ,
        pm ,
        pc ,
        customer_pm ,
        customer_rep ,
        project_size_classification ,
        projectapprovalstatus ,
        projectstartdate ,
        projectenddate ,
        billingtype ,
        projectriskclassification ,
        scopeofservice ,
        countryof_asset ,
        project_execution_location ,
        assettype ,
        publishedto_powerbi ,
        major_project ,
        h_wp_coa_ct_library ,
        execution_date ,
        source_system_name ,
        row_hash ,
        primary_key ,
        is_current ,
        eff_start_date ,
        eff_end_date ,
        {{run_date}} as created_date,
		{{run_date}} as updated_date,
		{{ generate_load_id(model) }} as load_id
from
    {{ source('curated_ecosys', 'curated_project_list') }}
where
    is_current = 1