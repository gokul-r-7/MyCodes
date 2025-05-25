{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = dbt.date_trunc("second", dbt.current_timestamp()) %}
  

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_account_group__c/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}


with agc_cdc as (
    select *  from {{ source('curated_salesforce', 'curated_account_group__c') }} agc
    where agc.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(agc.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

agc_data AS (
    select 
            id,
			ownerid,
			isdeleted,
			name,
			currencyisocode,
			createddate,
			createdbyid,
			lastmodifieddate,
			lastmodifiedbyid,
			systemmodstamp,
			lastvieweddate,
			lastreferenceddate,
			account_group_customer_typec,
			account_group_lob_stewardship_newc,
			account_group_tier_newc,
			account_group_sharepoint_urlc,
			ecrmigrationexternalidc,
			in_perimeterc,
			account_group_leadc,
			current_fy_account_plan_finalizedc,
			regular_account_team_meeting_cadencec,
            cast(execution_date as date) as etl_load_date,
			CAST({{run_date}} as timestamp) as model_created_date,
            CAST({{run_date}} as timestamp) as model_updated_date,
            {{ generate_load_id(model) }} as model_load_id	
            from agc_cdc
)

SELECT  * FROM agc_data
