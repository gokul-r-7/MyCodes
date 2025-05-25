{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_unit__c/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}


with uc_cdc as (
    select *  from {{ source('curated_salesforce', 'curated_unit__c') }} uc
    where uc.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(uc.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

uc_data AS (
    select 
            id,
			ownerid,
			isdeleted,
			name,
			currencyisocode,
			recordtypeid,
			createddate,
			createdbyid,
			lastmodifieddate,
			lastmodifiedbyid,
			systemmodstamp,
			lastvieweddate,
			lastreferenceddate,
			activec,
			business_unitc,
			ecrmigrationexternalidc,
			legacyc,
			line_of_businessc,
			sales_plan_gross_margin_q1c,
			sales_plan_gross_margin_q2c,
			sales_plan_gross_margin_q3c,
			sales_plan_gross_margin_q4c,
			sales_plan_gross_marginc,
			sales_plan_hours_q1c,
			sales_plan_hours_q2c,
			sales_plan_hours_q3c,
			sales_plan_hours_q4c,
			sales_plan_hoursc,
			sales_plan_revenue_q1c,
			sales_plan_revenue_q2c,
			sales_plan_revenue_q3c,
			sales_plan_revenue_q4c,
			sales_plan_revenuec,
			sales_regionc,
			sectorc,
			bl_shortc,
			business_linec,
			countryc,
			executing_officec,
			fin_systemc,
			l2c,
			legacy_ecr_puc,
			officec,
			sales_plan_gross_margin_audc,
			sales_plan_gross_margin_q1_audc,
			sales_plan_gross_margin_q2_audc,
			sales_plan_gross_margin_q3_audc,
			sales_plan_gross_margin_q4_audc,
			sales_plan_revenue_audc,
			sales_plan_revenue_q1_audc,
			sales_plan_revenue_q2_audc,
			sales_plan_revenue_q3_audc,
			sales_plan_revenue_q4_audc,
			business_segmentc,
			gbs_businessc,
			gbs_locationc,
			gbs_performance_unitc,
			gbs_regionc,
			selling_businessc,
			fiscal_yearc,
			descriptionc,
			ecr_l0c,
			gbs_market_subsectorc,
			market_sector_l1c,
			market_segment_l3c,
			market_subsector_l2c,
            cast(execution_date as date) as etl_load_date,
	        CAST({{ run_date }} AS TIMESTAMP) AS model_created_date,
        	CAST({{ run_date }} AS TIMESTAMP) AS model_updated_date,
            {{ generate_load_id(model) }} as model_load_id	
            from uc_cdc
)

SELECT  * FROM uc_data
