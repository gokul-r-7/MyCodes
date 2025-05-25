{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_multi_office_split__c/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}


with mosc_cdc as (
    select *  from {{ source('curated_salesforce', 'curated_multi_office_split__c') }} mosc
    where mosc.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(mosc.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

mosc_data AS (
    SELECT 
        id,
        isdeleted,
        name,
        currencyisocode,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        lastactivitydate,
        opportunityc,
        auto_adjust_datesc,
        average_gross_marginc,
        contains_legacy_pusc,
        datasourcec,
        ecrmigrationexternalidc,
        end_datec,
        executing_buc,
        executing_lobc,
        gm_per_dayc as gm_per_dayc,
        gross_marginc,
        CAST(hoursc AS FLOAT) AS hoursc,
        lagc,
        leadc,
        mean_datec,
        CAST(number_of_daysc AS FLOAT) AS number_of_daysc,
        CAST(number_of_monthsc AS FLOAT) AS number_of_monthsc,
        performance_unit_puc,
        probable_gm_per_dayc AS probable_gm_per_dayc,
        probable_gmc,
        resource_typec,
        revenuec  AS revenuec,
        spreading_formulac,
        CAST(standard_deviation_monthsc AS FLOAT) AS standard_deviation_monthsc,
        standard_deviationc,
        CAST(standard_deviation_in_daysc AS FLOAT) AS standard_deviation_in_daysc,
        start_datec,
        CAST(monthly_revenue_spreadc AS FLOAT) AS monthly_revenue_spreadc,
        revenue_spread_total_gmc AS revenue_spread_total_gmc,
        dnu_rstriggerc,
        advisian_service_linec,
        business_linec,
        crmt_business_linec,
        crmt_idc,
        crmt_officec,
        CAST(contract_revenuec AS FLOAT) AS contract_revenuec,
        global_service_linec,
        lead_scope_of_servicesc,
        scope_of_servicesc,
        worley_capability_sectorc,
        worley_capability_subsectorc,
        global_service_line_slc,
        pcg_business_linec,
        service_linec,
        CAST(gm_per_day_contractc AS FLOAT) AS gm_per_day_contractc,
        CAST(gm_per_day_corpc AS FLOAT) AS gm_per_day_corpc,
        CAST(gross_margin_contractc AS FLOAT) AS gross_margin_contractc,
        CAST(gross_margin_corpc AS FLOAT) AS gross_margin_corpc,
        CAST(probable_gm_contractc AS FLOAT) AS probable_gm_contractc,
        CAST(probable_gm_corpc AS FLOAT) AS probable_gm_corpc,
        CAST(probable_gm_per_day_contractc AS FLOAT) AS probable_gm_per_day_contractc,
        CAST(probable_gm_per_day_corpc AS FLOAT) AS probable_gm_per_day_corpc,
        CAST(revenue_contractc AS FLOAT) AS revenue_contractc,
        CAST(revenue_corpc AS FLOAT) AS revenue_corpc,
        CAST(revenue_spread_total_gm_corpc AS FLOAT) AS revenue_spread_total_gm_corpc,
        CAST(revenue_spread_total_gm_contractc AS FLOAT) AS revenue_spread_total_gm_contractc,
        sf_id_18c,
        business_segmentc,
        fiscal_values_updatedc,
        is_gidc,
        mos_scope_of_servicesc,
        CAST(probable_revenue_contractc AS FLOAT) AS probable_revenue_contractc,
        CAST(probable_revenue_corpc AS FLOAT) AS probable_revenue_corpc,
        mos_pcg_namec,
        mos_pcg_excludec,
        CAST(execution_date AS DATE) AS etl_load_date,
        CAST({{ run_date }} AS TIMESTAMP) AS model_created_date,
        CAST({{ run_date }} AS TIMESTAMP) AS model_updated_date,
        {{ generate_load_id(model) }} AS model_load_id
    FROM mosc_cdc
)

SELECT * FROM mosc_data
