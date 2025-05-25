{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_datacapture_snapshot/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}

WITH opportunity AS (
    SELECT *
    FROM  {{ source('curated_salesforce', 'curated_opportunity') }} opp
    where opp.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(opp.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),
accounts AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_account') }} acc
    where acc.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(acc.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

bnp AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_b_p_request__c') }} b
    where b.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(.bexecution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

multi_office_split AS (
    SELECT *
    FROM  {{ source('curated_salesforce', 'curated_multi_office_split__c') }} mos
    where mos.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(mos.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

unit AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_unit__c') }} un
    where un.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(un.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

users AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_user') }} ur
    where ur.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(ur.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),
jecob_project AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_jacobs_project__c') }} jp
    where jp.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(jp.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

territory AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_conv_territory') }}
),

table_conv_country AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_conv_country') }}
),

table_conv_tier AS (
    SELECT *
    FROM {{ source('curated_salesforce', 'curated_conv_tier') }}
)

SELECT
    acc.id as id_account,
    opp.country_of_assetc AS asset_country,
    tc.asset_country AS asset_country_full,
    opp.asset_typec AS asset_type,
    opp.bid_officec AS bid_office,
    opp.opportunity_structurec AS category_opportunity,
    opp.project_classification_audc AS classification_project,
    opp.contract_award_typec AS contract_type,
    opp.contract_award_type_2c AS contract_type_award,
    CAST(opp.closedate AS DATE) AS date_award,
    CAST(opp.lastmodifieddate AS DATE) AS date_lastmodified,
    CAST(opp.proposal_due_datec AS DATE) AS date_proposaldue,
    CAST(opp.rfp_datec AS DATE) AS date_rfp,
    opp.forecastcategory AS forecast_category,
    opp.forecast_categoryc AS forecast_includeexclude,
    opp.lead_global_service_linec AS advisian_hub_lead,
    opp.currencyisocode AS id_currency_contract,
    opp.id AS id_opportunity,
    opp.parent_opportunityc AS id_opportunity_parent,
    opp.jacobs_industry_groupc,
    opp.lead_performance_unit_puc,
    opp.worley_capability_sectorc AS legacy_market_sector,
    opp.marketsc,
    opp.name AS name_opportunity,
    us.name as name_owner,
    opp.proposal_managerc AS name_proposal_manager,
    opp.ownerid,
    opp.project_categoriesc,
    opp.project_rolec,
    opp.sec_codes_and_categoriesc,
    opp.scope_of_servicesc AS selling_scope,
    opp.worley_capability_lead_subsectorc AS lead_capability,
    opp.lead_service_linec AS service_line_lead,
    opp.service_typec AS service_type_pap,
    opp.specialtiesc,
    opp.stagename AS stage_actual,
    opp.initial_stagec AS stage_initial,
    opp.project_statusc AS status_activeinactive,
    opp.statusc AS status_description,
    opp.opportunity_tierc AS tier_opportunity,
    tt.tier_opportunity_short,
    opp.top_prospectc as top_prospect_c ,
    REPLACE(u1.name,' FY24', '') AS sales_plan_unit,
    REPLACE(u1.selling_businessc,' FY24', '') AS selling_business,
    REPLACE(u1.sales_regionc,' FY24', '') AS selling_territory,
    --tr.selling_territory_short AS selling_territory_short,
    mos.leadc AS boolean_lead_multiofficesplit,
    CASE 
        WHEN u1.name = 'Colombia - Bogota - Workshare' 
             OR u2.business_unitc = 'India GID' THEN TRUE
        ELSE FALSE 
    END AS bool_gid,
    mos.scope_of_servicesc AS executing_scope,
    mos.resource_typec AS resource_type,
    CASE 
        WHEN mos.leadc = TRUE AND (
            u1.name = 'Colombia - Bogota - Workshare' OR u2.business_unitc = 'India GID'
        ) THEN 'Lead/GID'
        WHEN mos.leadc = TRUE THEN 'Lead'
        WHEN u1.name = 'Colombia - Bogota - Workshare' OR u2.business_unitc = 'India GID' THEN 'GID'
        WHEN mos.scope_of_servicesc = 'PS - GID Home Office Margin Recognition' THEN 'GID Margin Rec.'
    ELSE 'Workshare'
    END AS role_multiofficesplit,
    mos.advisian_service_linec AS service_line,
    mos.spreading_formulac AS spreading_formula,
    CAST(mos.start_datec AS DATE) AS date_start_multiofficesplit,
    CAST(mos.end_datec AS DATE) AS date_end_multiofficesplit,
    mos.id AS id_multiofficesplit,
    u3.legacy_ecr_puc as entity,
    u3.gbs_businessc as executing_business,
    u3.	business_linec as executing_business_line,
    acc.account_group_in_perimeterc AS account_perimeter,
    CASE 
        WHEN acc.account_group_in_perimeterc = TRUE THEN 'In'
        ELSE 'Out'
    END AS account_perimeter_inout,
    acc.account_group_tierc AS account_tier,
    acc.name AS name_account,
    acc.acct_group_id_namec AS name_account_group,
    CASE 
        WHEN mos.leadc = TRUE AND mos.opportunityc = opp.id THEN mos.business_segmentc
        ELSE NULL
    END as executing_business_segment_lead,
    u3.countryc AS executing_country,
    u3.executing_officec as executing_office,
    u3.gbs_performance_unitc AS executing_business_unit,
    u3.gbs_regionc AS executing_region,
    CAST(mos.createddate AS DATE) AS date_created_mos,
    CAST(opp.createddate AS DATE) AS date_created_opp,
    u4.market_sector_l1c as market_sector,
    mos.worley_capability_subsectorc AS capability,
    u4.market_subsector_l2c as market_subsector,
    u4.name as market_segment,
    u4.ecr_l0c as ecr,
    CASE 
        WHEN u4.id = mos.performance_unit_puc THEN u1.name
        ELSE 'undefined'
    END AS profit_centre_group,
    opp.campaignid,
    opp.getc AS goget_get,
    opp.goc AS  goget_go,
    CAST(mos.number_of_daysc as float) AS  duration_days_multiofficesplit,
    CAST(mos.number_of_monthsc as float) AS  duration_months_multiofficesplit,
    SUM(CASE 
            WHEN b.opportunityc = opp.id THEN CAST(b.b_p_budget_corporatec AS double)
            ELSE 0 
        END) AS cost_BandP_actual,
    SUM(CASE 
        WHEN b.opportunityc = opp.id 
             AND opp.recordtypeid IN ('0121U000000KDPvQAO', '0121U000000KR3yQAG')
             AND b.statusc IN ('Active', 'Approved', 'Closed', 'Pending Close')
        THEN CAST(b.b_p_budget_corporatec AS double)
        ELSE 0
    END) AS cost_BandP_budget,
    SUM(CASE 
        WHEN b.opportunityc = opp.id 
             AND opp.recordtypeid IN ('0121U000000KDPvQAO', '0121U000000KR3yQAG')
             AND b.statusc IN ('Active', 'Approved', 'Closed', 'Pending Close')
        THEN CAST(b.work_hoursc AS double)
        ELSE 0
    END) AS hours_BandP_budget,
    CAST(SUM(mos.gross_margin_corpc) as float)  AS gm_mos_unfactored_AllForecastIncludeExclude,
    CAST(SUM(mos.hoursc) as float) AS hours_mos_unfactored_AllForecastIncludeExclude,
    SUM(CASE 
            WHEN mos.opportunityc = opp.id THEN CAST(mos.revenue_corpc as double) 
            ELSE 0 
        END) AS revenue_mos_unfactored_AllForecastIncludeExclude,

    SUM(CASE 
            WHEN jp.opportunityc = opp.id
                 AND jp.id IS NOT NULL 
                 AND jp.recordtypeid = '0124Q0000018L3wQAE' 
            THEN CAST(jp.budgeted_revenue_corpc as float) 
            ELSE 0 
        END) AS revenue_financial_opp_budget,
    SUM(CASE 
            WHEN jp.opportunityc = opp.id 
                 AND jp.id IS NOT NULL 
                 AND jp.recordtypeid = '0124Q0000018L3wQAE' 
            THEN CAST(jp.actual_revenue_corpc as double) 
            ELSE 0 
        END) AS revenue_financial_opp_actual,
    SUM(CASE 
            WHEN jp.opportunityc = opp.id 
                 AND jp.id IS NOT NULL 
                 AND jp.recordtypeid = '0124Q0000018L3wQAE' 
            THEN CAST(jp.budgeted_gross_margin_corpc as double)
            ELSE 0 
        END) AS gm_financial_opp_budget,
    SUM(CASE 
            WHEN jp.opportunityc = opp.id 
                 AND jp.id IS NOT NULL 
                 AND jp.recordtypeid = '0124Q0000018L3wQAE' 
            THEN CAST(jp.actual_gross_margin_corpc as double)
            ELSE 0 
        END ) AS gm_financial_opp_actual,
    CAST({{ run_date }} AS TIMESTAMP) AS model_created_date,
    CAST({{ run_date }} AS TIMESTAMP) AS model_updated_date,
    {{ generate_load_id(model) }} as model_load_id	
FROM opportunity opp
LEFT JOIN accounts acc ON opp.accountid = acc.id
LEFT JOIN multi_office_split mos ON opp.id = mos.opportunityc
LEFT JOIN unit u1 ON u1.id = TRIM(REPLACE(opp.sales_plan_unitc, ' FY24', ''))
LEFT JOIN unit u2 ON u2.id = opp.sales_plan_unitc
LEFT JOIN table_conv_country tc ON opp.country_of_assetc = tc.api_name
LEFT JOIN table_conv_tier tt ON opp.opportunity_tierc = tt.tier_opportunity
LEFT JOIN users us ON opp.ownerid = us.id
LEFT JOIN unit u3 ON u3.id = mos.performance_unit_puc
LEFT JOIN unit u4 ON u4.id = opp.market_segment_l3c
LEFT JOIN bnp b ON opp.id = b.opportunityc
LEFT JOIN jecob_project jp on opp.id = jp.opportunityc
--LEFT JOIN territory tr ON opp.sales_plan_unitc = tr.
GROUP BY 
acc.id,
opp.country_of_assetc,
tc.asset_country,
opp.asset_typec,
opp.bid_officec,
opp.opportunity_structurec,
opp.project_classification_audc,
opp.contract_award_typec,
opp.contract_award_type_2c,
opp.closedate,
opp.lastmodifieddate,
opp.proposal_due_datec,
opp.rfp_datec,
opp.forecastcategory,
opp.forecast_categoryc,
opp.lead_global_service_linec,
opp.currencyisocode,
opp.id,
opp.parent_opportunityc,
opp.jacobs_industry_groupc,
opp.lead_performance_unit_puc,
opp.worley_capability_sectorc,
opp.marketsc,
opp.name,
us.name,
opp.proposal_managerc,
opp.ownerid,
opp.project_categoriesc,
opp.project_rolec,
opp.sec_codes_and_categoriesc,
opp.scope_of_servicesc,
opp.worley_capability_lead_subsectorc,
opp.lead_service_linec,
opp.service_typec,
opp.specialtiesc,
opp.stagename,
opp.initial_stagec,
opp.project_statusc,
opp.statusc,
opp.opportunity_tierc,
tt.tier_opportunity_short,
opp.top_prospectc,
REPLACE(u1.name, ' FY24', ''),
REPLACE(u1.selling_businessc, ' FY24', ''),
REPLACE(u1.sales_regionc, ' FY24', ''),
mos.leadc,
u1.name,
u2.business_unitc,
mos.scope_of_servicesc,
mos.resource_typec,
mos.advisian_service_linec,
mos.spreading_formulac,
mos.start_datec,
mos.end_datec,
mos.id,
u3.legacy_ecr_puc,
u3.gbs_businessc,
u3.business_linec,
acc.account_group_in_perimeterc,
acc.account_group_tierc,
acc.name,
acc.acct_group_id_namec,
u3.countryc,
u3.executing_officec,
u3.gbs_performance_unitc,
u3.gbs_regionc,
mos.createddate,
opp.createddate,
u4.market_sector_l1c,
mos.worley_capability_subsectorc,
u4.market_subsector_l2c,
u4.name,
u4.ecr_l0c,
mos.opportunityc,
opp.campaignid,
opp.getc,
opp.goc,
mos.number_of_daysc,
mos.number_of_monthsc,
mos.business_segmentc,
u4.id,
mos.performance_unit_puc