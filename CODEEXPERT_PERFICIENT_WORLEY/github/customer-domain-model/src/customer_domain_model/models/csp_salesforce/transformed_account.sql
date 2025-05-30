{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP" -%}


{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_account/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["csp_salesforce"]
    ) 
}}


with sfca_cdc as (
    select *  from {{ source('curated_salesforce', 'curated_account') }} sfca
    where sfca.is_current = 1
    {%- if execution_date_arg != "" %}
      and execution_date >= '{{ execution_date_arg }}'
    {%- else %}
        {%- if is_incremental() %}
            and cast(sfca.execution_date as DATE) > (select max(etl_load_date)  from {{ this }})
        {%- endif %}
    {%- endif %}
),

sfca_data AS (
    SELECT 
        id,
        isdeleted,
        masterrecordid,
        name,
        type,
        recordtypeid,
        parentid,
        CAST(billinglatitude AS FLOAT) AS billinglatitude,
        CAST(billinglongitude AS FLOAT) AS billinglongitude,
        billingstreet,
        billingcity,
        billingstate,
        billingpostalcode,
        billingcountry,
        billingstatecode,
        billingcountrycode,
        billinggeocodeaccuracy,
        billingaddress,
        shippingstreet,
        shippingcity,
        shippingstate,
        shippingpostalcode,
        shippingcountry,
        shippingstatecode,
        shippingcountrycode,
        CAST(shippinglatitude AS FLOAT) AS shippinglatitude,
        CAST(shippinglongitude AS FLOAT) AS shippinglongitude,
        shippinggeocodeaccuracy,
        shippingaddress,
        phone,
        fax,
        accountnumber,
        website,
        photourl,
        sic,
        industry,
        annualrevenue,
        numberofemployees,
        tickersymbol,
        description,
        rating,
        site,
        currencyisocode,
        ownerid,
        createddate,
        createdbyid,
        lastmodifieddate,
        lastmodifiedbyid,
        systemmodstamp,
        lastactivitydate,
        lastvieweddate,
        lastreferenceddate,
        jigsaw,
        jigsawcompanyid,
        accountsource,
        sicdesc,
        ecrmigrationexternalidc,
        account_aliasc,
        account_commentsc,
        account_sf_id_18c,
        account_group_customer_typec,
        account_group_lobc,
        account_group_tierc,
        account_groupc,
        account_lead_user_rolec,
        account_overviewc,
        account_pre_approvedc,
        account_strategyc,
        acct_group_id_namec,
        business_outlook_plan_summaryc,
        business_typec,
        capex_spending_approach_summaryc,
        ch2m_csp_prod_dedup_idc,
        ch2m_dynamics_idc,
        ch2m_oracle_account_idc,
        ch2m_oracle_account_numberc,
        ch2m_statusc,
        account_group_sharepoint_urlc,
        clients_with_active_pursuitsc,
        commercial_strategy_summaryc,
        confidentialc,
        core_business_goals_summaryc,
        core_valuesc,
        datamigridc,
        dnboptimizerdnb_d_u_n_s_numberc,
        dnboptimizerdnbcompanyrecordc,
        account_group_in_perimeterc,
        ld_use_dnb_optimizec,
        ecr_account_numberc,
        finance_review_notesc,
        finance_review_resultc,
        finance_reviewed_byc,
        latest_finance_reviewed_datec,
        request_finance_reviewc,
        tax_exempt_statusc,
        CAST(verified_sitesc AS FLOAT) AS verified_sitesc,
        finance_review_red_flagc,
        gbs_acctidc,
        dar_affiliated_entityc,
        account_group_leadc,
        jesa_client_typec,
        current_fy_account_plan_finalizedc,
        regular_account_team_meeting_cadencec,
        datasourcec,
        date_of_last_awardc,
        do_not_create_oracle_accountc,
        employee_contact_accountc,
        executive_leadership_summaryc,
        industryc,
        joint_venturec,
        legacy_8a_small_businesc,
        legacy_8a_small_business_datec,
        legacy_cage_codec,
        legacy_hubzone_certification_datec,
        legacy_hubzone_small_businessc,
        legacy_native_american_small_bizc,
        legacy_percent_crc,
        legacy_percent_ffpc,
        legacy_percent_tmc,
        legacy_revenue_from_governmentc,
        legacy_small_disadvantaged_businessc,
        operating_unitc,
        oracleaccountidc,
        oracleparentpartynumberc,
        oracle_account_namec,
        oracle_account_numberc,
        oracle_cust_account_idc,
        oracle_isprimaryc,
        oracle_parentpartyidc,
        previous_account_namesc,
        previously_approved_b_pc,
        primary_lobc,
        privatec,
        record_type_at_creationc,
        registration_informationc,
        request_confidentialc,
        safety_philosophy_summaryc,
        sectorc,
        sharepoint_urlc,
        small_business_typec,
        statusc,
        total_actual_b_pc,
        total_pending_b_pc,
        total_requested_b_pc,
        ultimate_parentc,
        vision_missionc,
        cb_inactive_accountc,
        cb_dcaa_verifiedc,
        cb_unresponsivec,
        cb_verifiedc,
        dt_dcaa_last_verifiedc,
        dt_dcaa_updatedc,
        dt_last_community_update_requestc,
        dt_last_verified_datec,
        fm_cb_dcaa_unresponsivec,
        CAST(fm_cost_volume_rating_averagec AS FLOAT) AS fm_cost_volume_rating_averagec,
        CAST(fm_partner_rating_averagec AS FLOAT) AS fm_partner_rating_averagec,
        fm_star_ratingc,
        CAST(frm_myaccountc AS FLOAT) AS frm_myaccountc,
        CAST(previously_approved_b_p_corpc AS FLOAT) AS previously_approved_b_p_corpc,
        CAST(total_actual_b_p_corpc AS FLOAT) AS total_actual_b_p_corpc,
        CAST(closed_won_opportunitiesc AS FLOAT) AS closed_won_opportunitiesc,
        CAST(fy_booking_gmc AS FLOAT) AS fy_booking_gmc,
        CAST(open_opportunitiesc AS FLOAT) AS open_opportunitiesc,
        pipeline_factored_gmc  AS pipeline_factored_gmc,
        fm_cost_volume_ratingc,
        CAST(of_opportunitiesc AS FLOAT) AS of_opportunitiesc,
        CAST(of_times_is_competitorc AS FLOAT) AS of_times_is_competitorc,
        CAST(of_times_is_partnerc AS FLOAT) AS of_times_is_partnerc,
        CAST(rus_cost_volume_rating_countc AS FLOAT) AS rus_cost_volume_rating_countc,
        CAST(rus_cost_volume_rating_sumc AS FLOAT) AS rus_cost_volume_rating_sumc,
        CAST(rus_partner_rating_countc AS FLOAT) AS rus_partner_rating_countc,
        CAST(rus_partner_rating_sumc AS FLOAT) AS rus_partner_rating_sumc,
        CAST(dnbconnectd_b_match_confidence_codec AS FLOAT) AS dnbconnectd_b_match_confidence_codec,
        CAST(dnbconnectd_b_name_match_scorec AS FLOAT) AS dnbconnectd_b_name_match_scorec,
        ag_vettedc,
        crmt_csp_prod_dedup_idc,
        crmt_idc,
        crmt_dupe_vettedc,
        jacobs_oracle_account_number_indiac,
        jacobs_oracleaccountid_indiac,
        duns_numberc,
        customer_legal_namec,
        naics_code_newc,
        naics_description_newc,
        account_group_textc,
        crmt_sharepoint_linkc,
        gbs_statusc,
        abc_compliance_ethics_check_last_updatec,
        abc_compliance_ethics_check_resultc,
        CAST(total_pending_dummyc AS FLOAT) AS total_pending_dummyc,
        CAST(total_requested_b_p_corpc AS FLOAT) AS total_requested_b_p_corpc,
        CAST(fy_booking_gm_corpc AS FLOAT) AS fy_booking_gm_corpc,
        CAST(pipeline_factored_gm_corpc AS FLOAT) AS pipeline_factored_gm_corpc,
        sanctions_check_requiredc,
        dnbconnectd_b_connect_company_profilec,
        dnbconnectd_b_match_data_profilec,
        dnbconnectd_b_match_gradec,
        dnbconnectd_b_match_typec,
        dnbconnectdunstradedupc,
        dnbconnectmatcheddunsc,
        event_primary_contactc,
        d_b_optimizer_dunsc,
        dnb_exclude_companyc,
        dnbconnect_dunsnumberc,
        CAST(execution_date AS DATE) AS etl_load_date,
        {{ run_date }} AS model_created_date,
        {{ run_date }} AS model_updated_date,
        {{ generate_load_id(model) }} AS model_load_id
    FROM sfca_cdc
)

SELECT * FROM sfca_data
