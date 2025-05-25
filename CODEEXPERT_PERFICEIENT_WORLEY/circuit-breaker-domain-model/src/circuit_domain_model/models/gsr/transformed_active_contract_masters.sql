{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_active_contract_masters/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}


SELECT
    CASE
        WHEN UPPER(active_contract_masters_id) = 'NAN' THEN NULL
        ELSE active_contract_masters_id
    END AS active_contract_masters_id,
    CASE
        WHEN UPPER(active_contract_masters_contract_reference) = 'NAN' THEN NULL
        ELSE active_contract_masters_contract_reference
    END AS active_contract_masters_contract_reference,
    CASE
        WHEN UPPER(active_contract_masters_vendor_name) = 'NAN' THEN NULL
        ELSE active_contract_masters_vendor_name
    END AS active_contract_masters_vendor_name,
    CASE
        WHEN UPPER(active_contract_masters_contract_name) = 'NAN' THEN NULL
        ELSE active_contract_masters_contract_name
    END AS active_contract_masters_contract_name,
    CASE
        WHEN UPPER(active_contract_masters_contract_type) = 'NAN' THEN NULL
        ELSE active_contract_masters_contract_type
    END AS active_contract_masters_contract_type,
    cast(
        CASE
        WHEN UPPER(active_contract_masters_current_contract_start_date) IN ('','NAT') THEN NULL
        ELSE active_contract_masters_current_contract_start_date
    END as timestamp
    ) as active_contract_masters_current_contract_start_date,
    cast(
        CASE
        WHEN UPPER(active_contract_masters_current_contract_end_date) IN ('','NAT') THEN NULL
        ELSE active_contract_masters_current_contract_end_date
    END as timestamp
    ) as active_contract_masters_current_contract_end_date,
    CASE
        WHEN UPPER(active_contract_masters_procurement_lead) = 'NAN' THEN NULL
        ELSE active_contract_masters_procurement_lead
    END AS active_contract_masters_procurement_lead,
    CASE
        WHEN UPPER(active_contract_masters_gbs_project_number) = 'NAN' THEN NULL
        ELSE active_contract_masters_gbs_project_number
    END AS active_contract_masters_gbs_project_number,
    CASE
        WHEN UPPER(active_contract_masters_wbs_task) = 'NAN' THEN NULL
        ELSE active_contract_masters_wbs_task
    END AS active_contract_masters_wbs_task,
    CASE
        WHEN UPPER(active_contract_masters_completed) = 'NAN' THEN NULL
        ELSE active_contract_masters_completed
    END AS active_contract_masters_completed,
    CASE
        WHEN UPPER(active_contract_masters_finance_signoff) = 'NAN' THEN NULL
        ELSE active_contract_masters_finance_signoff
    END AS active_contract_masters_finance_signoff,
    CASE
        WHEN UPPER(active_contract_masters_budget_fy_25_aud_forecast_renewal_acv) = 'NAN' THEN NULL
        ELSE active_contract_masters_budget_fy_25_aud_forecast_renewal_acv
    END AS active_contract_masters_budget_fy_25_aud_forecast_renewal_acv,
    CASE
        WHEN UPPER(active_contract_masters_budget_fy_25_contract_currency_forecast_renewal_a) = 'NAN' THEN NULL
        ELSE active_contract_masters_budget_fy_25_contract_currency_forecast_renewal_a
    END AS active_contract_masters_budget_fy_25_contract_currency_forecast_renewal_a,
    CASE
        WHEN UPPER(active_contract_masters_contract_base_currency) = 'NAN' THEN NULL
        ELSE active_contract_masters_contract_base_currency
    END AS active_contract_masters_contract_base_currency,
    CASE
        WHEN UPPER(active_contract_masters_current_approved_contract_commitment_aud) = 'NAN' THEN NULL
        ELSE active_contract_masters_current_approved_contract_commitment_aud
    END AS active_contract_masters_current_approved_contract_commitment_aud,
    CASE
        WHEN UPPER(active_contract_masters_current_approved_contract_commitment_base_currency) = 'NAN' THEN NULL
        ELSE active_contract_masters_current_approved_contract_commitment_base_currency
    END AS active_contract_masters_current_approved_contract_commitment_base_currency,
    CASE
        WHEN UPPER(active_contract_masters_current_contract_term_months) = 'NAN' THEN NULL
        ELSE active_contract_masters_current_contract_term_months
    END AS active_contract_masters_current_contract_term_months,
    CASE
        WHEN UPPER(active_contract_masters_date_of_completion) = 'NAN' THEN NULL
        ELSE active_contract_masters_date_of_completion
    END AS active_contract_masters_date_of_completion,
    CASE
        WHEN UPPER(active_contract_masters_digital_funded_yes__no) = 'NAN' THEN NULL
        ELSE active_contract_masters_digital_funded_yes__no
    END AS active_contract_masters_digital_funded_yes__no,
    CASE
        WHEN UPPER(active_contract_masters_digital_lt_budget_holder) = 'NAN' THEN NULL
        ELSE active_contract_masters_digital_lt_budget_holder
    END AS active_contract_masters_digital_lt_budget_holder,
    CASE
        WHEN UPPER(active_contract_masters_final_finance_invoice_amount) = 'NAN' THEN NULL
        ELSE active_contract_masters_final_finance_invoice_amount
    END AS active_contract_masters_final_finance_invoice_amount,
    CASE
        WHEN UPPER(active_contract_masters_finance_comments) = 'NAN' THEN NULL
        ELSE active_contract_masters_finance_comments
    END AS active_contract_masters_finance_comments,
    CASE
        WHEN UPPER(active_contract_masters_finance_invoice_conversion) = 'NAN' THEN NULL
        ELSE active_contract_masters_finance_invoice_conversion
    END AS active_contract_masters_finance_invoice_conversion,
    CASE
        WHEN UPPER(active_contract_masters_first_quote_contract_currency_acv) = 'NAN' THEN NULL
        ELSE active_contract_masters_first_quote_contract_currency_acv
    END AS active_contract_masters_first_quote_contract_currency_acv,
    CASE
        WHEN UPPER(active_contract_masters_flexera_entitlement_data_yes__no) = 'NAN' THEN NULL
        ELSE active_contract_masters_flexera_entitlement_data_yes__no
    END AS active_contract_masters_flexera_entitlement_data_yes__no,
    CASE
        WHEN UPPER(active_contract_masters_fy_25_aud_portion_of_forecast_acv) = 'NAN' THEN NULL
        ELSE active_contract_masters_fy_25_aud_portion_of_forecast_acv
    END AS active_contract_masters_fy_25_aud_portion_of_forecast_acv,
    CASE
        WHEN UPPER(active_contract_masters_fy25_renewal_target_review_month) = 'NAN' THEN NULL
        ELSE active_contract_masters_fy25_renewal_target_review_month
    END AS active_contract_masters_fy25_renewal_target_review_month,
    CASE
        WHEN UPPER(active_contract_masters_last_quote_actual_acv_fy_25_po_value__contract) = 'NAN' THEN NULL
        ELSE active_contract_masters_last_quote_actual_acv_fy_25_po_value__contract
    END AS active_contract_masters_last_quote_actual_acv_fy_25_po_value__contract,
    CASE
        WHEN UPPER(active_contract_masters_link_to_contract_and_po) = 'NAN' THEN NULL
        ELSE active_contract_masters_link_to_contract_and_po
    END AS active_contract_masters_link_to_contract_and_po,
    CASE
        WHEN UPPER(active_contract_masters_new_contract_end_date) = 'NAN' THEN NULL
        ELSE active_contract_masters_new_contract_end_date
    END AS active_contract_masters_new_contract_end_date,
    CASE
        WHEN UPPER(active_contract_masters_new_contract_start_date) = 'NAN' THEN NULL
        ELSE active_contract_masters_new_contract_start_date
    END AS active_contract_masters_new_contract_start_date,
    CASE
        WHEN UPPER(active_contract_masters_new_contract_term_months) = 'NAN' THEN NULL
        ELSE active_contract_masters_new_contract_term_months
    END AS active_contract_masters_new_contract_term_months,
    CASE
        WHEN UPPER(active_contract_masters_new_po) = 'NAN' THEN NULL
        ELSE active_contract_masters_new_po
    END AS active_contract_masters_new_po,
    CASE
        WHEN UPPER(active_contract_masters_notice_period_requirements_incl_auto_renewal) = 'NAN' THEN NULL
        ELSE active_contract_masters_notice_period_requirements_incl_auto_renewal
    END AS active_contract_masters_notice_period_requirements_incl_auto_renewal,
    CASE
        WHEN UPPER(active_contract_masters_opentit_usage__consumption_data_yes__no) = 'NAN' THEN NULL
        ELSE active_contract_masters_opentit_usage__consumption_data_yes__no
    END AS active_contract_masters_opentit_usage__consumption_data_yes__no,
    CASE
        WHEN UPPER(active_contract_masters_opentit_usage__consumption_data_yes__no_old) = 'NAN' THEN NULL
        ELSE active_contract_masters_opentit_usage__consumption_data_yes__no_old
    END AS active_contract_masters_opentit_usage__consumption_data_yes__no_old,
    CASE
        WHEN UPPER(active_contract_masters_other_cost_avoidence) = 'NAN' THEN NULL
        ELSE active_contract_masters_other_cost_avoidence
    END AS active_contract_masters_other_cost_avoidence,
    CASE
        WHEN UPPER(active_contract_masters_owner) = 'NAN' THEN NULL
        ELSE active_contract_masters_owner
    END AS active_contract_masters_owner,
    CASE
        WHEN UPPER(active_contract_masters_owning_business_unit) = 'NAN' THEN NULL
        ELSE active_contract_masters_owning_business_unit
    END AS active_contract_masters_owning_business_unit,
    CASE
        WHEN UPPER(active_contract_masters_procurement_comments) = 'NAN' THEN NULL
        ELSE active_contract_masters_procurement_comments
    END AS active_contract_masters_procurement_comments,
    CASE
        WHEN UPPER(active_contract_masters_completed_old) = 'NAN' THEN NULL
        ELSE active_contract_masters_completed_old
    END AS active_contract_masters_completed_old,
    CASE
        WHEN UPPER(active_contract_masters_created_by) = 'NAN' THEN NULL
        ELSE active_contract_masters_created_by
    END AS active_contract_masters_created_by,
    CASE
        WHEN UPPER(active_contract_masters_created_by_delegate) = 'NAN' THEN NULL
        ELSE active_contract_masters_created_by_delegate
    END AS active_contract_masters_created_by_delegate,
    CASE
        WHEN UPPER(active_contract_masters_created_on) = 'NAN' THEN NULL
        ELSE active_contract_masters_created_on
    END AS active_contract_masters_created_on,
    CASE
        WHEN UPPER(active_contract_masters_modified_by) = 'NAN' THEN NULL
        ELSE active_contract_masters_modified_by
    END AS active_contract_masters_modified_by,
    CASE
        WHEN UPPER(active_contract_masters_modified_by_delegate) = 'NAN' THEN NULL
        ELSE active_contract_masters_modified_by_delegate
    END AS active_contract_masters_modified_by_delegate,
    CASE
        WHEN UPPER(active_contract_masters_modified_on) = 'NAN' THEN NULL
        ELSE active_contract_masters_modified_on
    END AS active_contract_masters_modified_on,
    CASE
        WHEN UPPER(active_contract_masters_product_management_lead) = 'NAN' THEN NULL
        ELSE active_contract_masters_product_management_lead
    END AS active_contract_masters_product_management_lead,
    CASE
        WHEN UPPER(active_contract_masters_record_created_on) = 'NAN' THEN NULL
        ELSE active_contract_masters_record_created_on
    END AS active_contract_masters_record_created_on,
    CASE
        WHEN UPPER(active_contract_masters_renewal_in_fy_25) = 'NAN' THEN NULL
        ELSE active_contract_masters_renewal_in_fy_25
    END AS active_contract_masters_renewal_in_fy_25,
    CASE
        WHEN UPPER(active_contract_masters_snow_ticket_number) = 'NAN' THEN NULL
        ELSE active_contract_masters_snow_ticket_number
    END AS active_contract_masters_snow_ticket_number,
    CASE
        WHEN UPPER(active_contract_masters_status) = 'NAN' THEN NULL
        ELSE active_contract_masters_status
    END AS active_contract_masters_status,
    CASE
        WHEN UPPER(active_contract_masters_status_reason) = 'NAN' THEN NULL
        ELSE active_contract_masters_status_reason
    END AS active_contract_masters_status_reason,
    CASE
        WHEN UPPER(active_contract_masters_type) = 'NAN' THEN NULL
        ELSE active_contract_masters_type
    END AS active_contract_masters_type, 
    CASE
        WHEN execution_date IS NULL THEN NULL
        ELSE CAST(execution_date AS DATE)
    END AS model_execution_date,   
    {{run_date}} as model_created_date,
    {{run_date}} as model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
from {{ source('curated_hexagon','curated_entitlement_active_contract_masters') }}