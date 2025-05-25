{%- set execution_date_arg = var("execution_date", "") %}
{%- set run_date = "CURRENT_TIMESTAMP()" -%}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        file_format='iceberg',
        iceberg_expire_snapshots='False', 
        custom_location=target.location ~ 'transformed_budget_june_25/',
        table_properties={'write.target-file-size-bytes': '268435456'},
        on_schema_change='append_new_columns',
        full_refresh=true,
        tags=["circuit_breaker"]
        ) 
}}

select
  'Jun' as month,
  '2025' as year,
  CAST('2025-06-01 00:00:00' as TIMESTAMP) as budget_timestamp,
  CASE
    WHEN UPPER(budget_jun_25__bud__input_currency) = 'NAN' THEN NULL
    ELSE budget_jun_25__bud__input_currency
  END as budget_data_amount,
  CASE
    WHEN UPPER(budget_jun_25__bud) = 'NAN' THEN NULL
    ELSE budget_jun_25__bud
  END as budget_data_amount_aud,
  CASE
    WHEN UPPER(budget_jun_25__actuals) = 'NAN' THEN NULL
    ELSE budget_jun_25__actuals
  END as budget_actual_amount,
  CASE
    WHEN UPPER(budget_jun_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_jun_25__q1_fcst
  END as budget_forecast,
    CASE
    WHEN budget_budget_line_item = 'NAN' THEN NULL
    ELSE budget_budget_line_item
  END as budget_budget_line_item,
  CASE
    WHEN budget_level_1 = 'NAN' THEN NULL
    ELSE budget_level_1
  END as budget_level_1,
  CASE
    WHEN budget_level_2 = 'NAN' THEN NULL
    ELSE budget_level_2
  END as budget_level_2,
  CASE
    WHEN budget_hyperion_task_current_name = 'NAN' THEN NULL
    ELSE budget_hyperion_task_current_name
  END as budget_hyperion_task_current_name,
  CASE
    WHEN budget_hyperion_task_updated_name_if_required = 'NAN' THEN NULL
    ELSE budget_hyperion_task_updated_name_if_required
  END as budget_hyperion_task_updated_name_if_required,
  CASE
    WHEN budget_hyperion_task_number = 'NAN' THEN NULL
    ELSE budget_hyperion_task_number
  END as budget_hyperion_task_number ,
  CASE
    WHEN budget_level_1_access = 'NAN' THEN NULL
    ELSE budget_level_1_access
  END as budget_level_1_access,
  CASE
    WHEN budget_project_manager = 'NAN' THEN NULL
    ELSE budget_project_manager
  END as budget_project_manager,
  CASE
    WHEN budget_gbs_project_number = 'NAN' THEN NULL
    ELSE budget_gbs_project_number
  END as budget_gbs_project_number,
  CASE
    WHEN budget_wbs_task = 'NAN' THEN NULL
    ELSE budget_wbs_task
  END as budget_wbs_task,
  CASE
    WHEN budget_vendor = 'NAN' THEN NULL
    ELSE budget_vendor
  END as budget_vendor,
  CASE
    WHEN budget_gsr_id_no = 'NAN' THEN NULL
    ELSE budget_gsr_id_no
  END as budget_gsr_id_no,
  CASE
    WHEN budget_po = 'NAN' THEN NULL
    ELSE budget_po
  END as budget_po,
  CASE
    WHEN budget_function_point_of_contact = 'NAN' THEN NULL
    ELSE budget_function_point_of_contact
  END as budget_function_point_of_contact,
  CASE
    WHEN budget_hyperion_expenditure_type = 'NAN' THEN NULL
    ELSE budget_hyperion_expenditure_type
  END as budget_hyperion_expenditure_type,
  CASE
    WHEN budget_epm_account_wip = 'NAN' THEN NULL
    ELSE budget_epm_account_wip
  END as budget_epm_account_wip,
  CASE
    WHEN budget_cost_type = 'NAN' THEN NULL
    ELSE budget_cost_type
  END as budget_cost_type,
  CASE
    WHEN budget_cost_variability_driver = 'NAN' THEN NULL
    ELSE budget_cost_variability_driver
  END as budget_cost_variability_driver,
  CASE
    WHEN budget_vulnerability_h_m_l = 'NAN' THEN NULL
    ELSE budget_vulnerability_h_m_l
  END as budget_vulnerability_h_m_l,
  CASE
    WHEN budget_budget_item_description = 'NAN' THEN NULL
    ELSE budget_budget_item_description
  END as budget_budget_item_description,
  CASE
    WHEN budget_budget_category = 'NAN' THEN NULL
    ELSE budget_budget_category
  END as budget_budget_category,
  CASE
    WHEN budget_justification = 'NAN' THEN NULL
    ELSE budget_justification
  END as budget_justification,
  CASE
    WHEN budget_committed_yes_no = 'NAN' THEN NULL
    ELSE budget_committed_yes_no
  END as budget_committed_yes_no,
  CASE
    WHEN budget_service_period_start = 'NAN' THEN NULL
    ELSE budget_service_period_start
  END as budget_service_period_start,
  CASE
    WHEN budget_service_period_end = 'NAN' THEN NULL
    ELSE budget_service_period_end
  END as budget_service_period_end,
  CASE
    WHEN budget_input_currency = 'NAN' THEN NULL
    ELSE budget_input_currency
  END as budget_input_currency,
  CASE
    WHEN budget_contract_or_forecast_value_in_contract_currency = 'NAN' THEN NULL
    ELSE
      budget_contract_or_forecast_value_in_contract_currency

  END as budget_contract_or_forecast_value_in_contract_currency,
  CASE
    WHEN budget_service_period_start_1 = 'NAN' THEN NULL
    ELSE budget_service_period_start_1
  END as budget_service_period_start_1,
  CASE
    WHEN UPPER(budget_service_period_end_1) = 'NAN' THEN NULL
    ELSE budget_service_period_end_1
  END AS budget_service_period_end_1,
  CASE
    WHEN UPPER(budget_input_currency_1) = 'NAN' THEN NULL
    ELSE budget_input_currency_1
  END AS budget_input_currency_1,
  CASE
    WHEN UPPER(
      budget_contract_or_forecast_value_in_contract_currency_1
    ) = 'NAN' THEN NULL
    ELSE budget_contract_or_forecast_value_in_contract_currency_1
  END AS budget_contract_or_forecast_value_in_contract_currency_1,
  CASE
    WHEN UPPER(budget_contract_or_forecast_value_in_aud) = 'NAN' THEN NULL
    ELSE budget_contract_or_forecast_value_in_aud
  END AS budget_contract_or_forecast_value_in_aud,
  CASE
    WHEN UPPER(
      budget_licence_type_e_g_user_platform_device_count_etc
    ) = 'NAN' THEN NULL
    ELSE budget_licence_type_e_g_user_platform_device_count_etc
  END AS budget_licence_type_e_g_user_platform_device_count_etc,
  CASE
    WHEN UPPER(budget_licence_volume) = 'NAN' THEN NULL
    ELSE budget_licence_volume
  END AS budget_licence_volume,
  CASE
    WHEN UPPER(budget_cost_per_licence_in_contract_currency) = 'NAN' THEN NULL
    ELSE budget_cost_per_licence_in_contract_currency
  END AS budget_cost_per_licence_in_contract_currency,
  CASE
    WHEN UPPER(budget_contract_months) = 'NAN' THEN NULL
    ELSE budget_contract_months
  END AS budget_contract_months,
  CASE
    WHEN UPPER(budget_invoice_frequency) = 'NAN' THEN NULL
    ELSE budget_invoice_frequency
  END AS budget_invoice_frequency,
  CASE
    WHEN UPPER(budget_x) = 'NAN' THEN NULL
    ELSE budget_x
  END AS budget_x,
  CASE
    WHEN UPPER(budget_cpi_assumption_if_required) = 'NAN' THEN NULL
    ELSE budget_cpi_assumption_if_required
  END AS budget_cpi_assumption_if_required,
  CASE
    WHEN UPPER(budget_org_growth_assumption_if_required) = 'NAN' THEN NULL
    ELSE budget_org_growth_assumption_if_required
  END AS budget_org_growth_assumption_if_required,
  CASE
    WHEN UPPER(budget_x_1) = 'NAN' THEN NULL
    ELSE budget_x_1
  END AS budget_x_1,
  CASE
    WHEN UPPER(budget_fy25_full_year_budget_input_currency) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_budget_input_currency
  END AS budget_fy25_full_year_budget_input_currency,
  CASE
    WHEN UPPER(budget_jul_24__bud) = 'NAN' THEN NULL
    ELSE budget_jul_24__bud
  END AS budget_jul_24__bud,
  CASE
    WHEN UPPER(budget_aug_24__bud) = 'NAN' THEN NULL
    ELSE budget_aug_24__bud
  END AS budget_aug_24__bud,
  CASE
    WHEN UPPER(budget_sep_24__bud) = 'NAN' THEN NULL
    ELSE budget_sep_24__bud
  END AS budget_sep_24__bud,
  CASE
    WHEN UPPER(budget_oct_24__bud) = 'NAN' THEN NULL
    ELSE budget_oct_24__bud
  END AS budget_oct_24__bud,
  CASE
    WHEN UPPER(budget_nov_24__bud) = 'NAN' THEN NULL
    ELSE budget_nov_24__bud
  END AS budget_nov_24__bud,
  CASE
    WHEN UPPER(budget_dec_24__bud) = 'NAN' THEN NULL
    ELSE budget_dec_24__bud
  END AS budget_dec_24__bud,
  CASE
    WHEN UPPER(budget_jan_25__bud) = 'NAN' THEN NULL
    ELSE budget_jan_25__bud
  END AS budget_jan_25__bud,
  CASE
    WHEN UPPER(budget_feb_25__bud) = 'NAN' THEN NULL
    ELSE budget_feb_25__bud
  END AS budget_feb_25__bud,
  CASE
    WHEN UPPER(budget_mar_25__bud) = 'NAN' THEN NULL
    ELSE budget_mar_25__bud
  END AS budget_mar_25__bud,
  CASE
    WHEN UPPER(budget_apr_25__bud) = 'NAN' THEN NULL
    ELSE budget_apr_25__bud
  END AS budget_apr_25__bud,
  CASE
    WHEN UPPER(budget_may_25__bud) = 'NAN' THEN NULL
    ELSE budget_may_25__bud
  END AS budget_may_25__bud,
  CASE
    WHEN UPPER(budget_jun_25__bud) = 'NAN' THEN NULL
    ELSE budget_jun_25__bud
  END AS budget_jun_25__bud,
  CASE
    WHEN UPPER(budget_fy25_full_year_budget_aud_currency) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_budget_aud_currency
  END AS budget_fy25_full_year_budget_aud_currency,
  CASE
    WHEN UPPER(budget_x_2) = 'NAN' THEN NULL
    ELSE budget_x_2
  END AS budget_x_2,
  CASE
    WHEN UPPER(budget_x_3) = 'NAN' THEN NULL
    ELSE budget_x_3
  END AS budget_x_3,
  CASE
    WHEN UPPER(budget_jul_25__forecast) = 'NAN' THEN NULL
    ELSE budget_jul_25__forecast
  END AS budget_jul_25__forecast,
  CASE
    WHEN UPPER(budget_aug_25__forecast) = 'NAN' THEN NULL
    ELSE budget_aug_25__forecast
  END AS budget_aug_25__forecast,
  CASE
    WHEN UPPER(budget_sep_25__forecast) = 'NAN' THEN NULL
    ELSE budget_sep_25__forecast
  END AS budget_sep_25__forecast,
  CASE
    WHEN UPPER(budget_oct_25_forecast) = 'NAN' THEN NULL
    ELSE budget_oct_25_forecast
  END AS budget_oct_25_forecast,
  CASE
    WHEN UPPER(budget_nov_25__forecast) = 'NAN' THEN NULL
    ELSE budget_nov_25__forecast
  END AS budget_nov_25__forecast,
  CASE
    WHEN UPPER(budget_dec_25_forecast) = 'NAN' THEN NULL
    ELSE budget_dec_25_forecast
  END AS budget_dec_25_forecast,
  CASE
    WHEN UPPER(budget_jan_26_forecast) = 'NAN' THEN NULL
    ELSE budget_jan_26_forecast
  END AS budget_jan_26_forecast,
  CASE
    WHEN UPPER(budget_feb_26_forecast) = 'NAN' THEN NULL
    ELSE budget_feb_26_forecast
  END AS budget_feb_26_forecast,
  CASE
    WHEN UPPER(budget_mar_26_forecast) = 'NAN' THEN NULL
    ELSE budget_mar_26_forecast
  END AS budget_mar_26_forecast,
  CASE
    WHEN UPPER(budget_apr_26_forecast) = 'NAN' THEN NULL
    ELSE budget_apr_26_forecast
  END AS budget_apr_26_forecast,
  CASE
    WHEN UPPER(budget_may_26_forecast) = 'NAN' THEN NULL
    ELSE budget_may_26_forecast
  END AS budget_may_26_forecast,
  CASE
    WHEN UPPER(budget_jun_26_forecast) = 'NAN' THEN NULL
    ELSE budget_jun_26_forecast
  END AS budget_jun_26_forecast,
  CASE
    WHEN UPPER(budget_fy26_full_year_forecast_input_currency) = 'NAN' THEN NULL
    ELSE budget_fy26_full_year_forecast_input_currency
  END AS budget_fy26_full_year_forecast_input_currency,
  CASE
    WHEN UPPER(budget_jul_25__forecast_1) = 'NAN' THEN NULL
    ELSE budget_jul_25__forecast_1
  END AS budget_jul_25__forecast_1,
  CASE
    WHEN UPPER(budget_aug_25__forecast_1) = 'NAN' THEN NULL
    ELSE budget_aug_25__forecast_1
  END AS budget_aug_25__forecast_1,
  CASE
    WHEN UPPER(budget_sep_25__forecast_1) = 'NAN' THEN NULL
    ELSE budget_sep_25__forecast_1
  END AS budget_sep_25__forecast_1,
  CASE
    WHEN UPPER(budget_oct_25_forecast_1) = 'NAN' THEN NULL
    ELSE budget_oct_25_forecast_1
  END AS budget_oct_25_forecast_1,
  CASE
    WHEN UPPER(budget_nov_25__forecast_1) = 'NAN' THEN NULL
    ELSE budget_nov_25__forecast_1
  END AS budget_nov_25__forecast_1,
  CASE
    WHEN UPPER(budget_dec_25_forecast_1) = 'NAN' THEN NULL
    ELSE budget_dec_25_forecast_1
  END AS budget_dec_25_forecast_1,
  CASE
    WHEN UPPER(budget_jan_26_forecast_1) = 'NAN' THEN NULL
    ELSE budget_jan_26_forecast_1
  END AS budget_jan_26_forecast_1,
  CASE
    WHEN UPPER(budget_feb_26_forecast_1) = 'NAN' THEN NULL
    ELSE budget_feb_26_forecast_1
  END AS budget_feb_26_forecast_1,
  CASE
    WHEN UPPER(budget_mar_26_forecast_1) = 'NAN' THEN NULL
    ELSE budget_mar_26_forecast_1
  END AS budget_mar_26_forecast_1,
  CASE
    WHEN UPPER(budget_apr_26_forecast_1) = 'NAN' THEN NULL
    ELSE budget_apr_26_forecast_1
  END AS budget_apr_26_forecast_1,
  CASE
    WHEN UPPER(budget_may_26_forecast_1) = 'NAN' THEN NULL
    ELSE budget_may_26_forecast_1
  END AS budget_may_26_forecast_1,
  CASE
    WHEN UPPER(budget_jun_26_forecast_1) = 'NAN' THEN NULL
    ELSE budget_jun_26_forecast_1
  END AS budget_jun_26_forecast_1,
  CASE
    WHEN UPPER(budget_fy26_full_year_forecast_aud) = 'NAN' THEN NULL
    ELSE budget_fy26_full_year_forecast_aud
  END AS budget_fy26_full_year_forecast_aud,
  CASE
    WHEN UPPER(budget_jul_24__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_jul_24__q1_fcst
  END AS budget_jul_24__q1_fcst,
  CASE
    WHEN UPPER(budget_aug_24__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_aug_24__q1_fcst
  END AS budget_aug_24__q1_fcst,
  CASE
    WHEN UPPER(budget_sep_24__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_sep_24__q1_fcst
  END AS budget_sep_24__q1_fcst,
  CASE
    WHEN UPPER(budget_oct_24__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_oct_24__q1_fcst
  END AS budget_oct_24__q1_fcst,
  CASE
    WHEN UPPER(budget_nov_24__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_nov_24__q1_fcst
  END AS budget_nov_24__q1_fcst,
  CASE
    WHEN UPPER(budget_dec_24__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_dec_24__q1_fcst
  END AS budget_dec_24__q1_fcst,
  CASE
    WHEN UPPER(budget_jan_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_jan_25__q1_fcst
  END AS budget_jan_25__q1_fcst,
  CASE
    WHEN UPPER(budget_feb_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_feb_25__q1_fcst
  END AS budget_feb_25__q1_fcst,
  CASE
    WHEN UPPER(budget_mar_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_mar_25__q1_fcst
  END AS budget_mar_25__q1_fcst,
  CASE
    WHEN UPPER(budget_apr_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_apr_25__q1_fcst
  END AS budget_apr_25__q1_fcst,
  CASE
    WHEN UPPER(budget_may_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_may_25__q1_fcst
  END AS budget_may_25__q1_fcst,
  CASE
    WHEN UPPER(budget_jun_25__q1_fcst) = 'NAN' THEN NULL
    ELSE budget_jun_25__q1_fcst
  END AS budget_jun_25__q1_fcst,
  CASE
    WHEN UPPER(budget_fy25_full_year_q1_fcst_aud_currency) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_q1_fcst_aud_currency
  END AS budget_fy25_full_year_q1_fcst_aud_currency,
  CASE
    WHEN UPPER(budget_q1_forecast_less_budget) = 'NAN' THEN NULL
    ELSE budget_q1_forecast_less_budget
  END AS budget_q1_forecast_less_budget,
  CASE
    WHEN UPPER(budget_x_4) = 'NAN' THEN NULL
    ELSE budget_x_4
  END AS budget_x_4,
  CASE
    WHEN UPPER(budget_jul_24__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_jul_24__q2_fcst
  END AS budget_jul_24__q2_fcst,
  CASE
    WHEN UPPER(budget_aug_24__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_aug_24__q2_fcst
  END AS budget_aug_24__q2_fcst,
  CASE
    WHEN UPPER(budget_sep_24__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_sep_24__q2_fcst
  END AS budget_sep_24__q2_fcst,
  CASE
    WHEN UPPER(budget_oct_24__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_oct_24__q2_fcst
  END AS budget_oct_24__q2_fcst,
  CASE
    WHEN UPPER(budget_nov_24__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_nov_24__q2_fcst
  END AS budget_nov_24__q2_fcst,
  CASE
    WHEN UPPER(budget_dec_24__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_dec_24__q2_fcst
  END AS budget_dec_24__q2_fcst,
  CASE
    WHEN UPPER(budget_jan_25__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_jan_25__q2_fcst
  END AS budget_jan_25__q2_fcst,
  CASE
    WHEN UPPER(budget_feb_25__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_feb_25__q2_fcst
  END AS budget_feb_25__q2_fcst,
  CASE
    WHEN UPPER(budget_mar_25__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_mar_25__q2_fcst
  END AS budget_mar_25__q2_fcst,
  CASE
    WHEN UPPER(budget_apr_25__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_apr_25__q2_fcst
  END AS budget_apr_25__q2_fcst,
  CASE
    WHEN UPPER(budget_may_25__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_may_25__q2_fcst
  END AS budget_may_25__q2_fcst,
  CASE
    WHEN UPPER(budget_jun_25__q2_fcst) = 'NAN' THEN NULL
    ELSE budget_jun_25__q2_fcst
  END AS budget_jun_25__q2_fcst,
  CASE
    WHEN UPPER(budget_fy25_full_year_q2_fcst_aud_currency) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_q2_fcst_aud_currency
  END AS budget_fy25_full_year_q2_fcst_aud_currency,
  CASE
    WHEN UPPER(budget_jul_24__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_jul_24__q3_fcst
  END AS budget_jul_24__q3_fcst,
  CASE
    WHEN UPPER(budget_aug_24__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_aug_24__q3_fcst
  END AS budget_aug_24__q3_fcst,
  CASE
    WHEN UPPER(budget_sep_24__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_sep_24__q3_fcst
  END AS budget_sep_24__q3_fcst,
  CASE
    WHEN UPPER(budget_oct_24__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_oct_24__q3_fcst
  END AS budget_oct_24__q3_fcst,
  CASE
    WHEN UPPER(budget_nov_24__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_nov_24__q3_fcst
  END AS budget_nov_24__q3_fcst,
  CASE
    WHEN UPPER(budget_dec_24__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_dec_24__q3_fcst
  END AS budget_dec_24__q3_fcst,
  CASE
    WHEN UPPER(budget_jan_25__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_jan_25__q3_fcst
  END AS budget_jan_25__q3_fcst,
  CASE
    WHEN UPPER(budget_feb_25__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_feb_25__q3_fcst
  END AS budget_feb_25__q3_fcst,
  CASE
    WHEN UPPER(budget_mar_25__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_mar_25__q3_fcst
  END AS budget_mar_25__q3_fcst,
  CASE
    WHEN UPPER(budget_apr_25__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_apr_25__q3_fcst
  END AS budget_apr_25__q3_fcst,
  CASE
    WHEN UPPER(budget_may_25__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_may_25__q3_fcst
  END AS budget_may_25__q3_fcst,
  CASE
    WHEN UPPER(budget_jun_25__q3_fcst) = 'NAN' THEN NULL
    ELSE budget_jun_25__q3_fcst
  END AS budget_jun_25__q3_fcst,
  CASE
    WHEN UPPER(budget_fy25_full_year_q3_fcst_aud_currency) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_q3_fcst_aud_currency
  END AS budget_fy25_full_year_q3_fcst_aud_currency,
  CASE
    WHEN UPPER(budget_x_5) = 'NAN' THEN NULL
    ELSE budget_x_5
  END AS budget_x_5,
  CASE
    WHEN UPPER(budget_fy25_full_year_actuals_aud) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_actuals_aud
  END AS budget_fy25_full_year_actuals_aud,
  CASE
    WHEN UPPER(budget_x_6) = 'NAN' THEN NULL
    ELSE budget_x_6
  END AS budget_x_6,
  CASE
    WHEN UPPER(budget_x_7) = 'NAN' THEN NULL
    ELSE budget_x_7
  END AS budget_x_7,
  CASE
    WHEN UPPER(budget_jul_24_var) = 'NAN' THEN NULL
    ELSE budget_jul_24_var
  END AS budget_jul_24_var,
  CASE
    WHEN UPPER(budget_aug_24__var) = 'NAN' THEN NULL
    ELSE budget_aug_24__var
  END AS budget_aug_24__var,
  CASE
    WHEN UPPER(budget_sep_24__var) = 'NAN' THEN NULL
    ELSE budget_sep_24__var
  END AS budget_sep_24__var,
  CASE
    WHEN UPPER(budget_oct_24_var) = 'NAN' THEN NULL
    ELSE budget_oct_24_var
  END AS budget_oct_24_var,
  CASE
    WHEN UPPER(budget_nov_24__var) = 'NAN' THEN NULL
    ELSE budget_nov_24__var
  END AS budget_nov_24__var,
  CASE
    WHEN UPPER(budget_dec_24_var) = 'NAN' THEN NULL
    ELSE budget_dec_24_var
  END AS budget_dec_24_var,
  CASE
    WHEN UPPER(budget_jan_25_var) = 'NAN' THEN NULL
    ELSE budget_jan_25_var
  END AS budget_jan_25_var,
  CASE
    WHEN UPPER(budget_feb_25_var) = 'NAN' THEN NULL
    ELSE budget_feb_25_var
  END AS budget_feb_25_var,
  CASE
    WHEN UPPER(budget_mar_25_var) = 'NAN' THEN NULL
    ELSE budget_mar_25_var
  END AS budget_mar_25_var,
  CASE
    WHEN UPPER(budget_apr_25_var) = 'NAN' THEN NULL
    ELSE budget_apr_25_var
  END AS budget_apr_25_var,
  CASE
    WHEN UPPER(budget_may_25_var) = 'NAN' THEN NULL
    ELSE budget_may_25_var
  END AS budget_may_25_var,
  CASE
    WHEN UPPER(budget_jun_25_var) = 'NAN' THEN NULL
    ELSE budget_jun_25_var
  END AS budget_jun_25_var,
  CASE
    WHEN UPPER(budget_fy25_full_year_var_aud) = 'NAN' THEN NULL
    ELSE budget_fy25_full_year_var_aud
  END AS budget_fy25_full_year_var_aud,
  CASE
    WHEN UPPER(budget_x_8) = 'NAN' THEN NULL
    ELSE budget_x_8
  END AS budget_x_8,
  CASE
    WHEN UPPER(budget_fx) = 'NAN' THEN NULL
    ELSE budget_fx
  END AS budget_fx,
  CASE
    WHEN UPPER(budget_amortization) = 'NAN' THEN NULL
    ELSE budget_amortization
  END AS budget_amortization,
  CASE
    WHEN UPPER(budget_usage_1) = 'NAN' THEN NULL
    ELSE budget_usage_1
  END AS budget_usage_1,
  CASE
    WHEN UPPER(budget_accounting_related) = 'NAN' THEN NULL
    ELSE budget_accounting_related
  END AS budget_accounting_related,
  CASE
    WHEN UPPER(budget_accounting_related_onerous_provision) = 'NAN' THEN NULL
    ELSE budget_accounting_related_onerous_provision
  END AS budget_accounting_related_onerous_provision,
  CASE
    WHEN UPPER(budget_budget) = 'NAN' THEN NULL
    ELSE budget_budget
  END AS budget_budget,
  CASE
    WHEN UPPER(budget_budget_type) = 'NAN' THEN NULL
    ELSE budget_budget_type
  END AS budget_budget_type,
  CASE
    WHEN UPPER(budget_current_risk_ranking) = 'NAN' THEN NULL
    ELSE budget_current_risk_ranking
  END AS budget_current_risk_ranking,
  CASE
    WHEN UPPER(budget_opportunities) = 'NAN' THEN NULL
    ELSE budget_opportunities
  END AS budget_opportunities,
  CASE
    WHEN UPPER(budget_risk_rating_remarks) = 'NAN' THEN NULL
    ELSE budget_risk_rating_remarks
  END AS budget_risk_rating_remarks,
  CASE
    WHEN UPPER(budget_x_9) = 'NAN' THEN NULL
    ELSE budget_x_9
  END AS budget_x_9,
  CASE
    WHEN UPPER(budget_commentaries) = 'NAN' THEN NULL
    ELSE budget_commentaries
  END AS budget_commentaries,
  CASE
    WHEN UPPER(budget_contract_renewal_savings) = 'NAN' THEN NULL
    ELSE budget_contract_renewal_savings
  END AS budget_contract_renewal_savings,
  CASE
    WHEN UPPER(budget_onerous_provision) = 'NAN' THEN NULL
    ELSE budget_onerous_provision
  END AS budget_onerous_provision,
  CASE
    WHEN UPPER(budget_usage_2) = 'NAN' THEN NULL
    ELSE budget_usage_2
  END AS budget_usage_2,
  CASE
    WHEN UPPER(budget_accounting) = 'NAN' THEN NULL
    ELSE budget_accounting
  END AS budget_accounting,
  CASE
    WHEN UPPER(
      budget_capex_related_issue_change_in_capex_amount_timing
    ) = 'NAN' THEN NULL
    ELSE budget_capex_related_issue_change_in_capex_amount_timing
  END AS budget_capex_related_issue_change_in_capex_amount_timing,
  CASE
    WHEN UPPER(budget_fx_variance) = 'NAN' THEN NULL
    ELSE budget_fx_variance
  END AS budget_fx_variance,
  CASE
    WHEN UPPER(budget_unutilised_budget_release) = 'NAN' THEN NULL
    ELSE budget_unutilised_budget_release
  END AS budget_unutilised_budget_release,
  CASE
    WHEN UPPER(budget_unnamed_191) = 'NAN' THEN NULL
    ELSE budget_unnamed_191
  END AS budget_unnamed_191,
  CASE
    WHEN UPPER(budget_contract_renewal) = 'NAN' THEN NULL
    ELSE budget_contract_renewal
  END AS budget_contract_renewal,
  CASE
    WHEN UPPER(budget_onerous_provision_1) = 'NAN' THEN NULL
    ELSE budget_onerous_provision_1
  END AS budget_onerous_provision_1,
  CASE
    WHEN UPPER(budget_variance) = 'NAN' THEN NULL
    ELSE budget_variance
  END AS budget_variance,
  CASE
    WHEN UPPER(budget_x_10) = 'NAN' THEN NULL
    ELSE budget_x_10
  END AS budget_x_10,
  CASE
    WHEN UPPER(budget_unnamed_196) = 'NAN' THEN NULL
    ELSE budget_unnamed_196
  END AS budget_unnamed_196,
  CASE
    WHEN UPPER(budget_finance_response) = 'NAN' THEN NULL
    ELSE budget_finance_response
  END as budget_finance_response

    ,CASE
        WHEN execution_date IS NULL THEN NULL
        ELSE CAST(execution_date AS DATE)
    END AS execution_date,
    {{run_date}} AS model_created_date,
    {{run_date}} AS model_updated_date,
    {{ generate_load_id(model) }} as model_load_id
  
 from {{ source('curated_hexagon', 'curated_finance_budget') }}