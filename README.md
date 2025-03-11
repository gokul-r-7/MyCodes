CREATE EXTERNAL TABLE IF NOT EXISTS your_table_name (
    column1_name column1_type,
    column2_name column2_type,
    column3_name column3_type,
    ...
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','  -- Assuming your CSV is comma-separated
LINES TERMINATED BY '\n'  -- Adjust if line endings are different
LOCATION 's3://your-bucket-name/your-folder-path/';













chsi_tier_migration_desc column not able to check, since the file size is large

dmas_alteryx_etl.cro_eligible_dly_fact_v3/dmas_alteryx_etl.cro_eligible_dly_fact_v3.csv
ds_order_funnel/ds_order_funnel_0128_unique.csv
ds_order_funnel/ds_order_funnel_0128.csv
ds_order_funnel/ds_order_funnel.csv
DS_revenue_cro_eligible.csv
MDU_QC_WOs/MDU_QC_WOS_202411051740.csv





chsi_tier_migration_desc is present
ds_revenue_15dec_23dec.csv
ds_revenue_summary.csv


chsi_tier_migration_desc is not present
DIM_REGION/DIM_REGION_202410290239.csv
DIM_SITE/DIM_SITE_202410290253.csv
DIM_SUB_REGION/DIM_SUB_REGION_202410290230.csv
chsi_tier_migration_desc is present
MDU_PROPERTY_ATTRIBUTES_CURR_202411041909.csvv



LRP_Targets_v02042025_Missing_Revenue.csv
LRP_Targets_v02262024.csv

account_dim
`customer_key` double, 
  `account_nbr` double, 
  `site_id` smallint, 
  `res_com_ind` string, 
  `customer_status` string, 
  `house_type` string, 
  `account_guid` string, 
  `user_guid_primary` string, 
  `employee_flag` tinyint, 
  `test_account_flag` double, 
  `inception_date` string, 
  `sale_acquisition_channel` string, 
  `registration_date` timestamp, 
  `registration_traffic_source_detail` string, 
  `registration_traffic_source` string, 
  `data_flag` tinyint, 
  `tv_flag` tinyint, 
  `phone_flag` tinyint, 
  `homelife_flag` string, 
  `mobile_flag` string, 
  `sub_status_desc` string, 
  `email_count` bigint, 
  `notification_email_flag` string, 
  `notification_phone_flag` string, 
  `email_verified_flag` string, 
  `phone_verified_flag` string, 
  `email_verified_date` string, 
  `phone_verified_date` string, 
  `email_opt_out` string, 
  `phone_opt_out` string, 
  `pano_flag` string, 
  `pano_device` string, 
  `easy_pay_flag` string, 
  `do_not_call_flag` tinyint, 
  `do_not_email_flag` tinyint, 
  `do_not_mail_flag` tinyint, 
  `do_not_market_flag` tinyint, 
  `last_contacted_date_ivr_call` date, 
  `last_contacted_date_cox_com` date, 
  `last_contacted_date_cox_app` date, 
  `cox_segment` string, 
  `demographic_info1` string, 
  `demographic_info2` string)
  `time_key` string


profile_dim
`user_guid` string, 
  `res_com_ind` string, 
  `primary_flag` string, 
  `customer_key` double, 
  `account_nbr` double, 
  `site_id` smallint, 
  `customer_status` string, 
  `inception_date` string, 
  `registration_date` timestamp, 
  `user_permission` string, 
  `preferred_contact_method` string, 
  `placeholder2` string, 
  `tsv_enrolled_status` string, 
  `tsv_email_flag` int, 
  `tsv_call_flag` int, 
  `tsv_sms_flag` int, 
  `preference_placeholder1` string, 
  `preference_placeholder2` string, 
  `preferences_last_used_date` string, 
  `last_logged_in_date_okta` string, 
  `last_logged_in_os_cox_app` string, 
  `last_password_change_date` string
  `time_key` string

transaction_adobe_fact
`customer_key` bigint, 
  `user_guid` string, 
  `adobe_ecid` string, 
  `adobe_visit_id` string, 
  `activity_date` string, 
  `registration_date` timestamp, 
  `inception_date` string, 
  `navigation_step` string, 
  `methods` string, 
  `feature` string, 
  `activity_page` string, 
  `p10` string, 
  `previous_page` string, 
  `activity_pagename` string, 
  `server_error` string, 
  `client_error` string, 
  `traffic_source_detail_sc_id` string
  `activity_name` string



transaction_okta_day_agg
  `event_date` date, 
  `authentication_host` string, 
  `authentication_channel` string, 
  `authentication_attempt` bigint, 
  `authentication_success_result` bigint, 
  `authentication_error` string, 
  `activity_type` string
  `authentication_method` string


transaction_okta_user_agg
user_guid` string, 
  `authentication_host` string, 
  `authentication_channel` string, 
  `authentication_attempt` bigint, 
  `authentication_success_result` bigint, 
  `authentication_error` string, 
  `activity_type` string
  authentication_method` string



account_dim_old
`customer_key` double, 
  `account_nbr` double, 
  `site_id` smallint, 
  `res_com_ind` string, 
  `customer_status` string, 
  `house_type` string, 
  `account_guid` string, 
  `user_guid_primary` string, 
  `employee_flag` tinyint, 
  `test_account_flag` double, 
  `inception_date` string, 
  `sale_acquisition_channel` string, 
  `registration_date` timestamp, 
  `registration_traffic_source_detail` string, 
  `registration_traffic_source` varchar(16), 
  `data_flag` tinyint, 
  `tv_flag` tinyint, 
  `phone_flag` tinyint, 
  `homelife_flag` string, 
  `mobile_flag` varchar(1), 
  `sub_status_desc` string, 
  `email_count` bigint, 
  `notification_email_flag` varchar(1), 
  `notification_phone_flag` varchar(1), 
  `email_verified_flag` varchar(1), 
  `phone_verified_flag` varchar(1), 
  `email_verified_date` string, 
  `phone_verified_date` string, 
  `email_opt_out` string, 
  `phone_opt_out` string, 
  `pano_flag` string, 
  `pano_device` string, 
  `easy_pay_flag` string, 
  `do_not_call_flag` tinyint, 
  `do_not_email_flag` tinyint, 
  `do_not_mail_flag` tinyint, 
  `do_not_market_flag` tinyint, 
  `last_contacted_date_ivr_call` date, 
  `last_contacted_date_cox_com` date, 
  `last_contacted_date_cox_app` date, 
  `cox_segment` string, 
  `demographic_info1` string, 
  `demographic_info2` string
  `time_key` string


profile_dim_old
`user_guid` string, 
  `res_com_ind` string, 
  `primary_flag` string, 
  `customer_key` double, 
  `account_nbr` double, 
  `site_id` smallint, 
  `customer_status` string, 
  `inception_date` string, 
  `registration_date` timestamp, 
  `user_permission` string, 
  `preferred_contact_method` string, 
  `placeholder2` string, 
  `tsv_enrolled_status` varchar(25), 
  `tsv_email_flag` int, 
  `tsv_call_flag` int, 
  `tsv_sms_flag` int, 
  `preference_placeholder1` string, 
  `preference_placeholder2` string, 
  `preferences_last_used_date` string, 
  `last_logged_in_date_okta` string, 
  `last_logged_in_os_cox_app` varchar(7), 
  `last_password_change_date` string
  `time_key` string

transaction_adobe_fact_old
customer_key` bigint, 
  `user_guid` string, 
  `adobe_ecid` string, 
  `adobe_visit_id` string, 
  `activity_date` string, 
  `registration_date` timestamp, 
  `inception_date` string, 
  `activity_name` varchar(22), 
  `navigation_step` varchar(18), 
  `methods` varchar(22), 
  `feature` varchar(18), 
  `activity_page` string, 
  `p10` string, 
  `previous_page` string, 
  `activity_pagename` string, 
  `server_error` string, 
  `client_error` string, 
  `traffic_source_detail_sc_id` string


transaction_okta_day_agg_old
`event_date` date, 
  `authentication_host` string, 
  `authentication_channel` string, 
  `authentication_attempt` bigint, 
  `authentication_success_result` bigint, 
  `authentication_error` string, 
  `authentication_method` varchar(17), 
  `activity_type` varchar(12)

transaction_okta_user_agg
`user_guid` string, 
  `authentication_host` string, 
  `authentication_channel` string, 
  `authentication_attempt` bigint, 
  `authentication_success_result` bigint, 
  `authentication_error` string, 
  `activity_type` string


transaction_okta_user_agg_old
  `user_guid` string, 
  `authentication_host` string, 
  `authentication_channel` string, 
  `authentication_attempt` bigint, 
  `authentication_success_result` bigint, 
  `authentication_error` string, 
  `authentication_method` varchar(17), 
  `activity_type` varchar(12)
