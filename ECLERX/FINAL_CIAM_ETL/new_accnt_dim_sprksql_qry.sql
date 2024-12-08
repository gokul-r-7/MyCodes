account_dim_sum_new_query = """
WITH customer_data AS (
    SELECT DISTINCT c.customer_key,
           CAST(c.account_nbr AS VARCHAR(255)) AS account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           CAST(c.account_guid AS VARCHAR(255)) AS account_guid,
           CAST(c.prim_account_holder_guid AS VARCHAR(255)) AS prim_account_holder_guid,
           c.test_account_key AS test_account_key,
           c.inception_dt
    FROM edw.customer_dim c
    WHERE test_account_key = 2
),
revenue_data AS (
    SELECT r.customer_key,
           r.site_id,
           r.dwelling_type_key,
           r.easy_pay_flag,
           CASE WHEN r.mobile_gross_mrc > 0 THEN '1' ELSE '0' END AS Mobile_Flag,
           r.customer_substatus_key,
           r.time_key
    FROM edw.customer_revenue_fact r
    WHERE r.bill_type_key != 2 AND
          to_date(r.time_key, 'yyyy-MM-dd') >= ADD_MONTHS(CURRENT_DATE, -13)
),
customer_status AS (
    SELECT customer_substatus_key, sub_status_desc 
    FROM edw.customer_substatus_dim
),
dwelling_data AS (
    SELECT DISTINCT d.dwelling_type_key,
           d.dwelling_type_desc
    FROM edw.dwelling_type_dim d
),
account_summary AS (
    SELECT s.customer_key,
           s.employee_flag,
           s.data_flag,
           s.site_id,
           s.cable_flag,
           s.wireless_flag,
           s.do_not_call_flag,
           s.do_not_email_flag,
           s.do_not_mail_flag,
           s.telephony_flag,
           s.do_not_market_flag,
           s.time_key,
           ROW_NUMBER() OVER (PARTITION BY s.customer_key ORDER BY s.time_key DESC) AS rn
    FROM edw.cust_acct_sum s
),
guid_data AS (
    SELECT g.customer_key,
           g.create_dt,
           g.household_member_guid,
           ROW_NUMBER() OVER (PARTITION BY g.customer_key ORDER BY g.create_dt ASC) AS rn
    FROM edw.customer_guid_dtl_dim g
),
ivr_contact AS (
    SELECT i.customer_key,
           DATE_FORMAT(CAST(i.time_key AS TIMESTAMP), 'yyyy-MM') AS Contact_Month,
           MAX(i.time_key) AS Last_Contacted_Date_IVR_Call
    FROM `call`.`call_ivr_fact` i
    GROUP BY i.customer_key, DATE_FORMAT(CAST(i.time_key AS TIMESTAMP), 'yyyy-MM')
),
web_data AS (
    SELECT d.customer_key,
           DATE_FORMAT(CAST(d.dt AS TIMESTAMP), 'yyyy-MM') AS Contact_Month,
           MAX(d.dt) AS Last_Contacted_Date_Cox_com,
           ROW_NUMBER() OVER (PARTITION BY d.customer_key ORDER BY dt) AS rn
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS TIMESTAMP), 'yyyy-MM'), dt
),
mob_data AS (
    SELECT mob.customer_key,
           DATE_FORMAT(CAST(mob.dt AS TIMESTAMP), 'yyyy-MM') AS Contact_Month,
           MAX(mob.dt) AS Last_Contacted_Date_Cox_App,
           ROW_NUMBER() OVER (PARTITION BY mob.customer_key ORDER BY dt) AS rn
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS TIMESTAMP), 'yyyy-MM'), dt
),
web_contact AS (
    SELECT campaign,
           evar61_coxcust_guid,
           MIN(dt) AS dt,
           ROW_NUMBER() OVER (PARTITION BY evar61_coxcust_guid ORDER BY MIN(dt)) AS rn
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
    GROUP BY campaign, evar61_coxcust_guid
),
app_contact AS (
    SELECT coxcust_guid_v61,
           MIN(dt) AS dt,
           post_evar40,
           ROW_NUMBER() OVER (PARTITION BY coxcust_guid_v61 ORDER BY MIN(dt)) AS rn
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
    GROUP BY coxcust_guid_v61, post_evar40
)
SELECT DISTINCT
    r.customer_key AS Customer_Key,
    c.account_nbr AS Account_Nbr,
    r.site_id AS Site_Id,
    c.res_comm_ind AS Res_Com_Ind,
    c.customer_status_cd AS Customer_Status,
    d.dwelling_type_desc AS House_Type,
    c.account_guid AS Account_GUID,
    c.prim_account_holder_guid AS User_GUID_Primary,
    s.employee_flag AS Employee_Flag,
    c.test_account_key AS Test_Account_Flag,
    c.inception_dt AS Inception_Date,
    CAST(NULL AS VARCHAR(255)) AS Sale_Acquisition_Channel,
    g.create_dt AS Registration_Date,
    COALESCE(web.campaign, app.post_evar40) AS Registration_Traffic_Source_Detail,
    CASE
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_cns_ocall_event255' THEN 'Email-Order'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_z_acct_onb%' THEN 'Email-Onboarding'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em%' THEN 'Email-Others'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_sms%' THEN 'sms'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_dm%' THEN 'direct mail'
        WHEN SUBSTRING(LOWER(COALESCE(web.campaign, app.post_evar40)), LENGTH(COALESCE(web.campaign, app.post_evar40)) - 5, 6) = 'vanity' THEN 'Vanity URL'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE '%panoapp%' THEN 'panoapp'
        WHEN COALESCE(web.campaign, app.post_evar40) IS NOT NULL AND LENGTH(COALESCE(web.campaign, app.post_evar40)) > 0 THEN 'organic'
        ELSE 'null'  
    END AS Registration_Traffic_Source,
    s.data_flag AS Data_Flag,
    s.cable_flag AS TV_Flag,
    s.telephony_flag AS Phone_Flag,
    CAST(NULL AS VARCHAR(255)) AS Homelife_Flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    CAST(NULL AS VARCHAR(255)) AS Pano_Flag,
    CAST(NULL AS VARCHAR(255)) AS Pano_Device,
    r.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    ivr.Last_Contacted_Date_IVR_Call,
    w.Last_Contacted_Date_Cox_com,
    mob.Last_Contacted_Date_Cox_App, 
    CAST(NULL AS VARCHAR(255)) AS Cox_Segment,
    CAST(NULL AS VARCHAR(255)) AS Demographic_Info1,
    CAST(NULL AS VARCHAR(255)) AS Demographic_Info2,
    r.time_key
FROM revenue_data r
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
LEFT JOIN (SELECT * FROM account_summary WHERE rn = 1) s ON r.customer_key = CAST(s.customer_key AS DOUBLE)
LEFT JOIN customer_status cd ON r.customer_substatus_key = cd.customer_substatus_key 
LEFT JOIN customer_data c ON r.customer_key = CAST(c.customer_key AS DOUBLE)
LEFT JOIN (SELECT * FROM guid_data WHERE rn = 1) g ON r.customer_key = CAST(g.customer_key AS DOUBLE)
LEFT JOIN ivr_contact ivr ON r.customer_key = CAST(ivr.customer_key AS DOUBLE)
    AND DATE_FORMAT(CAST(r.time_key AS TIMESTAMP), 'yyyy-MM') >= ivr.Contact_Month
LEFT JOIN web_data w ON r.customer_key = CAST(w.customer_key AS DOUBLE)
    AND DATE_FORMAT(CAST(r.time_key AS TIMESTAMP), 'yyyy-MM') >= w.Contact_Month AND w.rn = 1
LEFT JOIN mob_data mob ON r.customer_key = CAST(mob.customer_key AS DOUBLE) AND mob.rn = 1
    AND DATE_FORMAT(CAST(r.time_key AS TIMESTAMP), 'yyyy-MM') >= mob.Contact_Month
LEFT JOIN (SELECT * FROM web_contact WHERE rn = 1) web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN (SELECT * FROM app_contact WHERE rn = 1) app ON g.household_member_guid = app.coxcust_guid_v61
"""
