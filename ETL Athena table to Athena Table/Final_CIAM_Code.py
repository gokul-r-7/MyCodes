import sys
import re
import uuid
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#SQL Queries
account_dim_sum_13 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           CAST(c.account_nbr AS varchar) AS account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           CAST(c.account_guid AS varchar) AS account_guid,
           CAST(c.prim_account_holder_guid AS varchar) AS prim_account_holder_guid,
           c.test_account_key AS test_account_key,
           c.inception_dt
    FROM edw.customer_dim c
),
revenue_data AS (
    SELECT r.customer_key,
           r.dwelling_type_key,
           MAX(r.time_key) AS MAX_Time_Key
    FROM edw.customer_revenue_fact r
    GROUP BY r.customer_key, r.dwelling_type_key
),
dwelling_data AS (
    SELECT distinct d.dwelling_type_key,
           d.dwelling_type_desc
    FROM edw.dwelling_type_dim d
),
account_summary AS (
    SELECT
           s.CUSTOMER_TYPE_CD,
           s.account_nbr,
           s.customer_key,
           s.employee_flag,
           s.data_flag,
           s.site_id,
           s.cable_flag,
           s.wireless_flag,
           s.easy_pay_flag,
           s.do_not_call_flag,
           s.do_not_email_flag,
           s.do_not_mail_flag,
           s.telephony_flag ,
           s.do_not_market_flag,
           s.time_key
    FROM edw.cust_acct_sum s
    WHERE
    DATE_PARSE(s.time_key, '%Y-%m-%d') >= DATE_ADD('month', -13, CURRENT_DATE)
),
guid_data AS (
    SELECT g.customer_key,
           g.create_dt,
           g.household_member_guid,
           ROW_NUMBER() over (partition by g.customer_key order by g.create_dt asc) as rn
           FROM edw.customer_guid_dtl_dim g
),
ivr_contact AS (
    SELECT distinct i.customer_key,
           MAX(i.time_key) AS Last_Contacted_Date_IVR_Call
    FROM "call"."call_ivr_fact" i
    GROUP BY i.customer_key
),
web_data as(select d.customer_key, MAX(d.dt) AS Last_Contacted_Date_Cox_com  FROM webanalytics.web_contact_history d group by d.customer_key),
web_contact AS (
    SELECT
        campaign,
        evar61_coxcust_guid,
        dt
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
),
app_contact AS (
    SELECT
        coxcust_guid_v61,
        dt,
        post_evar40
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
),
mob_data as(select mob.customer_key, MAX(mob.dt) AS Last_Contacted_Date_Cox_App from mobile_data_temp.app_contact_history mob group by mob.customer_key)
SELECT distinct
    s.customer_key AS Customer_Key,
    s.account_nbr AS Account_Nbr,
    s.site_id AS Site_Id,
    c.res_comm_ind AS Res_Com_Ind,
    c.customer_status_cd AS Customer_Status,
    d.dwelling_type_desc AS House_Type,
    c.account_guid AS Account_GUID,
    c.prim_account_holder_guid AS User_GUID_Primary,
    s.employee_flag AS Employee_Flag,
    c.test_account_key AS Test_Account_Flag,
    c.inception_dt AS Inception_Date,
    CAST(NULL AS varchar) AS "Sale_Acquisition_Channel",
    g.create_dt AS Registration_Date,
    COALESCE(web.campaign, app.post_evar40) AS Registration_Traffic_Source_Detail,
    CASE 
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em%' THEN 'email'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_sms%' THEN 'sms'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_dm%' THEN 'direct mail'
        WHEN SUBSTRING(LOWER(COALESCE(web.campaign, app.post_evar40)), LENGTH(COALESCE(web.campaign, app.post_evar40)) - 5, 6) = 'vanity' THEN 'Vanity URL'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE '%panoapp%' THEN 'panoapp'
        ELSE 'undefined'
    END AS Registration_Traffic_Source,
    s.data_flag AS Data_Flag,
    s.cable_flag AS TV_Flag,
    s.telephony_flag AS "Phone_Flag",
    CAST(NULL AS varchar) AS "Homelife_Flag",
    s.wireless_flag AS Mobile_Flag,
    CAST(NULL AS varchar) AS "Pano_Flag",
    CAST(NULL AS varchar) AS "Pano_Device",
    s.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    ivr.Last_Contacted_Date_IVR_Call,
    d.Last_Contacted_Date_Cox_com,
    mob.Last_Contacted_Date_Cox_App, 
    CAST(NULL AS varchar) AS "Cox_Segment",
    CAST(NULL AS varchar) AS "Demographic_Info1",
    CAST(NULL AS varchar) AS "Demographic_Info2",
    s.time_key
FROM account_summary s LEFT JOIN customer_data c on CAST(s.customer_key AS bigint)=c.customer_key 
LEFT JOIN revenue_data r ON s.customer_key = CAST(r.customer_key AS bigint)
LEFT JOIN (select * from guid_data where rn=1) g ON s.customer_key = CAST(g.customer_key AS bigint)
LEFT JOIN ivr_contact ivr ON s.customer_key = CAST(ivr.customer_key AS bigint)
LEFT JOIN web_data d on s.customer_key=CAST(d.customer_key as bigint)
LEFT JOIN mob_data mob on s.customer_key=CAST(mob.customer_key as bigint)
LEFT JOIN web_contact web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN app_contact app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
"""
account_dim_sum_1 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           CAST(c.account_nbr AS varchar) AS account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           CAST(c.account_guid AS varchar) AS account_guid,
           CAST(c.prim_account_holder_guid AS varchar) AS prim_account_holder_guid,
           c.test_account_key AS test_account_key,
           c.inception_dt
    FROM edw.customer_dim c
),
revenue_data AS (
    SELECT r.customer_key,
           r.dwelling_type_key,
           MAX(r.time_key) AS MAX_Time_Key
    FROM edw.customer_revenue_fact r
    GROUP BY r.customer_key, r.dwelling_type_key
),
dwelling_data AS (
    SELECT distinct d.dwelling_type_key,
           d.dwelling_type_desc
    FROM edw.dwelling_type_dim d
),
account_summary AS (
    SELECT
           s.CUSTOMER_TYPE_CD,
           s.account_nbr,
           s.customer_key,
           s.employee_flag,
           s.data_flag,
           s.site_id,
           s.cable_flag,
           s.wireless_flag,
           s.easy_pay_flag,
           s.do_not_call_flag,
           s.do_not_email_flag,
           s.do_not_mail_flag,
           s.telephony_flag ,
           s.do_not_market_flag,
           s.time_key
    FROM edw.cust_acct_sum s
    WHERE
    DATE_PARSE(s.time_key, '%Y-%m-%d') >= DATE_ADD('month', -1, CURRENT_DATE)
),
guid_data AS (
    SELECT g.customer_key,
           g.create_dt,
           g.household_member_guid,
           ROW_NUMBER() over (partition by g.customer_key order by g.create_dt asc) as rn
           FROM edw.customer_guid_dtl_dim g
),
ivr_contact AS (
    SELECT distinct i.customer_key,
           MAX(i.time_key) AS Last_Contacted_Date_IVR_Call
    FROM "call"."call_ivr_fact" i
    GROUP BY i.customer_key
),
web_data as(select d.customer_key, MAX(d.dt) AS Last_Contacted_Date_Cox_com  FROM webanalytics.web_contact_history d group by d.customer_key),
web_contact AS (
    SELECT
        campaign,
        evar61_coxcust_guid,
        dt
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
),
app_contact AS (
    SELECT
        coxcust_guid_v61,
        dt,
        post_evar40
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
),
mob_data as(select mob.customer_key, MAX(mob.dt) AS Last_Contacted_Date_Cox_App from mobile_data_temp.app_contact_history mob group by mob.customer_key)
SELECT distinct
    s.customer_key AS Customer_Key,
    s.account_nbr AS Account_Nbr,
    s.site_id AS Site_Id,
    c.res_comm_ind AS Res_Com_Ind,
    c.customer_status_cd AS Customer_Status,
    d.dwelling_type_desc AS House_Type,
    c.account_guid AS Account_GUID,
    c.prim_account_holder_guid AS User_GUID_Primary,
    s.employee_flag AS Employee_Flag,
    c.test_account_key AS Test_Account_Flag,
    c.inception_dt AS Inception_Date,
    CAST(NULL AS varchar) AS "Sale_Acquisition_Channel",
    g.create_dt AS Registration_Date,
    COALESCE(web.campaign, app.post_evar40) AS Registration_Traffic_Source_Detail,
    CASE 
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em%' THEN 'email'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_sms%' THEN 'sms'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_dm%' THEN 'direct mail'
        WHEN SUBSTRING(LOWER(COALESCE(web.campaign, app.post_evar40)), LENGTH(COALESCE(web.campaign, app.post_evar40)) - 5, 6) = 'vanity' THEN 'Vanity URL'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE '%panoapp%' THEN 'panoapp'
        ELSE 'undefined'
    END AS Registration_Traffic_Source,
    s.data_flag AS Data_Flag,
    s.cable_flag AS TV_Flag,
    s.telephony_flag AS "Phone_Flag",
    CAST(NULL AS varchar) AS "Homelife_Flag",
    s.wireless_flag AS Mobile_Flag,
    CAST(NULL AS varchar) AS "Pano_Flag",
    CAST(NULL AS varchar) AS "Pano_Device",
    s.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    ivr.Last_Contacted_Date_IVR_Call,
    d.Last_Contacted_Date_Cox_com,
    mob.Last_Contacted_Date_Cox_App, 
    CAST(NULL AS varchar) AS "Cox_Segment",
    CAST(NULL AS varchar) AS "Demographic_Info1",
    CAST(NULL AS varchar) AS "Demographic_Info2",
    s.time_key
FROM account_summary s LEFT JOIN customer_data c on CAST(s.customer_key AS bigint)=c.customer_key 
LEFT JOIN revenue_data r ON s.customer_key = CAST(r.customer_key AS bigint)
LEFT JOIN (select * from guid_data where rn=1) g ON s.customer_key = CAST(g.customer_key AS bigint)
LEFT JOIN ivr_contact ivr ON s.customer_key = CAST(ivr.customer_key AS bigint)
LEFT JOIN web_data d on s.customer_key=CAST(d.customer_key as bigint)
LEFT JOIN mob_data mob on s.customer_key=CAST(mob.customer_key as bigint)
LEFT JOIN web_contact web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN app_contact app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
"""

profile_dim_sum_13 = """
WITH customer_dim AS (
    SELECT 
        b.customer_key AS Customer_Key,
        b.account_nbr AS Account_Nbr,
        b.site_id AS Site_Id,
        b.customer_status_cd AS Customer_Status,
        b.inception_dt AS Inception_Date,
        b.res_comm_ind AS Res_Com_Ind,
        b.test_account_key 
    FROM
        edw.customer_dim b
),
account_summary AS (
    SELECT
           s.CUSTOMER_TYPE_CD,
           s.account_nbr,
           s.customer_key,
           s.site_id,
           s.time_key
    FROM edw.cust_acct_sum s
    WHERE DATE_PARSE(s.time_key, '%Y-%m-%d') >= DATE_ADD('month', -13, CURRENT_DATE)
),
guid_data AS (
    SELECT 
        a.customer_key,
        a.create_dt AS Registration_date,
        a.user_id,
        a.prim_customer_flag AS Primary_Flag,
        a.household_member_guid AS User_GUID,
        a.cox_email_address,  
        email_counts.Email_Account_Count
    FROM
        edw.customer_guid_dtl_dim a
    INNER JOIN 
        edw.customer_dim b ON b.customer_key = a.customer_key
    LEFT JOIN (
        SELECT 
            a.cox_email_address,
            COUNT(DISTINCT b.customer_key) AS Email_Account_Count
        FROM 
            edw.customer_guid_dtl_dim a
        INNER JOIN 
            edw.customer_dim b ON b.customer_key = a.customer_key
        WHERE 
            b.customer_status_cd = 'A'
            AND b.employee_flag = 0
            AND b.res_comm_ind = 'R'
            AND b.customer_type_cd = '1'
            AND b.test_account_key = 2
        GROUP BY 
            a.cox_email_address
        HAVING 
            COUNT(DISTINCT b.customer_key) >= 1
    ) email_counts ON a.cox_email_address = email_counts.cox_email_address
    ),
web_contact AS (
    SELECT
        w.customer_key,
        MAX(w.dt) AS Last_Logged_In_Date_Cox_com
    FROM
        webanalytics.web_contact_history w
    GROUP BY
        w.customer_key
),
mob_data as(
    select mob.customer_key, MAX(mob.dt) AS Last_Logged_In_Date_Cox_App from mobile_data_temp.app_contact_history mob group by mob.customer_key
),
app_contact AS (
    SELECT
        m.customer_key,
        MAX(m.post_mobileappid) AS Last_Logged_In_App_ID
    FROM
        mobile_data_temp.app_contact_history m
    GROUP BY
        m.customer_key
),
latest_event_dates AS (
    SELECT
        a.user_id,
        d.outcome_reason,
        MAX(d.event_date) AS latest_event_date
    FROM
        edw.customer_guid_dtl_dim a
        LEFT JOIN ciam.mfa_factors_enrolled d ON a.user_id = d.username
    WHERE
        d.outcome_result = 'SUCCESS'
    GROUP BY
        a.user_id, d.outcome_reason
),
mfa_data AS (
    SELECT DISTINCT
        a.user_id,
        MAX(o.event_date) AS last_logged_in_date_okta,
        c.eventtype AS TSV_Enrolled_Status,
        c.event_date AS max_event_date,
        MAX(CASE WHEN d.outcome_reason LIKE '%EMAIL%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_EMAIL_Flag,
        MAX(CASE WHEN d.outcome_reason LIKE '%CALL%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_CALL_Flag,
        MAX(CASE WHEN d.outcome_reason LIKE '%SMS%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_SMS_Flag
    FROM
        edw.customer_guid_dtl_dim a
        LEFT JOIN (
            SELECT
                username,
                eventtype,
                event_date,
                ROW_NUMBER() OVER (PARTITION BY username ORDER BY event_date DESC) AS rn
            FROM
                ciam.mfa_total_mfa_users
            WHERE
                outcome_result = 'SUCCESS'
        ) c ON a.user_id = c.username AND c.rn = 1
        LEFT JOIN ciam.mfa_factors_enrolled d ON a.user_id = d.username
        LEFT JOIN latest_event_dates e ON a.user_id = e.user_id AND d.outcome_reason = e.outcome_reason
        LEFT JOIN ciam.successful_authentications_okta o ON a.user_id = o.actor_alternateid
    GROUP BY
        a.user_id, c.eventtype, c.event_date
),
account_details AS (
    SELECT 
    ac.account_nbr,
    ac.vndr_cust_account_key,
    ac.do_not_email AS Email_Opt_Out,
    ac.do_not_call AS Phone_Opt_Out,
    em.bounced_flg AS Email_Bounce,
    em.unsubscribe_flg
FROM 
    camp_mgmt.accounts ac 
LEFT JOIN 
    camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT
        customer_number,
        MAX(CASE WHEN notification_method = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Email_Flag,
        MAX(CASE WHEN notification_method = 'PHONE' THEN 'Y' ELSE 'N' END) AS Phone_Flag,
        MAX(CASE WHEN notification_method = 'EMAIL' AND confirm_flag = 'Y' THEN 'Y' ELSE 'N' END) AS Email_Verified_Flag,
        MAX(CASE WHEN notification_method = 'PHONE' AND confirm_flag = 'Y' THEN 'Y' ELSE 'N' END) AS Phone_Verified_Flag,
        MAX(CASE 
            WHEN notification_method = 'EMAIL' THEN 
                CASE 
                    WHEN verification_date != 0 THEN 
                        CONCAT('20', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 2, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 4, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 6, 2)
                        )
                    ELSE NULL 
                END
        END) AS email_verified_date,
        MAX(CASE 
            WHEN notification_method = 'PHONE' THEN 
                CASE 
                    WHEN verification_date != 0 THEN 
                        CONCAT('20', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 2, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 4, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 6, 2)
                        )
                    ELSE NULL 
                END
        END) AS phone_verified_date
    FROM pstage.ALL_CUST_NOTIFICATION_METHODS n 
    GROUP BY customer_number
)
SELECT DISTINCT a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    s.Customer_Key,
    s.Account_Nbr,
    s.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    NULL AS "User_Permission",
    a.Email_Account_Count,
    nf.Email_Flag,
    nf.Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,   
    ad.Email_Bounce, 
    NULL AS "Preferred_Contact_Method",
    NULL AS "Placeholder2",
    mfa.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    NULL AS "Preference_Placeholder1",
    NULL AS "Preference_Placeholder2",
    NULL AS "Preferences_Last_Used_Date",
    mfa.last_logged_in_date_okta,
    web.Last_Logged_In_Date_Cox_com,
    mob.Last_Logged_In_Date_Cox_App,
    CASE 
        WHEN app.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN app.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END AS "Last_Logged_In_OS_Cox_App",
    NULL AS "Last_Password_Change_Date",
    s.time_key
FROM account_summary s left join customer_dim b on s.customer_key=b.customer_key
left join guid_data a on s.customer_key=a.customer_key 
LEFT JOIN mob_data mob on s.customer_key=CAST(mob.customer_key as bigint)
LEFT JOIN web_contact web ON s.customer_key = CAST(web.customer_key AS bigint)
LEFT JOIN app_contact app ON s.customer_key = CAST(app.customer_key AS bigint)
LEFT JOIN  mfa_data mfa ON a.user_id = mfa.user_id
LEFT JOIN account_details ad ON s.account_nbr = ad.account_nbr
LEFT JOIN notification_flags nf ON s.account_nbr = nf.customer_number
"""

profile_dim_sum_1 = """
WITH customer_dim AS (
    SELECT 
        b.customer_key AS Customer_Key,
        b.account_nbr AS Account_Nbr,
        b.site_id AS Site_Id,
        b.customer_status_cd AS Customer_Status,
        b.inception_dt AS Inception_Date,
        b.res_comm_ind AS Res_Com_Ind,
        b.test_account_key 
    FROM
        edw.customer_dim b
),
account_summary AS (
    SELECT
           s.CUSTOMER_TYPE_CD,
           s.account_nbr,
           s.customer_key,
           s.site_id,
           s.time_key
    FROM edw.cust_acct_sum s
    WHERE 
    DATE_PARSE(s.time_key, '%Y-%m-%d') >= DATE_ADD('month', -1, CURRENT_DATE)
),
guid_data AS (
    SELECT 
        a.customer_key,
        a.create_dt AS Registration_date,
        a.user_id,
        a.prim_customer_flag AS Primary_Flag,
        a.household_member_guid AS User_GUID,
        a.cox_email_address,  
        email_counts.Email_Account_Count
    FROM
        edw.customer_guid_dtl_dim a
    INNER JOIN 
        edw.customer_dim b ON b.customer_key = a.customer_key
    LEFT JOIN (
        SELECT 
            a.cox_email_address,
            COUNT(DISTINCT b.customer_key) AS Email_Account_Count
        FROM 
            edw.customer_guid_dtl_dim a
        INNER JOIN 
            edw.customer_dim b ON b.customer_key = a.customer_key
        WHERE 
            b.customer_status_cd = 'A'
            AND b.employee_flag = 0
            AND b.res_comm_ind = 'R'
            AND b.customer_type_cd = '1'
            AND b.test_account_key = 2
        GROUP BY 
            a.cox_email_address
        HAVING 
            COUNT(DISTINCT b.customer_key) >= 1
    ) email_counts ON a.cox_email_address = email_counts.cox_email_address
    ),
web_contact AS (
    SELECT
        w.customer_key,
        MAX(w.dt) AS Last_Logged_In_Date_Cox_com
    FROM
        webanalytics.web_contact_history w
    GROUP BY
        w.customer_key
),
mob_data as(
    select mob.customer_key, MAX(mob.dt) AS Last_Logged_In_Date_Cox_App from mobile_data_temp.app_contact_history mob group by mob.customer_key
),
app_contact AS (
    SELECT
        m.customer_key,
        MAX(m.post_mobileappid) AS Last_Logged_In_App_ID
    FROM
        mobile_data_temp.app_contact_history m
    GROUP BY
        m.customer_key
),
latest_event_dates AS (
    SELECT
        a.user_id,
        d.outcome_reason,
        MAX(d.event_date) AS latest_event_date
    FROM
        edw.customer_guid_dtl_dim a
        LEFT JOIN ciam.mfa_factors_enrolled d ON a.user_id = d.username
    WHERE
        d.outcome_result = 'SUCCESS'
    GROUP BY
        a.user_id, d.outcome_reason
),
mfa_data AS (
    SELECT DISTINCT
        a.user_id,
        MAX(o.event_date) AS last_logged_in_date_okta,
        c.eventtype AS TSV_Enrolled_Status,
        c.event_date AS max_event_date,
        MAX(CASE WHEN d.outcome_reason LIKE '%EMAIL%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_EMAIL_Flag,
        MAX(CASE WHEN d.outcome_reason LIKE '%CALL%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_CALL_Flag,
        MAX(CASE WHEN d.outcome_reason LIKE '%SMS%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_SMS_Flag
    FROM
        edw.customer_guid_dtl_dim a
        LEFT JOIN (
            SELECT
                username,
                eventtype,
                event_date,
                ROW_NUMBER() OVER (PARTITION BY username ORDER BY event_date DESC) AS rn
            FROM
                ciam.mfa_total_mfa_users
            WHERE
                outcome_result = 'SUCCESS'
        ) c ON a.user_id = c.username AND c.rn = 1
        LEFT JOIN ciam.mfa_factors_enrolled d ON a.user_id = d.username
        LEFT JOIN latest_event_dates e ON a.user_id = e.user_id AND d.outcome_reason = e.outcome_reason
        LEFT JOIN ciam.successful_authentications_okta o ON a.user_id = o.actor_alternateid
    GROUP BY
        a.user_id, c.eventtype, c.event_date
),
account_details AS (
    SELECT 
    ac.account_nbr,
    ac.vndr_cust_account_key,
    ac.do_not_email AS Email_Opt_Out,
    ac.do_not_call AS Phone_Opt_Out,
    em.bounced_flg AS Email_Bounce,
    em.unsubscribe_flg
FROM 
    camp_mgmt.accounts ac 
LEFT JOIN 
    camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT
        customer_number,
        MAX(CASE WHEN notification_method = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Email_Flag,
        MAX(CASE WHEN notification_method = 'PHONE' THEN 'Y' ELSE 'N' END) AS Phone_Flag,
        MAX(CASE WHEN notification_method = 'EMAIL' AND confirm_flag = 'Y' THEN 'Y' ELSE 'N' END) AS Email_Verified_Flag,
        MAX(CASE WHEN notification_method = 'PHONE' AND confirm_flag = 'Y' THEN 'Y' ELSE 'N' END) AS Phone_Verified_Flag,
        MAX(CASE 
            WHEN notification_method = 'EMAIL' THEN 
                CASE 
                    WHEN verification_date != 0 THEN 
                        CONCAT('20', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 2, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 4, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 6, 2)
                        )
                    ELSE NULL 
                END
        END) AS email_verified_date,
        MAX(CASE 
            WHEN notification_method = 'PHONE' THEN 
                CASE 
                    WHEN verification_date != 0 THEN 
                        CONCAT('20', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 2, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 4, 2), '-', 
                               SUBSTRING(CAST(verification_date AS VARCHAR), 6, 2)
                        )
                    ELSE NULL 
                END
        END) AS phone_verified_date
    FROM pstage.ALL_CUST_NOTIFICATION_METHODS n 
    GROUP BY customer_number
)
SELECT DISTINCT a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    s.Customer_Key,
    s.Account_Nbr,
    s.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    NULL AS "User_Permission",
    a.Email_Account_Count,
    nf.Email_Flag,
    nf.Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,   
    ad.Email_Bounce, 
    NULL AS "Preferred_Contact_Method",
    NULL AS "Placeholder2",
    mfa.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    NULL AS "Preference_Placeholder1",
    NULL AS "Preference_Placeholder2",
    NULL AS "Preferences_Last_Used_Date",
    mfa.last_logged_in_date_okta,
    web.Last_Logged_In_Date_Cox_com,
    mob.Last_Logged_In_Date_Cox_App,
    CASE 
        WHEN app.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN app.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END AS "Last_Logged_In_OS_Cox_App",
    NULL AS "Last_Password_Change_Date",
    s.time_key
FROM account_summary s left join customer_dim b on s.customer_key=b.customer_key
left join guid_data a on s.customer_key=a.customer_key 
LEFT JOIN mob_data mob on s.customer_key=CAST(mob.customer_key as bigint)
LEFT JOIN web_contact web ON s.customer_key = CAST(web.customer_key AS bigint)
LEFT JOIN app_contact app ON s.customer_key = CAST(app.customer_key AS bigint)
LEFT JOIN  mfa_data mfa ON a.user_id = mfa.user_id
LEFT JOIN account_details ad ON s.account_nbr = ad.account_nbr
LEFT JOIN notification_flags nf ON s.account_nbr = nf.customer_number
"""

transaction_adobe_fact = """
SELECT DISTINCT 
    COALESCE(w.evar61_coxcust_guid, a.coxcust_guid_v61) AS User_guid,
    COALESCE(w.evar75_marketing_cloud_id) AS Adobe_ECID,
    COALESCE(w.visits, a.visits) AS Adobe_Visit_Id,
    COALESCE(w.date_time, a.date_time) AS Activity_Date,
    CASE 
        WHEN w.pagename LIKE 'cox:res:myprofile:communication%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:contacts%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:disability%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:email:confirmation%' THEN 'Email Verification'
        WHEN w.pagename LIKE 'cox:res:myprofile:forgot-password:%' THEN 'Forgot Password'
        WHEN w.pagename LIKE 'cox:res:myprofile:forgot-userid:%' THEN 'Forgot UserID'
        WHEN w.pagename LIKE 'cox:res:myprofile:forgotuserid:%' THEN 'Forgot UserID'
        WHEN w.pagename LIKE 'cox:res:myprofile:fuid:%' THEN 'Forgot UserID'
        WHEN w.pagename LIKE 'cox:res:myprofile:home%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:mailing-address%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:mailingaddress%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:manageusers%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:notifications%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:number-lock-protection%' THEN 'Number Lock Protection'
        WHEN w.pagename LIKE 'cox:res:myprofile:password%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:privacy%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:reg:%' THEN 'Registration'
        WHEN w.pagename LIKE 'cox:res:myprofile:security%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:updateprofile%' THEN 'Manage Profile'
        WHEN w.pagename LIKE 'cox:res:myprofile:syncupaccount%' THEN 'Sync Account'
        WHEN w.pagename LIKE 'cox:res:myprofile:tsv' THEN 'TSV Enrollment'
        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:email:%' THEN 'TSV Verification'
        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:call:%' THEN 'TSV Verification'
        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:text:%' THEN 'TSV Verification'
        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:reset:%' THEN 'TSV Reset'
        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:confirmation%' THEN 'Verify Contact'
        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:email%' THEN 'Verify Contact'
        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:landing%' THEN 'Verify Contact'
        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:phone%' THEN 'Verify Contact'
        ELSE 'Unknown'
    END AS Activity_Name,
    COALESCE(w.pagename, a.pagename) AS Activity_Page,
    COALESCE(w.mvvar3, a.server_form_error_p13) AS Server_Error,
    NULL AS Client_Error,
    COALESCE(w.campaign, a.post_evar40) AS Traffic_Source_Detail_sc_id
from 
edw.customer_guid_dtl_dim a2
LEFT JOIN 
edw.customer_dim b ON a2.customer_key = b.customer_key
LEFT JOIN 
webanalytics.web_contact_history w ON a2.household_member_guid = w.evar61_coxcust_guid
LEFT JOIN 
mobile_data_temp.app_contact_history a 
on a2.household_member_guid = a.coxcust_guid_v61
WHERE
    DATE_PARSE(SUBSTR(CAST(a.dt AS varchar), 1, 19), '%Y-%m-%d') >= DATE_ADD('day', -90, CURRENT_DATE)
    AND DATE_PARSE(SUBSTR(CAST(w.dt AS varchar), 1, 19), '%Y-%m-%d') >= DATE_ADD('day', -90, CURRENT_DATE)
    AND (a.pagename LIKE 'coxapp:reg:%' OR a.pagename LIKE 'coxapp:myaccount%')
    AND w.pagename LIKE 'cox:res:myprofile%'
"""
transaction_okta_user_agg_fact = """
WITH filtered_auth AS (
    SELECT
        actor_alternateid AS user_id,
        host,
        uuid,
        outcome_result,
        CAST(event_date AS DATE) AS event_date
    FROM
        ciam.successful_authentications_okta
    WHERE
        CAST(event_date AS DATE) >= DATE_ADD('day', -90, CURRENT_DATE)
),
filtered_signon AS (
    SELECT
        username AS user_id,
        application,
        CAST(event_date AS DATE) AS event_date
    FROM
        ciam.single_signon_okta
    WHERE
        CAST(event_date AS DATE) >= DATE_ADD('day', -90, CURRENT_DATE)
),
aggregated_auth AS (
    SELECT
        user_id,
        host,
        COUNT(uuid) AS Authentication_Attempt,
        COUNT(DISTINCT CASE WHEN outcome_result = 'SUCCESS' THEN uuid END) AS authentication_success_result
    FROM
        filtered_auth
    GROUP BY
        user_id, host
),
aggregated_signon AS (
    SELECT
        user_id,
        application
    FROM
        filtered_signon
    GROUP BY
        user_id, application
)
SELECT
    g.household_member_guid AS User_GUID,
a.host AS Authentication_Host,
    o.application AS Authentication_Channel,
    a.Authentication_Attempt,
    a.authentication_success_result,
    NULL AS Authentication_Error,
    'Login Credentials' AS authentication_method,
    CASE
        WHEN m.eventtype = 'group.user_membership.add' THEN 'tsv enrolled'
        ELSE NULL
    END AS activity_type
FROM
    edw.customer_guid_dtl_dim g
LEFT JOIN
    aggregated_auth a ON g.user_id = a.user_id
LEFT JOIN
    aggregated_signon o ON g.user_id = o.user_id
LEFT JOIN
    ciam.mfa_total_mfa_users m ON g.user_id = m.username
"""
transcation_okta_day_agg = """
WITH filtered_auth AS (
    SELECT
        actor_alternateid AS user_id,
        CAST(event_date AS DATE) AS event_date,
        host,
        uuid,
        outcome_result
    FROM
        ciam.successful_authentications_okta
    WHERE
        CAST(event_date AS DATE) >= DATE_ADD('year', -1, CURRENT_DATE)
),
filtered_signon AS (
    SELECT
        username AS user_id,
        application,
        CAST(event_date AS DATE) AS event_date
    FROM
        ciam.single_signon_okta
    WHERE
        CAST(event_date AS DATE) >= DATE_ADD('year', -1, CURRENT_DATE)
),
aggregated_auth AS (
    SELECT
        user_id,
        event_date,
        host,
        COUNT(uuid) AS Authentication_Attempt,
        COUNT(DISTINCT CASE WHEN outcome_result = 'SUCCESS' THEN uuid END) AS authentication_success_result
    FROM
        filtered_auth
    GROUP BY
        user_id, event_date, host
),
aggregated_signon AS (
    SELECT
        user_id,
        event_date,
        application
    FROM
        filtered_signon
    GROUP BY
        user_id, event_date, application
)
SELECT
    a.event_date AS Event_Date,
a.host AS Authentication_Host,
    o.application AS Authentication_Channel,
    a.Authentication_Attempt,
    a.authentication_success_result,
    NULL AS Authentication_Error,
    'Login Credentials' AS Authentication_method,
    CASE
        WHEN m.eventtype = 'group.user_membership.add' THEN 'tsv enrolled'
        ELSE NULL
    END AS Activity_type
FROM
    edw.customer_guid_dtl_dim g
LEFT JOIN
    aggregated_auth a ON g.user_id = a.user_id
LEFT JOIN
    aggregated_signon o ON g.user_id = o.user_id AND a.event_date = o.event_date
LEFT JOIN
    ciam.mfa_total_mfa_users m ON g.user_id = m.username
GROUP BY
a.event_date, a.host, o.application, m.eventtype,a.Authentication_Attempt,a.authentication_success_result
"""

# Athena Query Results Temporary files s3 path
account_dim_sum_temp = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/account_dim_sum_temp/"
profile_dim_sum_temp = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/profile_dim_sum_temp/"
transaction_adobe_fact_temp = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transaction_adobe_fact_temp/"
transaction_okta_user_agg_fact_temp = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transaction_okta_user_agg_fact_temp/"
transcation_okta_day_agg_temp = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transcation_okta_day_agg_temp/"

#Output s3 path
account_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/account_dim_sum/"
profile_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/profile_dim_sum/"
transaction_adobe_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transaction_adobe_fact/"
transaction_okta_user_agg_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transaction_okta_user_agg_fact/"
transcation_okta_day_agg_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transcation_okta_day_agg_fact/"

#PartitionKeys for the target files
account_dim_sum_partitionkeys = ["time_key"]
profile_dim_sum_partitionkeys = ["time_key"]
transaction_adobe_fact_partitionkeys = []
transaction_okta_user_agg_fact_partitionkeys = []
transcation_okta_day_agg_partitionkeys = []

glue_client = boto3.client('glue')
s3 = boto3.client('s3')
starttime = datetime.now()
start_time = starttime.strftime("%Y-%m-%d %H:%M:%S")
unique_id = str(uuid.uuid4())
job_name = "CIAM_ETL"
job_log_database_name = "default"
job_log_table_name = "job_log_table"
job_log_table_path = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/job_log_table/"

def job_lob_table_data(job_load_type,endtime,runtimeseconds,account_dim_count,profile_dim_count,adobe_fact_count,user_agg_fact_count,day_agg_count):
    job_log_data = {
    "id" : unique_id,
    "job_name" : job_name,
    "job_load_type" : job_load_type,
    "job_start_time" : start_time,
    "job_end_time" : endtime,
    "job_run_time" : runtimeseconds,
    "account_dim_sum_count" : account_dim_count,
    "profile_dim_sum_count" : profile_dim_count,
    "transaction_adobe_fact_count" : adobe_fact_count,
    "transaction_okta_user_agg_fact_count" : user_agg_fact_count,
    "transcation_okta_day_agg_output" :  day_agg_count,
    "created_date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    df = spark.createDataFrame([job_log_data])
    return df

def check_table_exists(database_name, table_name):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        # If the table exists, response will contain metadata
        return True
    except ClientError as e:
        # If the table doesn't exist, an exception is raised
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            return False
        else:
            raise  # Rethrow the exception if it's not a table not found error

# Usage example
exists = check_table_exists(job_log_database_name, job_log_table_name)

if exists:
    print(f"Table {job_log_table_name} exists in database {job_log_database_name}.")
else:
    print(f"Table {job_log_table_name} does not exist in database {job_log_database_name}.")

def check_load_type(database_name, table_name):
    # Read the Glue catalog table as a dynamic frame
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name
    )

    # Convert to DataFrame and check if 'LoadType' contains 'Latest13months'
    df = dynamic_frame.toDF()
    
    # Assuming 'LoadType' column is of string type
    load_type_values = df.select("job_load_type").distinct().collect()

    for row in load_type_values:
        if row['job_load_type'] == 'Latest 13 Months':
            return True
    return False


def load_data_from_athena(sql_query,temp_path,load_full_data=True):
    if load_full_data:
        # Read all data from Athena (Glue Catalog)
        read_df = (
        glueContext.read.format("jdbc")
        .option("driver", "com.simba.athena.jdbc.Driver")
        .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
        .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443;QueryTimeout=25200")
        .option("dbtable", f"({sql_query})")
        .option("S3OutputLocation",temp_path)
        .load()
        )
    else:
        # Read only the latest month data (Assuming a `Month` partition or similar column exists)
        # Replace `Month` with actual partition/column name for the month
        read_df = (
        glueContext.read.format("jdbc")
        .option("driver", "com.simba.athena.jdbc.Driver")
        .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
        .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443;QueryTimeout=25200")
        .option("dbtable", f"({sql_query})")
        .option("S3OutputLocation",temp_path)
        .load()
        )   
    return df

def write_to_s3(df,output_path,partitionkey):
    write_df = df.write \
    .partitionBy(partitionkey) \
    .format("parquet") \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .save(output_path)
    return write_df


s3_output_path ="s3://gokul-test-bucket-07/time_key_target/"
partition_keys = ["time_key"]
# Step 1: Check if the table exists
if check_table_exists(job_log_database_name, job_log_table_name):
    
    # Step 2: Check if 'LoadType' contains 'Latest13months'
    if check_load_type(job_log_database_name, job_log_table_name):
        # If 'Latest13months' is present, load only the latest month data
        account_dim_sum_df = load_data_from_athena(account_dim_sum_1,account_dim_sum_temp,load_full_data=False)
        profile_dim_sum_df = load_data_from_athena(profile_dim_sum_1,profile_dim_sum_temp,load_full_data=False)
        transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact,transaction_adobe_fact_temp,load_full_data=False)
        transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact,transaction_okta_user_agg_fact_temp,load_full_data=False)
        transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg,transcation_okta_day_agg_temp,load_full_data=False)
        account_dim_sum_df.printSchema()
        profile_dim_sum_df.printSchema()
        transaction_adobe_fact_df.printSchema()
        transaction_okta_user_agg_fact_df.printSchema()
        transcation_okta_day_agg_df.printSchema()
        account_dim_sum_df.show()
        profile_dim_sum_df.show()
        transaction_adobe_fact_df.show()
        transaction_okta_user_agg_fact_df.show()
        transcation_okta_day_agg_df.show()
        account_dim_sum_record_count = account_dim_sum_df.count()
        profile_dim_sum_record_count = profile_dim_sum_df.count()
        transaction_adobe_fact_record_count = transaction_adobe_fact_df.count()
        transaction_okta_user_agg_fact_record_count = transaction_okta_user_agg_fact_df.count()
        transcation_okta_day_agg_record_count = transcation_okta_day_agg_df.count()
        loadtype = "Latest Current Month"
    else:
        # If 'Latest13months' is not present, load all data
        account_dim_sum_df = load_data_from_athena(account_dim_sum_13,account_dim_sum_temp,load_full_data=True)
        profile_dim_sum_df = load_data_from_athena(profile_dim_sum_13,profile_dim_sum_temp,load_full_data=True)
        transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact,transaction_adobe_fact_temp,load_full_data=True)
        transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact,transaction_okta_user_agg_fact_temp,load_full_data=True)
        transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg,transcation_okta_day_agg_temp,load_full_data=True)
        account_dim_sum_df.printSchema()
        profile_dim_sum_df.printSchema()
        transaction_adobe_fact_df.printSchema()
        transaction_okta_user_agg_fact_df.printSchema()
        transcation_okta_day_agg_df.printSchema()
        account_dim_sum_df.show()
        profile_dim_sum_df.show()
        transaction_adobe_fact_df.show()
        transaction_okta_user_agg_fact_df.show()
        transcation_okta_day_agg_df.show()
        account_dim_sum_record_count = account_dim_sum_df.count()
        profile_dim_sum_record_count = profile_dim_sum_df.count()
        transaction_adobe_fact_record_count = transaction_adobe_fact_df.count()
        transaction_okta_user_agg_fact_record_count = transaction_okta_user_agg_fact_df.count()
        transcation_okta_day_agg_record_count = transcation_okta_day_agg_df.count()
        loadtype = "Latest 13 Months"

    
    # Step 3: Write the loaded data to S3
    account_dim_sum_df_write_df = write_to_s3(account_dim_sum_df,account_dim_sum_output,account_dim_sum_partitionkeys)
    profile_dim_sum_write_df = write_to_s3(profile_dim_sum_df,profile_dim_sum_output,profile_dim_sum_partitionkeys)
    transaction_adobe_fact_write_df = write_to_s3(transaction_adobe_fact_df,transaction_adobe_fact_output,transaction_adobe_fact_partitionkeys)
    transaction_okta_user_agg_fact_write_df = write_to_s3(transaction_okta_user_agg_fact_df,transaction_okta_user_agg_fact_output,transaction_okta_user_agg_fact_partitionkeys)
    transcation_okta_day_agg_write_df = write_to_s3(transcation_okta_day_agg_df,transcation_okta_day_agg_output,transcation_okta_day_agg_partitionkeys)
    account_dim_sum_df_write_df.show()
    profile_dim_sum_write_df.show()
    transaction_adobe_fact_write_df.show()
    transaction_okta_user_agg_fact_write_df.show()
    transcation_okta_day_agg_write_df.show()
    
else:
    months13_load_df = load_data_from_athena(load_full_data=True)
     # If 'Latest13months' is not present, load all data
    account_dim_sum_df = load_data_from_athena(account_dim_sum_13,account_dim_sum_temp,load_full_data=True)
    profile_dim_sum_df = load_data_from_athena(profile_dim_sum_13,profile_dim_sum_temp,load_full_data=True)
    transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact,transaction_adobe_fact_temp,load_full_data=True)
    transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact,transaction_okta_user_agg_fact_temp,load_full_data=True)
    transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg,transcation_okta_day_agg_temp,load_full_data=True)
    account_dim_sum_df.printSchema()
    profile_dim_sum_df.printSchema()
    transaction_adobe_fact_df.printSchema()
    transaction_okta_user_agg_fact_df.printSchema()
    transcation_okta_day_agg_df.printSchema()
    account_dim_sum_df.show()
    profile_dim_sum_df.show()
    transaction_adobe_fact_df.show()
    transaction_okta_user_agg_fact_df.show()
    transcation_okta_day_agg_df.show()
    account_dim_sum_record_count = account_dim_sum_df.count()
    profile_dim_sum_record_count = profile_dim_sum_df.count()
    transaction_adobe_fact_record_count = transaction_adobe_fact_df.count()
    transaction_okta_user_agg_fact_record_count = transaction_okta_user_agg_fact_df.count()
    transcation_okta_day_agg_record_count = transcation_okta_day_agg_df.count()
    loadtype = "Latest 13 Months"
 

endtime = datetime.now()
end_time = endtime.strftime("%Y-%m-%d %H:%M:%S")
run_time = endtime - starttime
runtime_str = str(run_time)
hours, remainder = divmod(run_time.seconds, 3600)
minutes, seconds = divmod(remainder, 60)
runtime_formatted = f"{hours:02}:{minutes:02}:{seconds:02}"
job_log_table_df = job_lob_table_data(loadtype,endtime,runtime_formatted,account_dim_sum_record_count,profile_dim_sum_record_count,transaction_adobe_fact_record_count,transaction_okta_user_agg_fact_record_count,transcation_okta_day_agg_record_count)
job_log_table_df.show()
job_log_table_write_df = job_log_table_df.write.format("parquet").mode("append").save(job_log_table_path)


bucket_name = "cci-dig-aicoe-data-sb" 
folder_paths = ["processed/ciam_data/account_dim_sum_output/","processed/ciam_data/profile_dim_sum_output/"]
# Define the regex pattern to match 'time_key' partitions
time_key_pattern = re.compile(r'time_key=(\d{4}-\d{2}-\d{2})/')
# Define a cutoff date for keeping only the last 13 months
cutoff_date = datetime.now() - timedelta(days=13 * 30)  # Roughly 13 months

def get_partition_dates(prefix):
    """Get all 'time_key' partitions as dates from the given S3 prefix."""
    partition_dates = []
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in result:
        for obj in result['Contents']:
            match = time_key_pattern.search(obj['Key'])
            if match:
                date_str = match.group(1)
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                partition_dates.append(date_obj)               
    return sorted(set(partition_dates))



def delete_old_partitions(prefix):
    """Delete partitions older than 13 months."""
    partition_dates = get_partition_dates(prefix)
    if len(partition_dates) > 13:
        for date in partition_dates[:-13]:  # Keep only the last 13 months
            partition_prefix = f"{prefix}time_key={date.strftime('%Y-%m-%d')}/"
            result = s3.list_objects_v2(Bucket=bucket_name, Prefix=partition_prefix)
            if 'Contents' in result:
                for obj in result['Contents']:
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"Deleted partition: {partition_prefix}")

# Run for deletion check on both folders
for folder in folder_paths:
    delete_old_partitions(folder)



job.commit()
