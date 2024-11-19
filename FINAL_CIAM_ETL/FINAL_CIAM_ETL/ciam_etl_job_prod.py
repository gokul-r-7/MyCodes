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
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
import logging


# Setup CloudWatch logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Configure requester pays settings for S3 access
glueContext._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
spark._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# Initialize Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#SQL Queries
account_dim_sum_13 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           CAST(c.account_nbr AS varchar(255)) AS account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           CAST(c.account_guid AS varchar(255)) AS account_guid,
           CAST(c.prim_account_holder_guid AS varchar(255)) AS prim_account_holder_guid,
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
    to_date(s.time_key, 'yyyy-MM-dd') >= current_date() - INTERVAL 13 MONTH
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
    FROM `call`.call_ivr_fact i
    GROUP BY i.customer_key
),
web_data AS (
    SELECT d.customer_key, MAX(d.dt) AS Last_Contacted_Date_Cox_com  
    FROM webanalytics.web_contact_history d 
    GROUP BY d.customer_key
),
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
mob_data AS (
    SELECT mob.customer_key, MAX(mob.dt) AS Last_Contacted_Date_Cox_App 
    FROM mobile_data_temp.app_contact_history mob 
    GROUP BY mob.customer_key
)
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
    CAST(NULL AS varchar(255)) AS `Sale_Acquisition_Channel`,
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
    s.telephony_flag AS `Phone_Flag`,
    CAST(NULL AS varchar(255)) AS `Homelife_Flag`,
    s.wireless_flag AS Mobile_Flag,
    CAST(NULL AS varchar(255)) AS `Pano_Flag`,
    CAST(NULL AS varchar(255)) AS `Pano_Device`,
    s.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    ivr.Last_Contacted_Date_IVR_Call,
    d.Last_Contacted_Date_Cox_com,
    mob.Last_Contacted_Date_Cox_App, 
    CAST(NULL AS varchar(255)) AS `Cox_Segment`,
    CAST(NULL AS varchar(255)) AS `Demographic_Info1`,
    CAST(NULL AS varchar(255)) AS `Demographic_Info2`,
    s.time_key
FROM account_summary s 
LEFT JOIN customer_data c ON CAST(s.customer_key AS bigint) = c.customer_key 
LEFT JOIN revenue_data r ON s.customer_key = CAST(r.customer_key AS bigint)
LEFT JOIN (SELECT * FROM guid_data WHERE rn = 1) g ON s.customer_key = CAST(g.customer_key AS bigint)
LEFT JOIN ivr_contact ivr ON s.customer_key = CAST(ivr.customer_key AS bigint)
LEFT JOIN web_data d ON s.customer_key = CAST(d.customer_key AS bigint)
LEFT JOIN mob_data mob ON s.customer_key = CAST(mob.customer_key AS bigint)
LEFT JOIN web_contact web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN app_contact app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
"""
account_dim_sum_1 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           CAST(c.account_nbr AS varchar(255)) AS account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           CAST(c.account_guid AS varchar(255)) AS account_guid,
           CAST(c.prim_account_holder_guid AS varchar(255)) AS prim_account_holder_guid,
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
    to_date(s.time_key, 'yyyy-MM-dd') >= date_add(current_date(), -30)
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
    FROM `call`.call_ivr_fact i
    GROUP BY i.customer_key
),
web_data AS (
    SELECT d.customer_key, MAX(d.dt) AS Last_Contacted_Date_Cox_com  
    FROM webanalytics.web_contact_history d 
    GROUP BY d.customer_key
),
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
mob_data AS (
    SELECT mob.customer_key, MAX(mob.dt) AS Last_Contacted_Date_Cox_App 
    FROM mobile_data_temp.app_contact_history mob 
    GROUP BY mob.customer_key
)
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
    CAST(NULL AS varchar(255)) AS `Sale_Acquisition_Channel`,
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
    s.telephony_flag AS `Phone_Flag`,
    CAST(NULL AS varchar(255)) AS `Homelife_Flag`,
    s.wireless_flag AS Mobile_Flag,
    CAST(NULL AS varchar(255)) AS `Pano_Flag`,
    CAST(NULL AS varchar(255)) AS `Pano_Device`,
    s.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    ivr.Last_Contacted_Date_IVR_Call,
    d.Last_Contacted_Date_Cox_com,
    mob.Last_Contacted_Date_Cox_App, 
    CAST(NULL AS varchar(255)) AS `Cox_Segment`,
    CAST(NULL AS varchar(255)) AS `Demographic_Info1`,
    CAST(NULL AS varchar(255)) AS `Demographic_Info2`,
    s.time_key
FROM account_summary s 
LEFT JOIN customer_data c ON CAST(s.customer_key AS bigint) = c.customer_key 
LEFT JOIN revenue_data r ON s.customer_key = CAST(r.customer_key AS bigint)
LEFT JOIN (SELECT * FROM guid_data WHERE rn = 1) g ON s.customer_key = CAST(g.customer_key AS bigint)
LEFT JOIN ivr_contact ivr ON s.customer_key = CAST(ivr.customer_key AS bigint)
LEFT JOIN web_data d ON s.customer_key = CAST(d.customer_key AS bigint)
LEFT JOIN mob_data mob ON s.customer_key = CAST(mob.customer_key AS bigint)
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
    WHERE to_date(s.time_key, 'yyyy-MM-dd') >= current_date() - INTERVAL 13 MONTH
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
mob_data AS (
    SELECT
        mob.customer_key, 
        MAX(mob.dt) AS Last_Logged_In_Date_Cox_App 
    FROM 
        mobile_data_temp.app_contact_history mob 
    GROUP BY 
        mob.customer_key
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
        em.unsubscribe_flg,
        ROW_NUMBER() OVER (PARTITION BY ac.account_nbr ORDER BY CASE WHEN em.bounced_flg IS NOT NULL THEN 1 ELSE 2 END) AS rn
    FROM camp_mgmt.accounts ac
    LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT
    cmcnbr AS customer_number,
    MAX(CASE WHEN cmmeth = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Email_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' THEN 'Y' ELSE 'N' END) AS Phone_Flag,
    MAX(CASE WHEN cmmeth = 'EMAIL' AND cmcffl = 'Y' THEN 'Y' ELSE 'N' END) AS Email_Verified_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' AND cmcffl = 'Y' THEN 'Y' ELSE 'N' END) AS Phone_Verified_Flag,

    -- Formatting the email verification date, ensuring non-empty string handling
    MAX(CASE 
        WHEN cmmeth = 'EMAIL' THEN 
            CASE 
                WHEN cmcfdt IS NOT NULL AND cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS email_verified_date,

    -- Formatting the phone verification date, ensuring non-empty string handling
    MAX(CASE 
        WHEN cmmeth = 'PHONE' THEN 
            CASE 
                WHEN cmcfdt IS NOT NULL AND cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS phone_verified_date

FROM pstage.stg_all_cust_notification_methods n 
GROUP BY cmcnbr
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
    NULL AS `User_Permission`,
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
    NULL AS `Preferred_Contact_Method`,
    NULL AS `Placeholder2`,
    mfa.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    NULL AS `Preference_Placeholder1`,
    NULL AS `Preference_Placeholder2`,
    NULL AS `Preferences_Last_Used_Date`,
    mfa.last_logged_in_date_okta,
    web.Last_Logged_In_Date_Cox_com,
    mob.Last_Logged_In_Date_Cox_App,
    CASE 
        WHEN app.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN app.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END AS `Last_Logged_In_OS_Cox_App`,
    NULL AS `Last_Password_Change_Date`,
    s.time_key
FROM account_summary s 
LEFT JOIN customer_dim b ON s.customer_key = b.customer_key
LEFT JOIN guid_data a ON s.customer_key = a.customer_key 
LEFT JOIN mob_data mob ON s.customer_key = CAST(mob.customer_key AS bigint)
LEFT JOIN web_contact web ON s.customer_key = CAST(web.customer_key AS bigint)
LEFT JOIN app_contact app ON s.customer_key = CAST(app.customer_key AS bigint)
LEFT JOIN mfa_data mfa ON a.user_id = mfa.user_id
LEFT JOIN account_details ad ON s.account_nbr = ad.account_nbr AND ad.rn = 1
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
    WHERE to_date(s.time_key, 'yyyy-MM-dd') >= date_add(current_date(), -30)
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
mob_data AS (
    SELECT
        mob.customer_key, 
        MAX(mob.dt) AS Last_Logged_In_Date_Cox_App 
    FROM 
        mobile_data_temp.app_contact_history mob 
    GROUP BY 
        mob.customer_key
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
        em.unsubscribe_flg,
        ROW_NUMBER() OVER (PARTITION BY ac.account_nbr ORDER BY CASE WHEN em.bounced_flg IS NOT NULL THEN 1 ELSE 2 END) AS rn
    FROM camp_mgmt.accounts ac
    LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT
    cmcnbr AS customer_number,
    MAX(CASE WHEN cmmeth = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Email_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' THEN 'Y' ELSE 'N' END) AS Phone_Flag,
    MAX(CASE WHEN cmmeth = 'EMAIL' AND cmcffl = 'Y' THEN 'Y' ELSE 'N' END) AS Email_Verified_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' AND cmcffl = 'Y' THEN 'Y' ELSE 'N' END) AS Phone_Verified_Flag,

    -- Formatting the email verification date, ensuring non-empty string handling
    MAX(CASE 
        WHEN cmmeth = 'EMAIL' THEN 
            CASE 
                WHEN cmcfdt IS NOT NULL AND cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS email_verified_date,

    -- Formatting the phone verification date, ensuring non-empty string handling
    MAX(CASE 
        WHEN cmmeth = 'PHONE' THEN 
            CASE 
                WHEN cmcfdt IS NOT NULL AND cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS phone_verified_date

FROM pstage.stg_all_cust_notification_methods n 
GROUP BY cmcnbr
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
    NULL AS `User_Permission`,
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
    NULL AS `Preferred_Contact_Method`,
    NULL AS `Placeholder2`,
    mfa.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    NULL AS `Preference_Placeholder1`,
    NULL AS `Preference_Placeholder2`,
    NULL AS `Preferences_Last_Used_Date`,
    mfa.last_logged_in_date_okta,
    web.Last_Logged_In_Date_Cox_com,
    mob.Last_Logged_In_Date_Cox_App,
    CASE 
        WHEN app.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN app.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END AS `Last_Logged_In_OS_Cox_App`,
    NULL AS `Last_Password_Change_Date`,
    s.time_key
FROM account_summary s 
LEFT JOIN customer_dim b ON s.customer_key = b.customer_key
LEFT JOIN guid_data a ON s.customer_key = a.customer_key 
LEFT JOIN mob_data mob ON s.customer_key = CAST(mob.customer_key AS bigint)
LEFT JOIN web_contact web ON s.customer_key = CAST(web.customer_key AS bigint)
LEFT JOIN app_contact app ON s.customer_key = CAST(app.customer_key AS bigint)
LEFT JOIN mfa_data mfa ON a.user_id = mfa.user_id
LEFT JOIN account_details ad ON s.account_nbr = ad.account_nbr AND ad.rn = 1
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
FROM 
    edw.customer_guid_dtl_dim a2
LEFT JOIN 
    edw.customer_dim b ON a2.customer_key = b.customer_key
LEFT JOIN 
    webanalytics.web_contact_history w ON a2.household_member_guid = w.evar61_coxcust_guid
LEFT JOIN 
    mobile_data_temp.app_contact_history a 
    ON a2.household_member_guid = a.coxcust_guid_v61
WHERE
    to_date(SUBSTR(CAST(a.dt AS string), 1, 19), 'yyyy-MM-dd') >= DATE_SUB(CURRENT_DATE, 90)
    AND to_date(SUBSTR(CAST(w.dt AS string), 1, 19), 'yyyy-MM-dd') >= DATE_SUB(CURRENT_DATE, 90)
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
        CAST(event_date AS DATE) >= DATE_ADD(CURRENT_DATE, -90)
),
filtered_signon AS (
    SELECT
        username AS user_id,
        application,
        CAST(event_date AS DATE) AS event_date
    FROM
        ciam.single_signon_okta
    WHERE
        CAST(event_date AS DATE) >= DATE_ADD(CURRENT_DATE, -90)
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
        CAST(event_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR  -- Corrected here
),
filtered_signon AS (
    SELECT
        username AS user_id,
        application,
        CAST(event_date AS DATE) AS event_date
    FROM
        ciam.single_signon_okta
    WHERE
        CAST(event_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR  -- Corrected here
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
    a.event_date, a.host, o.application, m.eventtype, a.Authentication_Attempt, a.authentication_success_result
"""


#Output s3 path
account_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/account_dim_sum/"
profile_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/profile_dim_sum/"
transaction_adobe_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transaction_adobe_fact/"
transaction_okta_user_agg_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transaction_okta_user_agg_fact/"
transcation_okta_day_agg_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/transcation_okta_day_agg_fact/"


#PartitionKeys for the target files
account_dim_sum_partitionkeys = ["time_key"]
profile_dim_sum_partitionkeys = ["time_key"]
transaction_adobe_fact_partitionkeys = ["Activity_Name"]
transaction_okta_user_agg_fact_partitionkeys = ["authentication_method"]
transcation_okta_day_agg_partitionkeys = ["Authentication_method"]


# Initialize AWS clients
glue_client = boto3.client('glue')
s3 = boto3.client('s3')


# Job metadata
starttime = datetime.now()
start_time = starttime.strftime("%Y-%m-%d %H:%M:%S")
unique_id = str(uuid.uuid4())
job_name = "CIAM_ETL"
job_log_database_name = "ciam_test1"
job_log_table_name = "job_log_table"
job_log_table_path = "s3://cci-dig-aicoe-data-sb/processed/ciam_data/job_log_table/"




def job_lob_table_data(job_load_type, endtime, runtimeseconds, account_dim_count, profile_dim_count, adobe_fact_count, user_agg_fact_count, day_agg_count):
    
    """
    Create a DataFrame for job log data to be written to Glue catalog.
    
    Args:
        job_load_type (str): Type of the job load (e.g., 'Latest 13 Months')
        endtime (str): End time of the job
        runtimeseconds (str): Job runtime in seconds
        account_dim_count (int): Number of records in account dimension
        profile_dim_count (int): Number of records in profile dimension
        adobe_fact_count (int): Number of records in Adobe facts
        user_agg_fact_count (int): Number of records in user aggregation facts
        day_agg_count (int): Number of records in day aggregation
        
    Returns:
        DataFrame: A Spark DataFrame containing the job log data
    """

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
    try:
        df = spark.createDataFrame([job_log_data])
    except Exception as e:
        logger.error(f"Error creating job log data: {e}")
        raise e
    else:
        return df




def check_table_exists(database_name, table_name):
    
    """
    Check if a table exists in AWS Glue Catalog.
    
    Args:
        database_name (str): Name of the Glue database
        table_name (str): Name of the table to check
        
    Returns:
        bool: True if table exists, False otherwise
    """

    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        # If the table exists, response will contain metadata
    except ClientError as e:
        # If the table doesn't exist, an exception is raised
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            logger.info(f"Table {table_name} does not exist in database {database_name}.")
            return False
        elif e.response['Error']['Code'] == 'InvalidInputException':
            logger.info(f"Invalid Input Exception for Table {table_name} in database {database_name}.")
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceException':
            logger.info(f"Invalid Input Exception for Table {table_name} in database {database_name}.")
            raise e
        elif e.response['Error']['Code'] == 'OperationTimeoutException':
            logger.info(f"Internal Service Exception for Table {table_name} in database {database_name}.")
            raise e
        elif e.response['Error']['Code'] == 'GlueEncryptionException':
            logger.info(f"Glue Encryption Exception for Table {table_name} in database {database_name}.")
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotReadyException':
            logger.info(f"Resource NotRead Exception for Table {table_name} in database {database_name}.")
            raise e
        elif e.response['Error']['Code'] == 'FederationSourceException':
            logger.info(f"Federation Source Exception for Table {table_name} in database {database_name}.")
            raise e
        elif e.response['Error']['Code'] == 'FederationSourceRetryableException':
            logger.info(f"Federation Source Retryable Exception for Table {table_name} in database {database_name}.")
            raise e
        else:
            logger.error(f"Error checking if table exists: {e}")
            raise e # Rethrow the exception if it's not a table not found error
    else:
        return True

# Usage example
exists = check_table_exists(job_log_database_name, job_log_table_name)

if exists:
    print(f"Table {job_log_table_name} exists in database {job_log_database_name}.")
else:
    print(f"Table {job_log_table_name} does not exist in database {job_log_database_name}.")




def check_load_type(database_name, table_name):

    """
    Check if 'job_load_type' contains 'Latest 13 Months' in Glue table.
    
    Args:
        database_name (str): Name of the Glue database
        table_name (str): Name of the table to check
        
    Returns:
        bool: True if 'Latest 13 Months' exists, False otherwise
    """

    try:
        # Attempt to create a dynamic frame from the catalog
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name
        )
        # Convert the dynamic frame to a DataFrame
        df = dynamic_frame.toDF()
        
        # Select distinct 'job_load_type' values and collect them
        load_type_values = df.select("job_load_type").distinct().collect()

        # Check if any of the 'job_load_type' values equals 'Latest 13 Months'
        for row in load_type_values:
            if row['job_load_type'] == 'Latest 13 Months':
                return True
        
        # If 'Latest 13 Months' is not found, return False
        return False

    except Exception as e:
        # Catch all exceptions and log the error
        logger.error(f"Error checking load type: {e}")
        raise e

    else:
        # This block will be executed only if no exception was raised in the try block
        logger.info("Load type check completed successfully without errors.")




def load_data_from_athena(sql_query, load_full_data=True):

    """
    Load data from Athena using Spark SQL.
    
    Args:
        sql_query (str): SQL query to load data
        load_full_data (bool): Flag indicating whether to load full or partial data
        
    Returns:
        DataFrame: Spark DataFrame containing the loaded data
    """

    try:
        if load_full_data:
            logger.info(f"Loading full data for query: {sql_query}")
            read_df = spark.sql(sql_query)
        else:
            logger.info(f"Loading latest month data for query: {sql_query}")
            read_df = spark.sql(sql_query)
    except Exception as e:
        logger.error(f"Error loading data from Athena: {e}")
        raise e
    else:
        return read_df



def write_to_s3(df, output_path, partitionkey):

     """
    Write DataFrame to S3 with partitioning.
    
    Args:
        df (DataFrame): The DataFrame to write
        output_path (str): S3 output path
        partitionkey (list): List of columns to partition by
        
    Returns:
        DataFrame: The written DataFrame (for logging or chaining)
    """
    
    try:
        df = df.repartitionByRange(1, partitionkey)
        write_df = df.write \
            .partitionBy(partitionkey) \
            .format("parquet") \
            .option("compression", "gzip") \
            .mode("overwrite") \
            .save(output_path)
        logger.info(f"Data written to S3: {output_path}")
    except Exception as e:
        logger.error(f"Error writing data to S3: {e}")
        raise e
    else:
        return write_df



# Step 1: Check if the table exists
if check_table_exists(job_log_database_name, job_log_table_name):
    
    # Step 2: Check if 'LoadType' contains 'Latest13months'
    if check_load_type(job_log_database_name, job_log_table_name):
        # If 'Latest13months' is present, load only the latest month data
        account_dim_sum_df = load_data_from_athena(account_dim_sum_1,load_full_data=False)
        profile_dim_sum_df = load_data_from_athena(profile_dim_sum_1,load_full_data=False)
        transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact,load_full_data=False)
        transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact,load_full_data=False)
        transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg,load_full_data=False)
        loadtype = "Latest Current Month"
    else:
        # If 'Latest13months' is not present, load all data
        account_dim_sum_df = load_data_from_athena(account_dim_sum_13,load_full_data=True)
        profile_dim_sum_df = load_data_from_athena(profile_dim_sum_13,load_full_data=True)
        transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact,load_full_data=True)
        transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact,load_full_data=True)
        transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg,load_full_data=True)
        loadtype = "Latest 13 Months"

    
    account_dim_sum_df.cache()
    profile_dim_sum_df.cache()
    transaction_adobe_fact_df.cache()
    transaction_okta_user_agg_fact_df.cache()
    transcation_okta_day_agg_df.cache()


    account_dim_sum_df = account_dim_sum_df.withColumn("time_key", F.to_date(account_dim_sum_df["time_key"], "yyyy-MM-dd"))
    account_dim_sum_df.printSchema()
    for col_name, dtype in profile_dim_sum_df.dtypes:
        if dtype == 'void':
            profile_dim_sum_df = profile_dim_sum_df.withColumn(col_name, F.lit("").cast("string"))
    profile_dim_sum_df = profile_dim_sum_df.withColumn("time_key", F.to_date(profile_dim_sum_df["time_key"], "yyyy-MM-dd"))
    profile_dim_sum_df.printSchema()
    for col_name, dtype in transaction_adobe_fact_df.dtypes:
        if dtype == 'void':
            transaction_adobe_fact_df = transaction_adobe_fact_df.withColumn(col_name, F.lit("").cast("string"))
    transaction_adobe_fact_df.printSchema()
    for col_name, dtype in transaction_okta_user_agg_fact_df.dtypes:
        if dtype == 'void':
            transaction_okta_user_agg_fact_df = transaction_okta_user_agg_fact_df.withColumn(col_name, F.lit("").cast("string"))
    transaction_okta_user_agg_fact_df.printSchema()
    for col_name, dtype in transcation_okta_day_agg_df.dtypes:
        if dtype == 'void':
            transcation_okta_day_agg_df = transcation_okta_day_agg_df.withColumn(col_name, F.lit("").cast("string"))
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


    # Step 3: Write the loaded data to S3
    account_dim_sum_df_write_df = write_to_s3(account_dim_sum_df,account_dim_sum_output,account_dim_sum_partitionkeys)
    profile_dim_sum_write_df = write_to_s3(profile_dim_sum_df,profile_dim_sum_output,profile_dim_sum_partitionkeys)
    transaction_adobe_fact_write_df = write_to_s3(transaction_adobe_fact_df,transaction_adobe_fact_output,transaction_adobe_fact_partitionkeys)
    transaction_okta_user_agg_fact_write_df = write_to_s3(transaction_okta_user_agg_fact_df,transaction_okta_user_agg_fact_output,transaction_okta_user_agg_fact_partitionkeys)
    transcation_okta_day_agg_write_df = write_to_s3(transcation_okta_day_agg_df,transcation_okta_day_agg_output,transcation_okta_day_agg_partitionkeys)


else:


     # If 'Latest13months' is not present, load all data
    account_dim_sum_df = load_data_from_athena(account_dim_sum_13,load_full_data=True)
    profile_dim_sum_df = load_data_from_athena(profile_dim_sum_13,load_full_data=True)
    transaction_adobe_fact_df = load_data_from_athena(transaction_adobe_fact,load_full_data=True)
    transaction_okta_user_agg_fact_df = load_data_from_athena(transaction_okta_user_agg_fact,load_full_data=True)
    transcation_okta_day_agg_df = load_data_from_athena(transcation_okta_day_agg,load_full_data=True)


    account_dim_sum_df.cache()
    profile_dim_sum_df.cache()
    transaction_adobe_fact_df.cache()
    transaction_okta_user_agg_fact_df.cache()
    transcation_okta_day_agg_df.cache()


    account_dim_sum_df = account_dim_sum_df.withColumn("time_key", F.to_date(account_dim_sum_df["time_key"], "yyyy-MM-dd"))
    account_dim_sum_df.printSchema()
    for col_name, dtype in profile_dim_sum_df.dtypes:
        if dtype == 'void':
            profile_dim_sum_df = profile_dim_sum_df.withColumn(col_name, F.lit("").cast("string"))
    profile_dim_sum_df = profile_dim_sum_df.withColumn("time_key", F.to_date(profile_dim_sum_df["time_key"], "yyyy-MM-dd"))
    profile_dim_sum_df.printSchema()
    for col_name, dtype in transaction_adobe_fact_df.dtypes:
        if dtype == 'void':
            transaction_adobe_fact_df = transaction_adobe_fact_df.withColumn(col_name, F.lit("").cast("string"))
    transaction_adobe_fact_df.printSchema()
    for col_name, dtype in transaction_okta_user_agg_fact_df.dtypes:
        if dtype == 'void':
            transaction_okta_user_agg_fact_df = transaction_okta_user_agg_fact_df.withColumn(col_name, F.lit("").cast("string"))
    transaction_okta_user_agg_fact_df.printSchema()
    for col_name, dtype in transcation_okta_day_agg_df.dtypes:
        if dtype == 'void':
            transcation_okta_day_agg_df = transcation_okta_day_agg_df.withColumn(col_name, F.lit("").cast("string"))
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


endtime = datetime.now()
end_time = endtime.strftime("%Y-%m-%d %H:%M:%S")
run_time = endtime - starttime
runtime_str = str(run_time)
hours, remainder = divmod(run_time.seconds, 3600)
minutes, seconds = divmod(remainder, 60)
runtime_formatted = f"{hours:02}:{minutes:02}:{seconds:02}"


try:
    job_log_table_df = job_lob_table_data(loadtype,endtime,runtime_formatted,account_dim_sum_record_count,profile_dim_sum_record_count,transaction_adobe_fact_record_count,transaction_okta_user_agg_fact_record_count,transcation_okta_day_agg_record_count)
except Exception as e:
    logger.error(f"Error creating job log data {e}")
    raise e
else:
    logger.info("creating job log data")
    job_log_table_df.show()


try:
    job_log_table_write_df = job_log_table_df.write.format("parquet").mode("append").save(job_log_table_path)
    logger.info("writing job log data in s3 path")
except Exception as e:
    logger.error(f"Error writing job log data {e}")
    raise e



# Define S3 bucket name and folder paths to process
bucket_name = "cci-dig-aicoe-data-sb"
folder_paths = ["processed/ciam_data/account_dim_sum/", "processed/ciam_data/profile_dim_sum/"]



# Define the regex pattern to match 'time_key' partitions
time_key_pattern = re.compile(r'time_key=(\d{4}-\d{2}-\d{2})/')
# Define a cutoff date for keeping only the last 13 months
cutoff_date = datetime.now() - timedelta(days=13 * 30)  # Roughly 13 months




# Define the S3 bucket and folder paths to process
def get_partition_dates(prefix):
    """
    Retrieve all unique 'time_key' partition dates from S3 objects under the given prefix.
    
    Args:
        prefix (str): S3 folder path prefix to search for partition keys.
    
    Returns:
        list: A sorted list of datetime objects representing partition dates.
    """
    partition_dates = []  # List to store partition dates
    
    try:
        # List objects under the given S3 prefix
        result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        # Check if 'Contents' is present in the result
        if 'Contents' in result:
            for obj in result['Contents']:
                try:
                    # Attempt to match the time_key using regex
                    match = time_key_pattern.search(obj['Key'])
                    if match:
                        # Parse the matched date string into a datetime object
                        date_str = match.group(1)
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                        partition_dates.append(date_obj)
                except Exception as e:
                    # Log any issues encountered while processing individual objects
                    logger.error(f"Error processing object {obj['Key']}: {e}")
        else:
            logger.warning(f"No objects found in the S3 prefix: {prefix}")
    
    except ClientError as e:
        if e.response['Error']['Code'] == 'S3.Client.exceptions.NoSuchBucket':
            logger.info(f"Bucket Name doesnt exists {bucket_name}.")
            raise e
        else:
            # Handle errors while accessing S3 (e.g., permissions issues)
            logger.error(f"Error listing objects in bucket {bucket_name}, prefix {prefix}: {e}")
    
    return sorted(set(partition_dates))  # Return unique and sorted partition dates




def delete_old_partitions(prefix):
    """
    Delete partitions from S3 that are older than 13 months.
    
    Args:
        prefix (str): S3 folder path prefix under which to delete old partitions.
    """
    try:
        # Retrieve all partition dates under the specified prefix
        partition_dates = get_partition_dates(prefix)
        
        # Check if there are more than 13 months worth of partitions
        if len(partition_dates) > 14:
            for date in partition_dates[:-14]:  # Keep only the last 13 months
                # Construct the partition prefix to match the partition date
                partition_prefix = f"{prefix}time_key={date.strftime('%Y-%m-%d')}/"
                
                try:
                    # List objects in the partition folder
                    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=partition_prefix)
                    
                    # Check if any objects exist in the partition
                    if 'Contents' in result:
                        for obj in result['Contents']:
                            try:
                                # Attempt to delete the object
                                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                                logger.info(f"Deleted partition: {partition_prefix}")
                            except ClientError as e:
                                # Log any issues encountered while deleting individual objects
                                logger.error(f"Error deleting object {obj['Key']}: {e}")
                    else:
                        logger.warning(f"No objects found in partition: {partition_prefix}")
              
                except ClientError as e:
                    if e.response['Error']['Code'] == 'S3.Client.exceptions.NoSuchBucket':
                        logger.info(f"Bucket Name doesnt exists {bucket_name}.")
                        raise e
                    else:
                        # Handle errors while accessing S3 (e.g., permissions issues)
                        logger.error(f"Error listing objects in bucket {bucket_name}, prefix {prefix}: {e}")
    
    except Exception as e:
        # Log any unexpected errors during partition deletion
        logger.error(f"Error deleting partitions for prefix {prefix}: {e}")




# Run the deletion check on both folders
for folder in folder_paths:
    try:
        # Call the delete_old_partitions function for each folder
        delete_old_partitions(folder)
    except Exception as e:
        # Log any errors encountered when processing each folder
        logger.error(f"Error processing folder {folder}: {e}")



job.commit()