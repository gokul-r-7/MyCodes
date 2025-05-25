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
from pyspark.storagelevel import StorageLevel
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
spark.catalog.clearCache()

# Initialize Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#SQL Queries
account_dim_sum_13 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           c.account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           c.account_guid,
           c.prim_account_holder_guid,
           c.test_account_key AS test_account_key,
           c.inception_dt
    FROM edw.customer_dim c
    WHERE test_account_key=2
),
revenue_data AS (
    SELECT distinct r.customer_key,
           r.site_id,
           r.dwelling_type_key,
           r.easy_pay_flag,
           CASE WHEN r.mobile_gross_mrc > 0 THEN '1' ELSE '0' END AS Mobile_Flag,
           r.customer_substatus_key,
           r.time_key
    FROM edw.customer_revenue_fact r
    WHERE r.bill_type_key != 2 
      AND to_date(r.time_key, 'yyyy-MM-dd') >= add_months(CURRENT_DATE, -13)
),
customer_status AS (
    SELECT distinct customer_substatus_key, sub_status_desc 
    FROM edw.customer_substatus_dim
),
dwelling_data AS (
    SELECT distinct d.dwelling_type_key,
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
           s.telephony_flag ,
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
account_details AS (
    SELECT distinct a.customer_key,
           ac.do_not_email AS Email_Opt_Out,
           ac.do_not_call AS Phone_Opt_Out,
           em.email_address,
           ROW_NUMBER() OVER (PARTITION BY a.customer_key ORDER BY ac.do_not_email) AS rn
    FROM edw.customer_dim a 
    LEFT JOIN camp_mgmt.accounts ac ON a.account_nbr = ac.account_nbr
    LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT distinct a.customer_key,
    MAX(CASE WHEN cmmeth = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Notification_Email_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' THEN 'Y' ELSE 'N' END) AS Notification_Phone_Flag,
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
                WHEN cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS phone_verified_date

FROM edw.customer_dim a 
LEFT JOIN pstage.stg_all_cust_notification_methods n ON a.account_nbr=n.cmcnbr
GROUP BY a.customer_key
),
web_contact AS (
    SELECT DISTINCT
        campaign,
        evar61_coxcust_guid,
        ROW_NUMBER() OVER (PARTITION BY evar61_coxcust_guid ORDER BY MIN(dt)) AS rn
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
    GROUP BY campaign, evar61_coxcust_guid
),
app_contact AS (
    SELECT DISTINCT
        coxcust_guid_v61,
        post_evar40,
        ROW_NUMBER() OVER (PARTITION BY coxcust_guid_v61 ORDER BY MIN(dt)) AS rn
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
    GROUP BY coxcust_guid_v61, post_evar40
),
ivr_contact AS (
    SELECT distinct 
        i.customer_key,
        DATE_FORMAT(CAST(i.time_key AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(i.time_key AS DATE)) AS Last_Contacted_Date_IVR_Call
    FROM `call`.call_ivr_fact i
    GROUP BY i.customer_key, DATE_FORMAT(CAST(i.time_key AS DATE), 'yyyy-MM')
),
web_data AS (
    SELECT distinct
        d.customer_key,
        DATE_FORMAT(CAST(d.dt AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(d.dt AS DATE)) AS Last_Contacted_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS DATE), 'yyyy-MM')
),
mob_data AS (
    SELECT distinct
        mob.customer_key,
        DATE_FORMAT(CAST(mob.dt AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(mob.dt AS DATE)) AS Last_Contacted_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS DATE), 'yyyy-MM')
)
SELECT distinct
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
    CAST(NULL AS varchar(255)) AS Sale_Acquisition_Channel,
    g.create_dt AS Registration_Date,
    COALESCE(web.campaign, app.post_evar40) AS Registration_Traffic_Source_Detail,
    CASE
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_cns_ocall_event255' THEN 'Email-Order'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_z_acct_onb%' THEN 'Email-Onboarding'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em%' THEN 'Email-Others'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_sms%' THEN 'sms'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_dm%' THEN 'direct mail'
        WHEN SUBSTRING(LOWER(COALESCE(web.campaign, app.post_evar40)), LENGTH(COALESCE(web.campaign, app.post_evar40)) - 5,6) = 'vanity' THEN 'Vanity URL'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE '%panoapp%' THEN 'panoapp'
        WHEN COALESCE(web.campaign, app.post_evar40) IS NOT NULL AND LENGTH(COALESCE(web.campaign, app.post_evar40)) > 0 THEN 'organic'
        ELSE 'null'  
    END AS Registration_Traffic_Source,
    s.data_flag AS Data_Flag,
    s.cable_flag AS TV_Flag,
    s.telephony_flag AS Phone_Flag,
    CAST(NULL AS varchar(255)) AS Homelife_Flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    COUNT(DISTINCT ad.email_address) AS email_count,
    nf.Notification_Email_Flag,
    nf.Notification_Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,
    CAST(NULL AS varchar(255)) AS Pano_Flag,
    CAST(NULL AS varchar(255)) AS Pano_Device,
    r.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    MAX(ivr.Last_Contacted_Date_IVR_Call) AS Last_Contacted_Date_IVR_Call,
    MAX(w.Last_Contacted_Date_Cox_com) AS Last_Contacted_Date_Cox_com,
    MAX(mob.Last_Contacted_Date_Cox_App) AS Last_Contacted_Date_Cox_App,
    CAST(NULL AS varchar(255)) AS Cox_Segment,
    CAST(NULL AS varchar(255)) AS Demographic_Info1,
    CAST(NULL AS varchar(255)) AS Demographic_Info2,
    r.time_key
FROM revenue_data r 
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
LEFT JOIN (SELECT * FROM account_summary WHERE rn=1) s ON r.customer_key = CAST(s.customer_key AS double)
LEFT JOIN customer_status cd ON r.customer_substatus_key = cd.customer_substatus_key 
LEFT JOIN customer_data c ON r.customer_key = CAST(c.customer_key AS double)
LEFT JOIN (SELECT * FROM account_details WHERE rn=1) ad ON r.customer_key = ad.customer_key
LEFT JOIN notification_flags nf ON r.customer_key = nf.customer_key
LEFT JOIN (SELECT * FROM guid_data WHERE rn=1) g ON r.customer_key = CAST(g.customer_key AS double)
LEFT JOIN (SELECT * FROM web_contact WHERE rn=1) web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN (SELECT * FROM app_contact WHERE rn=1) app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN ivr_contact ivr ON r.customer_key = CAST(ivr.customer_key AS double) AND ivr.Contact_Month <= r.time_key
LEFT JOIN web_data w ON r.customer_key = CAST(w.customer_key AS double) AND w.Contact_Month <= r.time_key
LEFT JOIN mob_data mob ON r.customer_key = CAST(mob.customer_key AS double) AND mob.Contact_Month <= r.time_key
GROUP BY
    r.customer_key,
    c.account_nbr,
    r.site_id,
    c.res_comm_ind,
    c.customer_status_cd,
    d.dwelling_type_desc,
    c.account_guid,
    c.prim_account_holder_guid,
    s.employee_flag,
    c.test_account_key,
    c.inception_dt,
    g.create_dt,
    COALESCE(web.campaign, app.post_evar40),
    s.data_flag,
    s.cable_flag,
    s.telephony_flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    nf.Notification_Email_Flag,
    nf.Notification_Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,
    r.easy_pay_flag,
    s.do_not_call_flag,
    s.do_not_email_flag,
    s.do_not_mail_flag,
    s.do_not_market_flag,
    r.time_key
"""

account_dim_sum_1 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           c.account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           c.account_guid,
           c.prim_account_holder_guid,
           c.test_account_key AS test_account_key,
           c.inception_dt
    FROM edw.customer_dim c
    WHERE test_account_key=2
),
revenue_data AS (
    SELECT distinct r.customer_key,
           r.site_id,
           r.dwelling_type_key,
           r.easy_pay_flag,
           CASE WHEN r.mobile_gross_mrc > 0 THEN '1' ELSE '0' END AS Mobile_Flag,
           r.customer_substatus_key,
           r.time_key
    FROM edw.customer_revenue_fact r
    WHERE r.bill_type_key != 2 
      AND to_date(r.time_key, 'yyyy-MM-dd') >= add_months(CURRENT_DATE, -1)
),
customer_status AS (
    SELECT distinct customer_substatus_key, sub_status_desc 
    FROM edw.customer_substatus_dim
),
dwelling_data AS (
    SELECT distinct d.dwelling_type_key,
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
           s.telephony_flag ,
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
account_details AS (
    SELECT distinct a.customer_key,
           ac.do_not_email AS Email_Opt_Out,
           ac.do_not_call AS Phone_Opt_Out,
           em.email_address,
           ROW_NUMBER() OVER (PARTITION BY a.customer_key ORDER BY ac.do_not_email) AS rn
    FROM edw.customer_dim a 
    LEFT JOIN camp_mgmt.accounts ac ON a.account_nbr = ac.account_nbr
    LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT distinct a.customer_key,
    MAX(CASE WHEN cmmeth = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Notification_Email_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' THEN 'Y' ELSE 'N' END) AS Notification_Phone_Flag,
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
                WHEN cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS phone_verified_date

FROM edw.customer_dim a 
LEFT JOIN pstage.stg_all_cust_notification_methods n ON a.account_nbr=n.cmcnbr
GROUP BY a.customer_key
),
web_contact AS (
    SELECT DISTINCT
        campaign,
        evar61_coxcust_guid,
        ROW_NUMBER() OVER (PARTITION BY evar61_coxcust_guid ORDER BY MIN(dt)) AS rn
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
    GROUP BY campaign, evar61_coxcust_guid
),
app_contact AS (
    SELECT DISTINCT
        coxcust_guid_v61,
        post_evar40,
        ROW_NUMBER() OVER (PARTITION BY coxcust_guid_v61 ORDER BY MIN(dt)) AS rn
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
    GROUP BY coxcust_guid_v61, post_evar40
),
ivr_contact AS (
    SELECT distinct 
        i.customer_key,
        DATE_FORMAT(CAST(i.time_key AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(i.time_key AS DATE)) AS Last_Contacted_Date_IVR_Call
    FROM `call`.call_ivr_fact i
    GROUP BY i.customer_key, DATE_FORMAT(CAST(i.time_key AS DATE), 'yyyy-MM')
),
web_data AS (
    SELECT distinct
        d.customer_key,
        DATE_FORMAT(CAST(d.dt AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(d.dt AS DATE)) AS Last_Contacted_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS DATE), 'yyyy-MM')
),
mob_data AS (
    SELECT distinct
        mob.customer_key,
        DATE_FORMAT(CAST(mob.dt AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(mob.dt AS DATE)) AS Last_Contacted_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS DATE), 'yyyy-MM')
)
SELECT distinct
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
    CAST(NULL AS varchar(255)) AS Sale_Acquisition_Channel,
    g.create_dt AS Registration_Date,
    COALESCE(web.campaign, app.post_evar40) AS Registration_Traffic_Source_Detail,
    CASE
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_cns_ocall_event255' THEN 'Email-Order'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_z_acct_onb%' THEN 'Email-Onboarding'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em%' THEN 'Email-Others'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_sms%' THEN 'sms'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_dm%' THEN 'direct mail'
        WHEN SUBSTRING(LOWER(COALESCE(web.campaign, app.post_evar40)), LENGTH(COALESCE(web.campaign, app.post_evar40)) - 5,6) = 'vanity' THEN 'Vanity URL'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE '%panoapp%' THEN 'panoapp'
        WHEN COALESCE(web.campaign, app.post_evar40) IS NOT NULL AND LENGTH(COALESCE(web.campaign, app.post_evar40)) > 0 THEN 'organic'
        ELSE 'null'  
    END AS Registration_Traffic_Source,
    s.data_flag AS Data_Flag,
    s.cable_flag AS TV_Flag,
    s.telephony_flag AS Phone_Flag,
    CAST(NULL AS varchar(255)) AS Homelife_Flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    COUNT(DISTINCT ad.email_address) AS email_count,
    nf.Notification_Email_Flag,
    nf.Notification_Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,
    CAST(NULL AS varchar(255)) AS Pano_Flag,
    CAST(NULL AS varchar(255)) AS Pano_Device,
    r.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    MAX(ivr.Last_Contacted_Date_IVR_Call) AS Last_Contacted_Date_IVR_Call,
    MAX(w.Last_Contacted_Date_Cox_com) AS Last_Contacted_Date_Cox_com,
    MAX(mob.Last_Contacted_Date_Cox_App) AS Last_Contacted_Date_Cox_App,
    CAST(NULL AS varchar(255)) AS Cox_Segment,
    CAST(NULL AS varchar(255)) AS Demographic_Info1,
    CAST(NULL AS varchar(255)) AS Demographic_Info2,
    r.time_key
FROM revenue_data r 
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
LEFT JOIN (SELECT * FROM account_summary WHERE rn=1) s ON r.customer_key = CAST(s.customer_key AS double)
LEFT JOIN customer_status cd ON r.customer_substatus_key = cd.customer_substatus_key 
LEFT JOIN customer_data c ON r.customer_key = CAST(c.customer_key AS double)
LEFT JOIN (SELECT * FROM account_details WHERE rn=1) ad ON r.customer_key = ad.customer_key
LEFT JOIN notification_flags nf ON r.customer_key = nf.customer_key
LEFT JOIN (SELECT * FROM guid_data WHERE rn=1) g ON r.customer_key = CAST(g.customer_key AS double)
LEFT JOIN (SELECT * FROM web_contact WHERE rn=1) web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN (SELECT * FROM app_contact WHERE rn=1) app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN ivr_contact ivr ON r.customer_key = CAST(ivr.customer_key AS double) AND ivr.Contact_Month <= r.time_key
LEFT JOIN web_data w ON r.customer_key = CAST(w.customer_key AS double) AND w.Contact_Month <= r.time_key
LEFT JOIN mob_data mob ON r.customer_key = CAST(mob.customer_key AS double) AND mob.Contact_Month <= r.time_key
GROUP BY
    r.customer_key,
    c.account_nbr,
    r.site_id,
    c.res_comm_ind,
    c.customer_status_cd,
    d.dwelling_type_desc,
    c.account_guid,
    c.prim_account_holder_guid,
    s.employee_flag,
    c.test_account_key,
    c.inception_dt,
    g.create_dt,
    COALESCE(web.campaign, app.post_evar40),
    s.data_flag,
    s.cable_flag,
    s.telephony_flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    nf.Notification_Email_Flag,
    nf.Notification_Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,
    r.easy_pay_flag,
    s.do_not_call_flag,
    s.do_not_email_flag,
    s.do_not_mail_flag,
    s.do_not_market_flag,
    r.time_key
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
        WHERE test_account_key=2
),
revenue_data AS (
    SELECT r.customer_key,
           r.site_id,
           MAX(last_logged_in_date_okta) AS last_logged_in_date_okta,
           r.time_key
    FROM edw.customer_revenue_fact r 
    LEFT JOIN (
        SELECT DISTINCT
            a.Customer_Key,
            a.household_member_guid AS User_GUID,
            DATE_FORMAT(CAST(o.event_date AS DATE), 'yyyy-MM') AS Contact_Month,
            MAX(o.event_date) AS last_logged_in_date_okta
         FROM
            edw.customer_guid_dtl_dim a 
            LEFT JOIN ciam.successful_authentications_okta o 
            ON a.user_id = o.actor_alternateid
            GROUP BY
            a.Customer_Key, a.household_member_guid, DATE_FORMAT(CAST(o.event_date AS DATE), 'yyyy-MM')
    ) o 
    ON r.customer_key = o.customer_key 
    AND o.Contact_Month <= r.time_key
    WHERE r.bill_type_key != 2 
    AND to_date(r.time_key, 'yyyy-MM-dd') >= add_months(CURRENT_DATE, -13)
    GROUP BY r.customer_key,
             r.site_id,
             r.time_key
),
guid_data AS (
    SELECT 
        a.customer_key,
        a.create_dt AS Registration_date,
        a.user_id,
        a.prim_customer_flag AS Primary_Flag,
        a.household_member_guid AS User_GUID
    FROM
        edw.customer_guid_dtl_dim a
),
tsv_data AS (
    SELECT 
        a.customer_key,
        a.household_member_guid AS User_GUID,
        CASE WHEN (t.`household member guid`) IS NOT NULL THEN 'group.user_membership.add'
             ELSE NULL
        END AS TSV_Enrolled_Status
    FROM
        edw.customer_guid_dtl_dim a 
    INNER JOIN ciam_datamodel.tsv_guid_data t 
        ON cast(a.household_member_guid AS varchar(255)) = cast(t.`household member guid` AS varchar(255))
),
mfa_data AS (
    SELECT DISTINCT
        d.username,
        MAX(CASE
            WHEN d.outcome_reason LIKE '%EMAIL%'
                 AND d.event_date = sub.latest_event_date THEN 1
            ELSE 0
        END) AS TSV_EMAIL_Flag,
        MAX(CASE
            WHEN d.outcome_reason LIKE '%CALL%'
                 AND d.event_date = sub.latest_event_date THEN 1
            ELSE 0
        END) AS TSV_CALL_Flag,
        MAX(CASE
            WHEN d.outcome_reason LIKE '%SMS%'
                 AND d.event_date = sub.latest_event_date THEN 1
            ELSE 0
        END) AS TSV_SMS_Flag
    FROM
        ciam.mfa_factors_enrolled d
    LEFT JOIN (
        SELECT
            username AS user_id,
            outcome_reason,
            MAX(event_date) AS latest_event_date
        FROM
            ciam.mfa_factors_enrolled
        WHERE
            outcome_result = 'SUCCESS'
        GROUP BY
            username, outcome_reason
    ) sub
    ON d.username = sub.user_id AND d.outcome_reason = sub.outcome_reason
    WHERE
        d.outcome_result = 'SUCCESS'
    GROUP BY d.username
),
mob_data AS (
    SELECT
        mob.customer_key,
        mob.post_mobileappid,
        CASE
            WHEN mob.post_mobileappid LIKE 'CoxAccount%' THEN 'iOS'
            WHEN mob.post_mobileappid LIKE 'Cox %' THEN 'Android'
            ELSE 'Null'
        END AS Last_Logged_In_OS_Cox_App
    FROM
        mobile_data_temp.app_contact_history mob
)
SELECT DISTINCT a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    r.Customer_Key,
    b.Account_Nbr,
    r.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    CAST(NULL AS varchar(255)) AS User_Permission,
    CAST(NULL AS varchar(255)) AS Preferred_Contact_Method,
    CAST(NULL AS varchar(255)) AS Placeholder2,
    t.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    CAST(NULL AS varchar(255)) AS Preference_Placeholder1,
    CAST(NULL AS varchar(255)) AS Preference_Placeholder2,
    CAST(NULL AS varchar(255)) AS Preferences_Last_Used_Date,
    MAX(r.last_logged_in_date_okta) AS Last_logged_in_date_okta,
    MAX(mob.Last_Logged_In_OS_Cox_App) AS Last_Logged_In_OS_Cox_App,
    CAST(NULL AS varchar(255)) AS Last_Password_Change_Date,
    r.time_key
FROM revenue_data r 
LEFT JOIN customer_dim b ON r.customer_key = b.customer_key
LEFT JOIN guid_data a ON r.customer_key = a.customer_key
LEFT JOIN tsv_data t ON r.customer_key = t.customer_key
LEFT JOIN mfa_data mfa ON a.user_id = mfa.username
LEFT JOIN mob_data mob ON r.customer_key = CAST(mob.customer_key AS double)
GROUP BY a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    r.Customer_Key,
    b.Account_Nbr,
    r.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    t.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    r.time_key
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
        WHERE test_account_key=2
),
revenue_data AS (
    SELECT r.customer_key,
           r.site_id,
           MAX(last_logged_in_date_okta) AS last_logged_in_date_okta,
           r.time_key
    FROM edw.customer_revenue_fact r 
    LEFT JOIN (
        SELECT DISTINCT
            a.Customer_Key,
            a.household_member_guid AS User_GUID,
            DATE_FORMAT(CAST(o.event_date AS DATE), 'yyyy-MM') AS Contact_Month,
            MAX(o.event_date) AS last_logged_in_date_okta
         FROM
            edw.customer_guid_dtl_dim a 
            LEFT JOIN ciam.successful_authentications_okta o 
            ON a.user_id = o.actor_alternateid
            GROUP BY
            a.Customer_Key, a.household_member_guid, DATE_FORMAT(CAST(o.event_date AS DATE), 'yyyy-MM')
    ) o 
    ON r.customer_key = o.customer_key 
    AND o.Contact_Month <= r.time_key
    WHERE r.bill_type_key != 2 
    AND to_date(r.time_key, 'yyyy-MM-dd') >= add_months(CURRENT_DATE, -1)
    GROUP BY r.customer_key,
             r.site_id,
             r.time_key
),
guid_data AS (
    SELECT 
        a.customer_key,
        a.create_dt AS Registration_date,
        a.user_id,
        a.prim_customer_flag AS Primary_Flag,
        a.household_member_guid AS User_GUID
    FROM
        edw.customer_guid_dtl_dim a
),
tsv_data AS (
    SELECT 
        a.customer_key,
        a.household_member_guid AS User_GUID,
        CASE WHEN (t.`household member guid`) IS NOT NULL THEN 'group.user_membership.add'
             ELSE NULL
        END AS TSV_Enrolled_Status
    FROM
        edw.customer_guid_dtl_dim a 
    INNER JOIN ciam_datamodel.tsv_guid_data t 
        ON cast(a.household_member_guid AS varchar(255)) = cast(t.`household member guid` AS varchar(255))
),
mfa_data AS (
    SELECT DISTINCT
        d.username,
        MAX(CASE
            WHEN d.outcome_reason LIKE '%EMAIL%'
                 AND d.event_date = sub.latest_event_date THEN 1
            ELSE 0
        END) AS TSV_EMAIL_Flag,
        MAX(CASE
            WHEN d.outcome_reason LIKE '%CALL%'
                 AND d.event_date = sub.latest_event_date THEN 1
            ELSE 0
        END) AS TSV_CALL_Flag,
        MAX(CASE
            WHEN d.outcome_reason LIKE '%SMS%'
                 AND d.event_date = sub.latest_event_date THEN 1
            ELSE 0
        END) AS TSV_SMS_Flag
    FROM
        ciam.mfa_factors_enrolled d
    LEFT JOIN (
        SELECT
            username AS user_id,
            outcome_reason,
            MAX(event_date) AS latest_event_date
        FROM
            ciam.mfa_factors_enrolled
        WHERE
            outcome_result = 'SUCCESS'
        GROUP BY
            username, outcome_reason
    ) sub
    ON d.username = sub.user_id AND d.outcome_reason = sub.outcome_reason
    WHERE
        d.outcome_result = 'SUCCESS'
    GROUP BY d.username
),
mob_data AS (
    SELECT
        mob.customer_key,
        mob.post_mobileappid,
        CASE
            WHEN mob.post_mobileappid LIKE 'CoxAccount%' THEN 'iOS'
            WHEN mob.post_mobileappid LIKE 'Cox %' THEN 'Android'
            ELSE 'Null'
        END AS Last_Logged_In_OS_Cox_App
    FROM
        mobile_data_temp.app_contact_history mob
)
SELECT DISTINCT a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    r.Customer_Key,
    b.Account_Nbr,
    r.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    CAST(NULL AS varchar(255)) AS User_Permission,
    CAST(NULL AS varchar(255)) AS Preferred_Contact_Method,
    CAST(NULL AS varchar(255)) AS Placeholder2,
    t.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    CAST(NULL AS varchar(255)) AS Preference_Placeholder1,
    CAST(NULL AS varchar(255)) AS Preference_Placeholder2,
    CAST(NULL AS varchar(255)) AS Preferences_Last_Used_Date,
    MAX(r.last_logged_in_date_okta) AS Last_logged_in_date_okta,
    MAX(mob.Last_Logged_In_OS_Cox_App) AS Last_Logged_In_OS_Cox_App,
    CAST(NULL AS varchar(255)) AS Last_Password_Change_Date,
    r.time_key
FROM revenue_data r 
LEFT JOIN customer_dim b ON r.customer_key = b.customer_key
LEFT JOIN guid_data a ON r.customer_key = a.customer_key
LEFT JOIN tsv_data t ON r.customer_key = t.customer_key
LEFT JOIN mfa_data mfa ON a.user_id = mfa.username
LEFT JOIN mob_data mob ON r.customer_key = CAST(mob.customer_key AS double)
GROUP BY a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    r.Customer_Key,
    b.Account_Nbr,
    r.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    t.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    r.time_key
"""

transaction_adobe_fact = """
WITH web_contact_history AS (
    SELECT
        customer_key,
        evar61_coxcust_guid AS User_guid,
        evar75_marketing_cloud_id AS Adobe_ECID,
        visits AS Adobe_Visit_Id,
        date_time AS Activity_Date,
        pagename AS Activity_Page,
        prop10_custom_links as p10,
        prop15_previous_page as previous_page,
        mvvar3 AS Server_Error,
        evar46_page_name as activity_pagename,
        CAST(NULL AS varchar(255)) AS Client_Error,
        campaign AS Traffic_Source_Detail_sc_id
    FROM webanalytics.web_contact_history
    WHERE to_date(SUBSTR(CAST(dt AS STRING), 1, 10), 'yyyy-MM-dd') >= DATE_ADD(CURRENT_DATE, -30)
    AND (pagename LIKE 'cox:res:myprofile%' OR evar46_page_name LIKE 'cox:res:myprofile%')
),
app_contact_history AS (
    SELECT
        cast(customer_key as bigint) as customer_key,
        coxcust_guid_v61 AS User_guid,
        CAST(NULL AS varchar(255)) AS Adobe_ECID,
        visits AS Adobe_Visit_Id,
        date_time AS Activity_Date,
        pagename AS Activity_Page,
        custom_link_clicks_p10 as p10,
        previous_page_p15 as previous_page,
        server_form_error_p13 AS Server_Error,
        page_name_v46 as activity_pagename,
        CAST(NULL AS varchar(255)) AS Client_Error,
        post_evar40 AS Traffic_Source_Detail_sc_id
    FROM mobile_data_temp.app_contact_history
    WHERE to_date(SUBSTR(CAST(dt AS STRING), 1, 10), 'yyyy-MM-dd') >= DATE_ADD(CURRENT_DATE, -30)
    AND ((pagename LIKE 'coxapp:reg:%' OR pagename LIKE 'coxapp:myaccount%') OR (page_name_v46 LIKE 'coxapp:reg:%' OR page_name_v46 LIKE 'coxapp:myaccount%'))
),
union_cte AS (
    SELECT * FROM web_contact_history W
    UNION ALL 
    SELECT * FROM app_contact_history a
)
SELECT DISTINCT 
    u.customer_key,
    u.User_guid,
    u.Adobe_ECID,
    u.Adobe_Visit_Id,
    u.Activity_Date,
    a2.create_dt as Registration_Date,
    b.inception_dt as Inception_date,
    CASE 
        WHEN u.Activity_Page LIKE '%:communication%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:contacts%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:disability%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:confirmation%' THEN 'Email Verification'
        WHEN u.Activity_Page LIKE '%:forgot-password:%' THEN 'Forgot Password'
        WHEN u.Activity_Page LIKE '%:forgot-userid:%' THEN 'Forgot UserID'
        WHEN u.Activity_Page LIKE '%:forgotuserid:%' THEN 'Forgot UserID'
        WHEN u.Activity_Page LIKE '%:fuid:%' THEN 'Forgot UserID'
        WHEN u.Activity_Page LIKE '%:home%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:mailing-address%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:mailingaddress%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:manageusers%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:notifications%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:number-lock-protection%' THEN 'Number Lock Protection'
        WHEN u.Activity_Page LIKE '%:password%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:privacy%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:reg:%' THEN 'Registration'
        WHEN u.Activity_Page LIKE '%:security%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:updateprofile%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:syncupaccount%' THEN 'Sync Account'
        WHEN u.Activity_Page LIKE '%:tsv' THEN 'TSV Enrollment'
        WHEN u.Activity_Page LIKE '%:tsv:email:%' THEN 'TSV Verification'
        WHEN u.Activity_Page LIKE '%:tsv:call:%' THEN 'TSV Verification'
        WHEN u.Activity_Page LIKE '%:tsv:text:%' THEN 'TSV Verification'
        WHEN u.Activity_Page LIKE '%:tsv:reset:%' THEN 'TSV Reset'
        WHEN u.Activity_Page LIKE '%:verify-identity:%' THEN 'Verify Contact'
        ELSE 'Unknown'        
    END AS Activity_Name,
    CASE
        WHEN u.Activity_Page LIKE '%:email' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%phone' then 'Attempt'  
        WHEN u.Activity_Page LIKE '%:phone:call' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%:phone:text' then 'Attempt'
        WHEN u.Activity_Page LIKE '%lookup-account' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%lookup-email' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%lookup-phone' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%lookup-address' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%userid-exists' then 'Already Registered'
        WHEN u.Activity_Page LIKE '%multi-address%' then 'Multiple Address' 
        WHEN u.Activity_Page LIKE '%:notifications' then 'Prefrence Center'
        WHEN u.Activity_Page LIKE '%:communication' then 'Prefrence Center'
        WHEN u.Activity_Page LIKE '%:secret-question' then 'Secret Question'
        WHEN u.Activity_Page LIKE '%:sendcode' then 'Send Code'
        WHEN u.Activity_Page LIKE 'cox:res:myprofile:reg:userid-password' then 'Setup Credentials'
        WHEN u.Activity_Page LIKE '%:landing' then 'Start page' 
        WHEN u.Activity_Page LIKE '%:confirmation' then 'Success' 
        WHEN u.Activity_Page LIKE '%:lookup-account-success' then 'Success'
        WHEN u.Activity_Page LIKE '%:lookup-address-success' then 'Success'
        WHEN u.Activity_Page LIKE '%:success' then 'Success' 
        WHEN u.Activity_Page LIKE '%:verification%' then 'Verify Code' 
        WHEN u.Activity_Page LIKE '%:verifycode' then 'Verify Code' 
        ELSE 'Unknown'
    END AS Navigation_step,
    CASE 
        WHEN u.activity_pagename LIKE '%:communication%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:contacts%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:disability%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:confirmation%' THEN 'Email Verification'
        WHEN u.activity_pagename LIKE '%:forgot-password:%' THEN 'Forgot Password'
        WHEN u.activity_pagename LIKE '%:forgot-userid:%' THEN 'Forgot UserID'
        WHEN u.activity_pagename LIKE '%:forgotuserid:%' THEN 'Forgot UserID'
        WHEN u.activity_pagename LIKE '%:fuid:%' THEN 'Forgot UserID'
        WHEN u.activity_pagename LIKE '%:home%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:mailing-address%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:mailingaddress%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:manageusers%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:notifications%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:number-lock-protection%' THEN 'Number Lock Protection'
        WHEN u.activity_pagename LIKE '%:password%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:privacy%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:reg:%' THEN 'Registration'
        WHEN u.activity_pagename LIKE '%:security%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:updateprofile%' THEN 'Manage Profile'
        WHEN u.activity_pagename LIKE '%:syncupaccount%' THEN 'Sync Account'
        WHEN u.activity_pagename LIKE '%:tsv' THEN 'TSV Enrollment'
        WHEN u.activity_pagename LIKE '%:tsv:email:%' THEN 'TSV Verification'
        WHEN u.activity_pagename LIKE '%:tsv:call:%' THEN 'TSV Verification'
        WHEN u.activity_pagename LIKE '%:tsv:text:%' THEN 'TSV Verification'
        WHEN u.activity_pagename LIKE '%:tsv:reset:%' THEN 'TSV Reset'
        WHEN u.activity_pagename LIKE '%:verify-identity:%' THEN 'Verify Contact'
        ELSE 'Unknown'        
    END AS methods,
    CASE
        WHEN u.activity_pagename LIKE '%:email' then 'Attempt' 
        WHEN u.activity_pagename LIKE '%phone' then 'Attempt'  
        WHEN u.activity_pagename LIKE '%:phone:call' then 'Attempt' 
        WHEN u.activity_pagename LIKE '%:phone:text' then 'Attempt'
        WHEN u.activity_pagename LIKE '%lookup-account' then 'Attempt' 
        WHEN u.activity_pagename LIKE '%lookup-email' then 'Attempt' 
        WHEN u.activity_pagename LIKE '%lookup-phone' then 'Attempt' 
        WHEN u.activity_pagename LIKE '%lookup-address' then 'Attempt' 
        WHEN u.activity_pagename LIKE '%userid-exists' then 'Already Registered'
        WHEN u.activity_pagename LIKE '%multi-address%' then 'Multiple Address' 
        WHEN u.activity_pagename LIKE '%:notifications' then 'Prefrence Center'
        WHEN u.activity_pagename LIKE '%:communication' then 'Prefrence Center'
        WHEN u.activity_pagename LIKE '%:secret-question' then 'Secret Question'
        WHEN u.activity_pagename LIKE '%:sendcode' then 'Send Code'
        WHEN u.activity_pagename LIKE 'cox:res:myprofile:reg:userid-password' then 'Setup Credentials'
        WHEN u.activity_pagename LIKE '%:landing' then 'Start page' 
        WHEN u.activity_pagename LIKE '%:confirmation' then 'Success' 
        WHEN u.activity_pagename LIKE '%:lookup-account-success' then 'Success'
        WHEN u.activity_pagename LIKE '%:lookup-address-success' then 'Success'
        WHEN u.activity_pagename LIKE '%:success' then 'Success' 
        WHEN u.activity_pagename LIKE '%:verification%' then 'Verify Code' 
        WHEN u.activity_pagename LIKE '%:verifycode' then 'Verify Code' 
        ELSE 'Unknown'
    END AS Feature,
    u.Activity_Page,
    u.p10,
    u.previous_page,
    u.activity_pagename,
    u.Server_Error,
    u.Client_Error,
    u.Traffic_Source_Detail_sc_id
FROM union_cte u
LEFT JOIN edw.customer_guid_dtl_dim a2 ON u.User_guid = a2.household_member_guid
LEFT JOIN edw.customer_dim b ON a2.customer_key = b.customer_key
GROUP BY u.customer_key,
    u.User_guid,
    u.Adobe_ECID,
    u.Adobe_Visit_Id,
    u.Activity_Date,
    u.Activity_Page,
    a2.create_dt,
    b.inception_dt,
    u.p10,
    u.previous_page,
    u.activity_pagename,
    u.Server_Error,
    u.Client_Error,
    u.Traffic_Source_Detail_sc_id,
    CASE 
        WHEN u.Activity_Page LIKE '%:communication%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:contacts%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:disability%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:confirmation%' THEN 'Email Verification'
        WHEN u.Activity_Page LIKE '%:forgot-password:%' THEN 'Forgot Password'
        WHEN u.Activity_Page LIKE '%:forgot-userid:%' THEN 'Forgot UserID'
        WHEN u.Activity_Page LIKE '%:forgotuserid:%' THEN 'Forgot UserID'
        WHEN u.Activity_Page LIKE '%:fuid:%' THEN 'Forgot UserID'
        WHEN u.Activity_Page LIKE '%:home%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:mailing-address%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:mailingaddress%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:manageusers%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:notifications%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:number-lock-protection%' THEN 'Number Lock Protection'
        WHEN u.Activity_Page LIKE '%:password%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:privacy%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:reg:%' THEN 'Registration'
        WHEN u.Activity_Page LIKE '%:security%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:updateprofile%' THEN 'Manage Profile'
        WHEN u.Activity_Page LIKE '%:syncupaccount%' THEN 'Sync Account'
        WHEN u.Activity_Page LIKE '%:tsv' THEN 'TSV Enrollment'
        WHEN u.Activity_Page LIKE '%:tsv:email:%' THEN 'TSV Verification'
        WHEN u.Activity_Page LIKE '%:tsv:call:%' THEN 'TSV Verification'
        WHEN u.Activity_Page LIKE '%:tsv:text:%' THEN 'TSV Verification'
        WHEN u.Activity_Page LIKE '%:tsv:reset:%' THEN 'TSV Reset'
        WHEN u.Activity_Page LIKE '%:verify-identity:%' THEN 'Verify Contact'
        ELSE 'Unknown'        
    END,
    CASE
        WHEN u.Activity_Page LIKE '%:email' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%phone' then 'Attempt'  
        WHEN u.Activity_Page LIKE '%:phone:call' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%:phone:text' then 'Attempt'
        WHEN u.Activity_Page LIKE '%lookup-account' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%lookup-email' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%lookup-phone' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%lookup-address' then 'Attempt' 
        WHEN u.Activity_Page LIKE '%userid-exists' then 'Already Registered'
        WHEN u.Activity_Page LIKE '%multi-address%' then 'Multiple Address' 
        WHEN u.Activity_Page LIKE '%:notifications' then 'Prefrence Center'
        WHEN u.Activity_Page LIKE '%:communication' then 'Prefrence Center'
        WHEN u.Activity_Page LIKE '%:secret-question' then 'Secret Question'
        WHEN u.Activity_Page LIKE '%:sendcode' then 'Send Code'
        WHEN u.Activity_Page LIKE 'cox:res:myprofile:reg:userid-password' then 'Setup Credentials'
        WHEN u.Activity_Page LIKE '%:landing' then 'Start page' 
        WHEN u.Activity_Page LIKE '%:confirmation' then 'Success' 
        WHEN u.Activity_Page LIKE '%:lookup-account-success' then 'Success'
        WHEN u.Activity_Page LIKE '%:lookup-address-success' then 'Success'
        WHEN u.Activity_Page LIKE '%:success' then 'Success' 
        WHEN u.Activity_Page LIKE '%:verification%' then 'Verify Code' 
        WHEN u.Activity_Page LIKE '%:verifycode' then 'Verify Code' 
        ELSE 'Unknown'
    END
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
    cast(NULL as varchar(255)) AS Authentication_Error,
    'Login Credentials' AS authentication_method,
    CASE
        WHEN m.eventtype = 'group.user_membership.add' THEN 'tsv enrolled'
        ELSE NULL
    END AS activity_type
FROM
    aggregated_auth a
LEFT JOIN edw.customer_guid_dtl_dim g ON a.user_id = g.user_id
LEFT JOIN aggregated_signon o ON o.user_id = g.user_id
LEFT JOIN ciam.mfa_total_mfa_users m ON m.username = g.user_id
GROUP BY 
    g.household_member_guid,
    a.host,
    o.application,
    a.Authentication_Attempt,
    a.authentication_success_result,
    CASE
        WHEN m.eventtype = 'group.user_membership.add' THEN 'tsv enrolled'
        ELSE NULL
    END
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
        CAST(event_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR  -- Fixed: Subtract 1 year from current date
),
filtered_signon AS (
    SELECT
        username AS user_id,
        application,
        CAST(event_date AS DATE) AS event_date
    FROM
        ciam.single_signon_okta
    WHERE
        CAST(event_date AS DATE) >= CURRENT_DATE - INTERVAL 1 YEAR  -- Fixed: Subtract 1 year from current date
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
    cast(NULL as varchar(255)) AS Authentication_Error,  -- Fixed: Specify length for varchar
    'Login Credentials' AS Authentication_method,
    CASE
        WHEN m.eventtype = 'group.user_membership.add' THEN 'tsv enrolled'
        ELSE NULL
    END AS Activity_type
FROM
    aggregated_auth a LEFT JOIN edw.customer_guid_dtl_dim g ON a.user_id = g.user_id
LEFT JOIN
    aggregated_signon o ON g.user_id = o.user_id AND a.event_date = o.event_date
LEFT JOIN
    ciam.mfa_total_mfa_users m ON g.user_id = m.username
GROUP BY 
    a.event_date,
    a.host,
    o.application,
    a.Authentication_Attempt,
    a.authentication_success_result,
    CASE
        WHEN m.eventtype = 'group.user_membership.add' THEN 'tsv enrolled'
        ELSE NULL
    END
"""


#Output s3 path
account_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_prod/account_dim_sum/"
profile_dim_sum_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_prod/profile_dim_sum/"
transaction_adobe_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_prod/transaction_adobe_fact/"
transaction_okta_user_agg_fact_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_prod/transaction_okta_user_agg_fact/"
transcation_okta_day_agg_output = "s3://cci-dig-aicoe-data-sb/processed/ciam_prod/transcation_okta_day_agg_fact/"


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
job_log_database_name = "ciam_prod"
job_log_table_name = "job_log_table"
job_log_table_path = "s3://cci-dig-aicoe-data-sb/processed/ciam_prod/job_log_table/"




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
        # Repartition the DataFrame by the given partition columns
        df = df.repartitionByRange(1, *partitionkey)
        
        # Write the DataFrame to S3 in Parquet format with partitioning and compression
        write_df = df.write \
            .partitionBy(*partitionkey) \
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

    
    #account_dim_sum_df.cache()
    #profile_dim_sum_df.cache()
    #transaction_adobe_fact_df.cache()
    #transaction_okta_user_agg_fact_df.cache()
    #transcation_okta_day_agg_df.cache()
    
    account_dim_sum_df.persist(StorageLevel.MEMORY_AND_DISK)
    profile_dim_sum_df.persist(StorageLevel.MEMORY_AND_DISK)
    transaction_adobe_fact_df.persist(StorageLevel.MEMORY_AND_DISK)
    transaction_okta_user_agg_fact_df.persist(StorageLevel.MEMORY_AND_DISK)
    transcation_okta_day_agg_df.persist(StorageLevel.MEMORY_AND_DISK)


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
    
    
    #account_dim_sum_df.cache()
    #profile_dim_sum_df.cache()
    #transaction_adobe_fact_df.cache()
    #transaction_okta_user_agg_fact_df.cache()
    #transcation_okta_day_agg_df.cache()


    account_dim_sum_df.persist(StorageLevel.MEMORY_AND_DISK)
    profile_dim_sum_df.persist(StorageLevel.MEMORY_AND_DISK)
    transaction_adobe_fact_df.persist(StorageLevel.MEMORY_AND_DISK)
    transaction_okta_user_agg_fact_df.persist(StorageLevel.MEMORY_AND_DISK)
    transcation_okta_day_agg_df.persist(StorageLevel.MEMORY_AND_DISK)
    

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
folder_paths = ["processed/ciam_prod/account_dim_sum/", "processed/ciam_prod/profile_dim_sum/"]



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
