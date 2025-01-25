
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
 
glueContext._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
spark._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
 
spark = glueContext.spark_session
job = Job(glueContext)
#spark.sparkContext.getConf().getAll()
#for item in spark.sparkContext.getConf().getAll():
 #   print(item)
day_agg = """
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
day_agg_df = spark.sql(day_agg)
day_agg_df.show()
day_agg_df.cache()
day_agg_df.printSchema()
day_agg_df.count()
user_agg = """
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
user_agg_df = spark.sql(user_agg)
user_agg_df.show()
user_agg_df.cache()
user_agg_df.printSchema()
user_agg_df.count()
adobe_fact = """
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
adobe_fact_df = spark.sql(adobe_fact)
adobe_fact_df.show()
adobe_fact_df.cache()
adobe_fact_df.printSchema()
adobe_fact_df.count()
account_dim = """
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
),
revenue_data AS (
    SELECT r.customer_key,
           r.dwelling_type_key,
           MAX(r.time_key) AS MAX_Time_Key
    FROM edw.customer_revenue_fact r
    GROUP BY r.customer_key, r.dwelling_type_key
),
dwelling_data AS (
    SELECT DISTINCT d.dwelling_type_key,
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
           s.telephony_flag,
           s.do_not_market_flag,
           s.time_key
    FROM edw.cust_acct_sum s
    WHERE
    TO_DATE(s.time_key, 'yyyy-MM-dd') >= DATE_ADD(CURRENT_DATE, -1)  -- Replace DATE_PARSE with TO_DATE
),
guid_data AS (
    SELECT g.customer_key,
           g.create_dt,
           g.household_member_guid,
           ROW_NUMBER() OVER (PARTITION BY g.customer_key ORDER BY g.create_dt ASC) AS rn
    FROM edw.customer_guid_dtl_dim g
),
ivr_contact AS (
    SELECT DISTINCT i.customer_key,
           MAX(i.time_key) AS Last_Contacted_Date_IVR_Call
    FROM `call`.`call_ivr_fact` i
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
        WHERE pagename = 'cox:res:myprofile:reg:confirmation'
    )
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
        WHERE pagename = 'coxapp:reg:confirmation'
    )
),
mob_data AS (
    SELECT mob.customer_key, MAX(mob.dt) AS Last_Contacted_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key
)
SELECT DISTINCT
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
    CAST(NULL AS VARCHAR(255)) AS Sale_Acquisition_Channel,
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
    s.telephony_flag AS Phone_Flag,  
    CAST(NULL AS VARCHAR(255)) AS Homelife_Flag,
    s.wireless_flag AS Mobile_Flag,
    CAST(NULL AS VARCHAR(255)) AS Pano_Flag,
    CAST(NULL AS VARCHAR(255)) AS Pano_Device,
    s.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    ivr.Last_Contacted_Date_IVR_Call,
    d.Last_Contacted_Date_Cox_com,
    mob.Last_Contacted_Date_Cox_App, 
    CAST(NULL AS VARCHAR(255)) AS Cox_Segment,
    CAST(NULL AS VARCHAR(255)) AS Demographic_Info1,
    CAST(NULL AS VARCHAR(255)) AS Demographic_Info2,
    s.time_key AS Account_Summary_Time_Key,  -- Explicitly alias the time_key here
    r.MAX_Time_Key AS Revenue_Data_Time_Key  -- Explicitly alias MAX time_key from revenue_data
FROM account_summary s 
LEFT JOIN customer_data c ON CAST(s.customer_key AS BIGINT) = c.customer_key 
LEFT JOIN revenue_data r ON s.customer_key = CAST(r.customer_key AS BIGINT)
LEFT JOIN (SELECT * FROM guid_data WHERE rn = 1) g ON s.customer_key = CAST(g.customer_key AS BIGINT)
LEFT JOIN ivr_contact ivr ON s.customer_key = CAST(ivr.customer_key AS BIGINT)
LEFT JOIN web_data d ON s.customer_key = CAST(d.customer_key AS BIGINT)
LEFT JOIN mob_data mob ON s.customer_key = CAST(mob.customer_key AS BIGINT)
LEFT JOIN web_contact web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN app_contact app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
LIMIT 10;
"""
account_dim_df = spark.sql(account_dim)
account_dim_df.show()
account_dim_df.cache()
account_dim_df.printSchema()
account_dim_df.count()
job.commit()