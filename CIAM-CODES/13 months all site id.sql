****************************Account Dim Query***********************************************
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

****************************Profile Dim Query*********************************************************
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


***********************************************************************************************************8888
TRANSACTION_ADOBE_FACT

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
    
*****************************************************************************************************************
TRANSACTION_OKTA_FACT: USER_AGGREGATED

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
    ciam.mfa_total_mfa_users m ON g.user_id = m.username; 
*************************************
TRANSACTION_OKTA_FACT: DAY_AGGREGATED

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

==========================================================
Profile_metric_view

CREATE VIEW profile_metric_view AS
SELECT
    a.customer_key,
    a.account_nbr,
    a.site_id,
    a.res_com_ind, 
    a.customer_status,
    a.house_type,
    a.account_guid,
    a.user_guid_primary,
    a.employee_flag,
    a.test_account_flag,
    a.inception_date ,
    a.sale_acquisition_channel,
    a.registration_date,
    a.registration_traffic_source_detail,
    a.registration_traffic_source,
    a.data_flag,
    a.tv_flag,
    a.phone_flag,
    a.homelife_flag,
    a.mobile_flag,
    a.pano_flag,
    a.pano_device,
    a.easy_pay_flag,
    a.do_not_call_flag,
    a.do_not_email_flag,
    a.do_not_mail_flag,
    a.do_not_market_flag,
    a.last_contacted_date_ivr_call,
    a.last_contacted_date_cox_com,
    a.last_contacted_date_cox_app,
    a.cox_segment,
    a.demographic_info1,
    a.demographic_info2,
    a.time_key AS account_time_key,
    p.user_guid,
    p.res_comm_ind AS profile_res_comm_ind,
    p.primary_flag,
    p.user_permission,
    p.email_account_count,
    p.email_flag,
    p.phone_flag AS profile_phone_flag,
    p.email_verified_flag,
    p.phone_verified_flag,
    p.email_verified_date,
    p.phone_verified_date,
    p.email_opt_out,
    p.phone_opt_out,
    p.email_bounce,
    p.preferred_contact_method,
    p.placeholder2,
    p.tsv_enrolled_status,
    p.tsv_email_flag,
    p.tsv_call_flag,
    p.tsv_sms_flag,
    p.preference_placeholder1,
    p.preference_placeholder2,
    p.preferences_last_used_date,
    p.last_logged_in_date_okta,
    p.last_logged_in_date_cox_com,
    p.last_logged_in_date_cox_app,
    p.last_logged_in_os_cox_app,
    p.last_password_change_date,
    p.time_key AS profile_time_key
FROM account_dim_sum a
JOIN profile_dim_sum p
ON a.customer_key = p.customer_key;

========================================================================