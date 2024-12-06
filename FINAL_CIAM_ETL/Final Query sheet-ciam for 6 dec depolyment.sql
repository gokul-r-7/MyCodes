********************************Account_dim**********************************************
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
where test_account_key=2
),
revenue_data AS (
SELECT r.customer_key,
r.site_id,
r.dwelling_type_key,
r.easy_pay_flag,
case when r.mobile_gross_mrc > 0 then '1' else '0' end as Mobile_Flag,
           r.customer_substatus_key,
		   r.time_key
    FROM edw.customer_revenue_fact r
	 WHERE r.bill_type_key != 2 and
    DATE_PARSE(r.time_key, '%Y-%m-%d') >= DATE_ADD('month', -13, CURRENT_DATE)
),
customer_status As
(select customer_substatus_key,sub_status_desc from edw.customer_substatus_dim),
dwelling_data AS (
    SELECT distinct d.dwelling_type_key,
           d.dwelling_type_desc
    FROM edw.dwelling_type_dim d
),
account_summary AS (
    SELECT
           s.customer_key,
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
           ROW_NUMBER() over (partition by g.customer_key order by g.create_dt asc) as rn
           FROM edw.customer_guid_dtl_dim g
),
web_contact AS (
        SELECT
        campaign,
        evar61_coxcust_guid,
        ROW_NUMBER() OVER (PARTITION BY evar61_coxcust_guid ORDER BY MIN(dt)) AS rn
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
        group by campaign,
        evar61_coxcust_guid
        ),
app_contact AS (
    SELECT
        coxcust_guid_v61,
        post_evar40,
		ROW_NUMBER() OVER (PARTITION BY coxcust_guid_v61 ORDER BY MIN(dt)) AS rn
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
        group BY
        coxcust_guid_v61,
        post_evar40
),
ivr_contact AS (
    SELECT distinct 
        i.customer_key,
        DATE_FORMAT(CAST(i.time_key AS DATE), '%Y-%m') AS Contact_Month,
        MAX(CAST(i.time_key AS DATE)) AS Last_Contacted_Date_IVR_Call
    FROM "call"."call_ivr_fact" i
    GROUP BY i.customer_key,DATE_FORMAT(CAST(i.time_key AS DATE), '%Y-%m')
),
web_data AS (
    SELECT distinct
        d.customer_key,
        DATE_FORMAT(CAST(d.dt AS DATE), '%Y-%m') AS Contact_Month,
        MAX(CAST(d.dt AS DATE)) AS Last_Contacted_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS DATE), '%Y-%m')
),
mob_data AS (
    SELECT distinct
        mob.customer_key,
        DATE_FORMAT(CAST(mob.dt AS DATE), '%Y-%m') AS Contact_Month,
        MAX(CAST(mob.dt AS DATE)) AS Last_Contacted_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS DATE), '%Y-%m')
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
    CAST(NULL AS varchar) AS "Sale_Acquisition_Channel",
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
    s.telephony_flag AS "Phone_Flag",
    CAST(NULL AS varchar) AS "Homelife_Flag",
    r.Mobile_Flag,
    cd.sub_status_desc,
    CAST(NULL AS varchar) AS "Pano_Flag",
    CAST(NULL AS varchar) AS "Pano_Device",
    r.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    MAX(ivr.Last_Contacted_Date_IVR_Call) as Last_Contacted_Date_IVR_Call,
    MAX(w.Last_Contacted_Date_Cox_com) as Last_Contacted_Date_Cox_com ,
    MAX(mob.Last_Contacted_Date_Cox_App) as Last_Contacted_Date_Cox_App, 
    CAST(NULL AS varchar) AS "Cox_Segment",
    CAST(NULL AS varchar) AS "Demographic_Info1",
    CAST(NULL AS varchar) AS "Demographic_Info2",
    r.time_key
FROM revenue_data r LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
LEFT JOIN (select * from account_summary where rn=1)s ON r.customer_key = CAST(s.customer_key AS double)
LEFT JOIN customer_status cd on r.customer_substatus_key=cd.customer_substatus_key 
LEFT JOIN customer_data c ON r.customer_key = CAST(c.customer_key AS double)
LEFT JOIN (select * from guid_data where rn=1) g ON r.customer_key = CAST(g.customer_key AS double)
LEFT JOIN (select * from web_contact where rn=1) web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN (select * from app_contact where rn=1) app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN ivr_contact ivr
    ON r.customer_key = CAST(ivr.customer_key AS double) and ivr.Contact_Month <= r.time_key
LEFT JOIN web_data w
    ON r.customer_key = CAST(w.customer_key AS double) and w.Contact_Month <=r.time_key
LEFT JOIN mob_data mob
    ON r.customer_key = CAST(mob.customer_key AS double) and mob.Contact_Month <= r.time_key
group by
r.customer_key,
    c.account_nbr,
    r.site_id,
    c.res_comm_ind,
    c.customer_status_cd,
    d.dwelling_type_desc,
    c.account_guid,
    c.prim_account_holder_guid,
    s.employee_flag,
    c.test_account_key ,
    c.inception_dt,
    g.create_dt,
    COALESCE(web.campaign, app.post_evar40),
    s.data_flag,
    s.cable_flag,
    s.telephony_flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    r.easy_pay_flag,
    s.do_not_call_flag,
    s.do_not_email_flag,
    s.do_not_mail_flag,
    s.do_not_market_flag,	
	r.time_key
	)
	
	
****************************************************Profile_dim********************************************************
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
        where test_account_key=2
),
revenue_data AS (
    SELECT r.customer_key,
		   r.site_id,
		   r.time_key
    FROM edw.customer_revenue_fact r
	 WHERE r.bill_type_key != 2 and
    DATE_PARSE(r.time_key, '%Y-%m-%d') >= DATE_ADD('month', -13, CURRENT_DATE)
),
guid_data AS (
    SELECT 
        a.customer_key,
        a.create_dt AS Registration_date,
        a.user_id,
        a.prim_customer_flag AS Primary_Flag,
        a.household_member_guid AS User_GUID,
        a.cox_email_address,
        e.email_account_count as Email_Account_Count
    FROM
        edw.customer_guid_dtl_dim a
    JOIN 
    (SELECT cox_email_address, COUNT(DISTINCT customer_key) AS email_account_count
     FROM edw.customer_guid_dtl_dim
     GROUP BY cox_email_address) e ON a.cox_email_address = e.cox_email_address
),
tsv_guid_data as (select cast(t."household member guid" as varchar)as tsv_guid from ciam_datamodel.tsv_guid_data t),
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
        DATE_FORMAT(CAST(o.event_date as DATE), '%Y-%m') AS Contact_Month,
        MAX(o.event_date) AS last_logged_in_date_okta,
        MAX(CASE WHEN d.outcome_reason LIKE '%EMAIL%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_EMAIL_Flag,
        MAX(CASE WHEN d.outcome_reason LIKE '%CALL%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_CALL_Flag,
        MAX(CASE WHEN d.outcome_reason LIKE '%SMS%' AND d.event_date = e.latest_event_date THEN 1 ELSE 0 END) AS TSV_SMS_Flag
    FROM
        edw.customer_guid_dtl_dim a
        LEFT JOIN ciam.mfa_factors_enrolled d ON a.user_id = d.username
        LEFT JOIN latest_event_dates e ON a.user_id = e.user_id AND d.outcome_reason = e.outcome_reason
        LEFT JOIN ciam.successful_authentications_okta o ON a.user_id = o.actor_alternateid
    GROUP BY
        a.user_id,DATE_FORMAT(CAST(o.event_date as DATE), '%Y-%m')
),
account_details AS (
    SELECT ac.account_nbr,
           ac.vndr_cust_account_key,
           ac.do_not_email AS Email_Opt_Out,
           ac.do_not_call AS Phone_Opt_Out
           --em.bounced_flg AS Email_Bounce,
           --em.unsubscribe_flg,
           --ROW_NUMBER() OVER (PARTITION BY ac.account_nbr ORDER BY CASE WHEN em.bounced_flg IS NOT NULL THEN 1 ELSE 2 END) AS rn
    FROM camp_mgmt.accounts ac
    LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
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
),
web_data AS (
    SELECT distinct
        d.customer_key,
        DATE_FORMAT(CAST(d.dt AS DATE), '%Y-%m') AS Contact_Month,
        MAX(CAST(d.dt AS DATE)) AS Last_Logged_In_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS DATE), '%Y-%m')
),
mob_data AS (
    SELECT distinct
        mob.customer_key,
        MAX(mob.post_mobileappid) AS Last_Logged_In_App_ID,
        DATE_FORMAT(CAST(mob.dt AS DATE), '%Y-%m') AS Contact_Month,
        MAX(CAST(mob.dt AS DATE)) AS Last_Logged_In_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS DATE), '%Y-%m')
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
    CAST(NULL AS varchar) AS "User_Permission",
    a.Email_Account_Count,
    nf.Email_Flag,
    nf.Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,   
    --ad.Email_Bounce,
    --ad.undelivered_sms,    
    CAST(NULL AS varchar) AS "Preferred_Contact_Method",
    CAST(NULL AS varchar)AS "Placeholder2",
    case WHEN (t.tsv_guid) IS NOT NULL THEN 'group.user_membership.add'
        ELSE NULL
        END AS TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    CAST(NULL AS varchar) AS "Preference_Placeholder1",
    CAST(NULL AS varchar) AS "Preference_Placeholder2",
    CAST(NULL AS varchar) AS "Preferences_Last_Used_Date",
    MAX(mfa.last_logged_in_date_okta)as last_logged_in_date_okta ,
    MAX(w.Last_Logged_In_Date_Cox_com) as Last_Logged_In_Date_Cox_com,
    MAX(mob.Last_Logged_In_Date_Cox_App) as Last_Logged_In_Date_Cox_App,
    CASE 
        WHEN mob.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN mob.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END AS "Last_Logged_In_OS_Cox_App",
    CAST(NULL AS varchar) AS "Last_Password_Change_Date",
    r.time_key
FROM revenue_data r left join customer_dim b on r.customer_key=b.customer_key
left join guid_data a on r.customer_key=a.customer_key
INNER JOIN tsv_guid_data t on a.User_GUID=t.tsv_guid
LEFT JOIN web_data w
    ON r.customer_key = CAST(w.customer_key AS double) and w.Contact_Month <=r.time_key
LEFT JOIN mob_data mob
    ON r.customer_key = CAST(mob.customer_key AS double) and mob.Contact_Month <= r.time_key
LEFT JOIN  mfa_data mfa ON a.user_id = mfa.user_id AND mfa.Contact_Month <= r.time_key
LEFT JOIN account_details ad ON b.account_nbr = ad.account_nbr
LEFT JOIN notification_flags nf ON b.account_nbr = nf.customer_number
group BY
a.User_GUID,
    b.Res_Com_Ind,
    a.Primary_Flag,
    r.Customer_Key,
    b.Account_Nbr,
    r.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    a.Email_Account_Count,
    nf.Email_Flag,
    nf.Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,   
    --ad.Email_Bounce,
    --ad.undelivered_sms,    
    case WHEN (t.tsv_guid) IS NOT NULL THEN 'group.user_membership.add'
        ELSE NULL
        END,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    CASE 
        WHEN mob.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN mob.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END ,
    r.time_key
	
*********************************************Transcation_adobe_fact**********************************

WITH web_contact_history AS (
    SELECT 
        evar61_coxcust_guid AS User_guid,
        evar75_marketing_cloud_id AS Adobe_ECID,
        visits AS Adobe_Visit_Id,
        date_time AS Activity_Date,
        pagename AS Activity_Page,
        mvvar3 AS Server_Error,
        CAST(NULL AS varchar) AS Client_Error,
        campaign AS Traffic_Source_Detail_sc_id
    FROM webanalytics.web_contact_history
    WHERE DATE_PARSE(SUBSTR(CAST(dt AS varchar), 1, 19), '%Y-%m-%d') >= DATE_ADD('day', -365, CURRENT_DATE)
    AND pagename LIKE 'cox:res:myprofile%'
),
app_contact_history AS (
    SELECT
        coxcust_guid_v61 AS User_guid,
        CAST(NULL AS varchar) AS Adobe_ECID,
        visits AS Adobe_Visit_Id,
        date_time AS Activity_Date,
        pagename AS Activity_Page,
        server_form_error_p13 AS Server_Error,
        CAST(NULL AS varchar) AS Client_Error,
        post_evar40 AS Traffic_Source_Detail_sc_id
    FROM mobile_data_temp.app_contact_history
    WHERE DATE_PARSE(SUBSTR(CAST(dt AS varchar), 1, 19), '%Y-%m-%d') >= DATE_ADD('day', -365, CURRENT_DATE)
    AND (pagename LIKE 'coxapp:reg:%' OR pagename LIKE 'coxapp:myaccount%')
),
union_cte AS (
    SELECT * FROM web_contact_history W
    UNION ALL 
    SELECT * FROM app_contact_history a
)
select distinct 
    User_guid,
    Adobe_ECID,
    Adobe_Visit_Id,
    Activity_Date,
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
    	WHEN u.Activity_Page LIKE '%:phone:text' then 	'Attempt'
    	WHEN u.Activity_Page LIKE '%lookup-account' then 	'Attempt' 
    	WHEN u.Activity_Page LIKE '%lookup-email' then	'Attempt' 
    	WHEN u.Activity_Page LIKE '%lookup-phone' then 	'Attempt' 
    	WHEN u.Activity_Page LIKE '%lookup-address' then 'Attempt' 
    	WHEN u.Activity_Page LIKE '%userid-exists' then 'Already Registered'
    	WHEN u.Activity_Page LIKE '%multi-address%' then 'Multiple Address' 
    	WHEN u.Activity_Page LIKE '%:notifications' then 'Prefrence Center'
    	WHEN u.Activity_Page LIKE '%:communication' then 'Prefrence Center'
    	WHEN u.Activity_Page LIKE '%:secret-question' then	'Secret Question'
    	WHEN u.Activity_Page LIKE '%:sendcode' then 'Send Code'
    	WHEN u.Activity_Page LIKE 'cox:res:myprofile:reg:userid-password' then 'Setup Credentials'
    	WHEN u.Activity_Page LIKE '%:landing' then 'Start page' 
    	WHEN u.Activity_Page LIKE '%:confirmation' then 'Success' 
    	WHEN u.Activity_Page LIKE '%:lookup-account-success' then 'Success'
    	WHEN u.Activity_Page LIKE '%:lookup-address-success' then 'Success'
    	WHEN u.Activity_Page LIKE '%:success'then 'Success' 
    	WHEN u.Activity_Page LIKE '%:verification%' then 'Verify Code' 
    	WHEN u.Activity_Page LIKE '%:verifycode' then 'Verify Code' 
    	ELSE 'Unknown'
    END AS Navigation_step,
    Activity_Page,
    Server_Error,
    Client_Error,
    Traffic_Source_Detail_sc_id
from union_cte u
left join edw.customer_guid_dtl_dim a2 on u.User_guid = a2.household_member_guid
left join edw.customer_dim b ON a2.customer_key = b.customer_key
group by User_guid,
    Adobe_ECID,
    Adobe_Visit_Id,
    Activity_Page,
    Activity_Date,
    a2.create_dt,
    b.inception_dt,
    Server_Error,
    Client_Error,
    Traffic_Source_Detail_sc_id,
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
    	WHEN u.Activity_Page LIKE '%:phone:text' then 	'Attempt'
    	WHEN u.Activity_Page LIKE '%lookup-account' then 	'Attempt' 
    	WHEN u.Activity_Page LIKE '%lookup-email' then	'Attempt' 
    	WHEN u.Activity_Page LIKE '%lookup-phone' then 	'Attempt' 
    	WHEN u.Activity_Page LIKE '%lookup-address' then 'Attempt' 
    	WHEN u.Activity_Page LIKE '%userid-exists' then 'Already Registered'
    	WHEN u.Activity_Page LIKE '%multi-address%' then 'Multiple Address' 
    	WHEN u.Activity_Page LIKE '%:notifications' then 'Prefrence Center'
    	WHEN u.Activity_Page LIKE '%:communication' then 'Prefrence Center'
    	WHEN u.Activity_Page LIKE '%:secret-question' then	'Secret Question'
    	WHEN u.Activity_Page LIKE '%:sendcode' then 'Send Code'
    	WHEN u.Activity_Page LIKE 'cox:res:myprofile:reg:userid-password' then 'Setup Credentials'
    	WHEN u.Activity_Page LIKE '%:landing' then 'Start page' 
    	WHEN u.Activity_Page LIKE '%:confirmation' then 'Success' 
    	WHEN u.Activity_Page LIKE '%:lookup-account-success' then 'Success'
    	WHEN u.Activity_Page LIKE '%:lookup-address-success' then 'Success'
    	WHEN u.Activity_Page LIKE '%:success'then 'Success' 
    	WHEN u.Activity_Page LIKE '%:verification%' then 'Verify Code' 
    	WHEN u.Activity_Page LIKE '%:verifycode' then 'Verify Code' 
    	ELSE 'Unknown'
    END
	
*******************************************TRANSACTION user level********************************
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
    aggregated_auth a LEFT JOIN edw.customer_guid_dtl_dim g ON a.user_id = g.user_id
LEFT JOIN
    aggregated_signon o ON g.user_id = o.user_id AND a.event_date = o.event_date
LEFT JOIN
    ciam.mfa_total_mfa_users m ON g.user_id = m.username	
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
    aggregated_auth a LEFT JOIN edw.customer_guid_dtl_dim g ON a.user_id = g.user_id
LEFT JOIN
    aggregated_signon o ON g.user_id = o.user_id AND a.event_date = o.event_date
LEFT JOIN
    ciam.mfa_total_mfa_users m ON g.user_id = m.username
GROUP BY
a.event_date, a.host, o.application, m.eventtype,a.Authentication_Attempt,a.authentication_success_result

