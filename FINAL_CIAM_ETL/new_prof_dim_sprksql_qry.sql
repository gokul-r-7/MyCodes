profile_dim_sum_new_query = """
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
    WHERE test_account_key = 2
),
revenue_data AS (
    SELECT r.customer_key,
           r.site_id,
           r.time_key
    FROM edw.customer_revenue_fact r
    WHERE r.bill_type_key != 2 AND
    to_date(r.time_key, 'yyyy-MM-dd') >= ADD_MONTHS(CURRENT_DATE, -13)
),
account_summary AS (
    SELECT
           s.account_nbr,
           s.customer_key,
           s.time_key,
           ROW_NUMBER() OVER (PARTITION BY s.customer_key ORDER BY s.time_key DESC) AS rn
    FROM edw.cust_acct_sum s
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
tsv_enrolled_data as (SELECT t.login from ciam_datamodel.mfa_usage_report t),
web_data AS (
    SELECT
    d.customer_key,
    date_format(CAST(d.dt AS TIMESTAMP), 'yyyy-MM') AS Contact_Month,
    ROW_NUMBER() OVER (PARTITION BY d.customer_key ORDER BY d.dt) AS rn,
    MAX(d.dt) AS Last_Logged_In_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, date_format(CAST(d.dt AS TIMESTAMP), 'yyyy-MM'), d.dt
),
mob_data AS (
    SELECT
    mob.customer_key,
    date_format(CAST(mob.dt AS TIMESTAMP), 'yyyy-MM') AS Contact_Month,
    MAX(mob.dt) AS Last_Logged_In_Date_Cox_App,
    ROW_NUMBER() OVER (PARTITION BY mob.customer_key ORDER BY mob.dt) AS rn
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, date_format(CAST(mob.dt AS TIMESTAMP), 'yyyy-MM'), mob.dt
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
    c.eventtype,
    date_format(CAST(o.event_date AS TIMESTAMP), 'yyyy-MM') AS Contact_Month,
    MAX(o.event_date) AS last_logged_in_date_okta,
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
    a.user_id, c.eventtype, c.event_date, date_format(CAST(o.event_date AS TIMESTAMP), 'yyyy-MM')
),
account_details AS (
    SELECT ac.account_nbr,
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
    r.Customer_Key,
    s.Account_Nbr,
    r.Site_Id,
    b.Customer_Status,
    b.Inception_Date,
    a.Registration_date,
    NULL AS User_Permission,
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
    NULL AS Preferred_Contact_Method,
    NULL AS Placeholder2,
    CASE WHEN t.login IS NOT NULL THEN 'group.user_membership.add'
        ELSE mfa.eventtype
        END AS TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    NULL AS Preference_Placeholder1,
    NULL AS Preference_Placeholder2,
    NULL AS Preferences_Last_Used_Date,
    mfa.last_logged_in_date_okta,
    w.Last_Logged_In_Date_Cox_com,
    mob.Last_Logged_In_Date_Cox_App,
    CASE 
        WHEN app.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN app.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END AS Last_Logged_In_OS_Cox_App,
    NULL AS Last_Password_Change_Date,
    r.time_key
FROM revenue_data r LEFT JOIN (SELECT * FROM account_summary WHERE rn=1) s ON r.customer_key = CAST(s.customer_key AS bigint)
LEFT JOIN customer_dim b ON r.customer_key = b.customer_key
LEFT JOIN guid_data a ON r.customer_key = a.customer_key 
LEFT JOIN tsv_enrolled_data t ON a.user_id = t.login
LEFT JOIN mob_data mob
ON r.customer_key = CAST(mob.customer_key AS double)
AND date_format(CAST(r.time_key AS TIMESTAMP), 'yyyy-MM') >= mob.Contact_Month AND mob.rn = 1
LEFT JOIN web_data w
ON r.customer_key = CAST(w.customer_key AS double)
AND date_format(CAST(r.time_key AS TIMESTAMP), 'yyyy-MM') >= w.Contact_Month AND w.rn = 1
LEFT JOIN app_contact app ON r.customer_key = CAST(app.customer_key AS bigint) 
LEFT JOIN mfa_data mfa ON a.user_id = mfa.user_id AND date_format(CAST(r.time_key AS TIMESTAMP), 'yyyy-MM') >= mfa.Contact_Month
LEFT JOIN account_details ad ON s.account_nbr = ad.account_nbr AND ad.rn = 1
LEFT JOIN notification_flags nf ON s.account_nbr = nf.customer_number
"""
