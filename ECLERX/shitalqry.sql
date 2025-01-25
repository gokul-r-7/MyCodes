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
        case WHEN (t."household member guid") IS NOT NULL THEN 'group.user_membership.add'
        ELSE NULL
        END AS TSV_Enrolled_Status,
        email_counts.Email_Account_Count
    FROM
        edw.customer_guid_dtl_dim a
    INNER JOIN ciam_datamodel.tsv_guid_data t on cast(a.household_member_guid as varchar)=cast(t."household member guid" as varchar)
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
        DATE_FORMAT(CAST(o.event_date as TIMESTAMP), '%Y-%m') AS Contact_Month,
        MAX(o.event_date) AS last_logged_in_date_okta,
        c.eventtype,
        --c.event_date AS max_event_date,
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
        a.user_id, c.eventtype,DATE_FORMAT(CAST(o.event_date as TIMESTAMP), '%Y-%m') --c.event_date
),
account_details AS (
    SELECT 
        ob.account_nbr,
        c.vndr_cust_account_key,
        ob.sms_gateway_status as undelivered_sms,
        c.bounced_flag,
        ROW_NUMBER() OVER (PARTITION BY c.vndr_cust_account_key ORDER BY CASE WHEN c.bounced_flag IS NOT NULL THEN 1 ELSE 2 END) AS rn
    FROM cci_mkt_sale.ac_smsgateway_outbound ob LEFT JOIN cci_mkt_sale.email_response c ON ob.vndr_cust_account_key = c.vndr_cust_account_key
),
Opt_out_details AS (
    SELECT ac.account_nbr,
           ac.vndr_cust_account_key,
           ac.do_not_email AS Email_Opt_Out,
           ac.do_not_call AS Phone_Opt_Out
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
        DATE_FORMAT(CAST(d.dt AS TIMESTAMP), '%Y-%m') AS Contact_Month,
        MAX(CAST(d.dt AS TIMESTAMP)) AS Last_Logged_In_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS TIMESTAMP), '%Y-%m')
),
mob_data AS (
    SELECT distinct
        mob.customer_key,
        MAX(mob.post_mobileappid) AS Last_Logged_In_App_ID,
        DATE_FORMAT(CAST(mob.dt AS TIMESTAMP), '%Y-%m') AS Contact_Month,
        MAX(CAST(mob.dt AS TIMESTAMP)) AS Last_Logged_In_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS TIMESTAMP), '%Y-%m')
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
    o.Email_Opt_Out, 
    o.Phone_Opt_Out,   
    ad.bounced_flag,
    ad.undelivered_sms,    
    CAST(NULL AS varchar) AS "Preferred_Contact_Method",
    CAST(NULL AS varchar)AS "Placeholder2",
    a.TSV_Enrolled_Status,
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
LEFT JOIN web_data w
    ON r.customer_key = CAST(w.customer_key AS double) and w.Contact_Month <=r.time_key
LEFT JOIN mob_data mob
    ON r.customer_key = CAST(mob.customer_key AS double) and mob.Contact_Month <= r.time_key
LEFT JOIN  mfa_data mfa ON a.user_id = mfa.user_id AND mfa.Contact_Month <= r.time_key
LEFT JOIN account_details ad ON b.account_nbr = ad.account_nbr AND ad.rn = 1
LEFT JOIN Opt_out_details o on b.account_nbr =o.account_nbr
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
    o.Email_Opt_Out, 
    o.Phone_Opt_Out,   
    ad.bounced_flag,
    ad.undelivered_sms,    
    a.TSV_Enrolled_Status,
    mfa.TSV_EMAIL_Flag,
    mfa.TSV_CALL_Flag,
    mfa.TSV_SMS_Flag,
    CASE 
        WHEN mob.Last_Logged_In_App_ID LIKE 'CoxAccount%' THEN 'iOS'
        WHEN mob.Last_Logged_In_App_ID LIKE 'Cox %' THEN 'Android'
        ELSE 'Null'
    END ,
    r.time_key
