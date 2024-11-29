With revenue_data AS (
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
 
ivr_contact AS (
    SELECT
        i.customer_key,
        DATE_FORMAT(CAST(i.time_key AS TIMESTAMP), '%Y-%m') AS Contact_Month,
        i.time_key
        --MAX(CAST(i.time_key AS TIMESTAMP)) AS Last_Contacted_Date_IVR_Call
    FROM "call"."call_ivr_fact" i
    GROUP BY i.customer_key,DATE_FORMAT(CAST(i.time_key AS TIMESTAMP), '%Y-%m'),i.time_key
),
web_data AS (
    SELECT
        d.customer_key,
        DATE_FORMAT(CAST(d.dt AS TIMESTAMP), '%Y-%m') AS Contact_Month,
        d.dt
        --MAX(CAST(d.dt AS TIMESTAMP)) AS Last_Contacted_Date_Cox_com
        --ROW_NUMBER() OVER (PARTITION BY d.customer_key ORDER BY dt) AS rn
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS TIMESTAMP), '%Y-%m'),d.dt
),
mob_data AS (
    SELECT
        mob.customer_key,
        DATE_FORMAT(CAST(mob.dt AS TIMESTAMP), '%Y-%m') AS Contact_Month,
        mob.dt
        --MAX(CAST(mob.dt AS TIMESTAMP)) AS Last_Contacted_Date_Cox_App
        --ROW_NUMBER() OVER (PARTITION BY mob.customer_key ORDER BY dt) AS rn
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS TIMESTAMP), '%Y-%m'),mob.dt
)
SELECT distinct
    r.customer_key AS Customer_Key,
    MAX(CAST(ivr.time_key AS TIMESTAMP))as Last_Contacted_Date_IVR_Call,
   MAX(CAST(w.dt AS TIMESTAMP))as Last_Contacted_Date_Cox_com,
    MAX(CAST(mob.dt AS TIMESTAMP)) as Last_Contacted_Date_Cox_App,
	 r.time_key
FROM revenue_data r LEFT JOIN ivr_contact ivr
    ON r.customer_key = CAST(ivr.customer_key AS double)
    AND DATE_FORMAT(CAST(r.time_key AS TIMESTAMP), '%Y-%m') >= ivr.Contact_Month
LEFT JOIN web_data w
    ON r.customer_key = CAST(w.customer_key AS double)
    AND DATE_FORMAT(CAST(r.time_key AS TIMESTAMP), '%Y-%m') >= w.Contact_Month 
LEFT JOIN mob_data mob
    ON r.customer_key = CAST(mob.customer_key AS double)
    AND DATE_FORMAT(CAST(r.time_key AS TIMESTAMP), '%Y-%m') >= mob.Contact_Month
group by r.time_key,r.customer_key)











































select g.household_member_guid from edw.customer_guid_dtl_dim g join ciam_datamodel.tsv_guid_data t on cast(g.household_member_guid as varchar)=cast(t.household_member_guid as varchar)
 
select * from ciam_datamodel.tsv_guid_data where HOUSEHOLD_MEMBER_GUID = '2a1e0a0d-3bc6-6c6b-9f00-017dd952b2c1'
 
select * from
 
select * from EDW.CUSTOMER_GUID_DTL_DIM GUID where HOUSEHOLD_MEMBER_GUID = '2a1e0a0d-3bc6-6c6b-9f00-017dd952b2c1'

































IF { FIXED [Customer Key]: 
        MAX(
            IF DATEPART('month', [time_key (copy)]) = DATEPART('month', DATEADD('month', -1, [selected_date])) 
               AND DATEPART('year', [time_key (copy)]) = DATEPART('year', DATEADD('month', -1, [selected_date])) 
            THEN 1 
            ELSE 0 
            END
        )
    } = 1 
   AND { FIXED [Customer Key]: 
        MAX(
            IF DATEPART('month', [time_key (copy)]) = DATEPART('month', [selected_date]) 
               AND DATEPART('year', [time_key (copy)]) = DATEPART('year', [selected_date]) 
            THEN 1 
            ELSE 0 
            END
        )
    } = 0
THEN 'Churned'
ELSE 'Active'
END


SELECT 
    "Customer_Key",
    CASE 
        WHEN MAX(CASE 
                    WHEN EXTRACT(MONTH FROM CAST("time_key" AS DATE)) = EXTRACT(MONTH FROM DATE_ADD('month', -1, CAST("time_key" AS DATE))) 
                         AND EXTRACT(YEAR FROM CAST("time_key" AS DATE)) = EXTRACT(YEAR FROM DATE_ADD('month', -1, CAST("time_key" AS DATE))) 
                    THEN 1 
                    ELSE 0 
                END) = 1 
            AND MAX(CASE 
                        WHEN EXTRACT(MONTH FROM CAST("time_key" AS DATE)) = EXTRACT(MONTH FROM CAST("time_key" AS DATE)) 
                             AND EXTRACT(YEAR FROM CAST("time_key" AS DATE)) = EXTRACT(YEAR FROM CAST("time_key" AS DATE)) 
                        THEN 1 
                        ELSE 0 
                    END) = 0 
        THEN 'Churned'
        ELSE 'Active'
    END AS status
FROM ciam_datamodel.account_dim_sum
GROUP BY "Customer_Key"


SELECT
    customer_key,
    CASE
        WHEN customer_key NOT IN (
            SELECT customer_key
            FROM ciam_datamodel.account_dim_sum
            WHERE CAST(time_key AS DATE) >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' MONTH
                AND CAST(time_key AS DATE) < DATE_TRUNC('month', CURRENT_DATE)
        ) 
        AND customer_key IN (
            SELECT customer_key
            FROM ciam_datamodel.account_dim_sum
            WHERE CAST(time_key AS DATE) >= DATE_TRUNC('month', CURRENT_DATE)
                AND CAST(time_key AS DATE) < DATE_ADD('month', 1, DATE_TRUNC('month', CURRENT_DATE))
        )
        THEN 'Active'
        ELSE 'Churned'
    END AS status
FROM ciam_datamodel.account_dim_sum group by customer_key




























select sum(distinct_customer_count),last_contacted_cox_app from account_aggregated_view group by last_contacted_cox_app
 
SELECT
    COUNT(DISTINCT customer_key) AS customer_count,
    CASE
        WHEN last_contacted_date_cox_app IS NULL OR last_contacted_date_cox_app = '' OR length(last_contacted_date_cox_app) = 0 THEN 'Never Contacted'
         WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) <=30  THEN 'last 30 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 0 AND 30  THEN '0-30 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 31 AND 90 THEN '31-90 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 91 AND 180 THEN '91-180 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 181 AND 365 THEN '6-12 months'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 366 AND 1030 THEN '1-3 Years'
        ELSE 'Above 2 Years'
    END AS last_contacted_date_cox_app
FROM
    account_dim_sum
GROUP BY
    CASE
        WHEN last_contacted_date_cox_app IS NULL OR last_contacted_date_cox_app = '' OR length(last_contacted_date_cox_app) = 0 THEN 'Never Contacted'
       WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) <=30  THEN 'last 30 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
        date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d'))BETWEEN 0 AND 30  THEN '0-30 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 31 AND 90 THEN '31-90 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 91 AND 180 THEN '91-180 days'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 181 AND 365 THEN '6-12 months'
        WHEN date_diff('day', CAST(time_key AS DATE),
             date_parse(SUBSTRING(last_contacted_date_cox_app, 1, 10), '%Y-%m-%d')) BETWEEN 366 AND 1030 THEN '1-3 Years'
        ELSE 'Above 2 Years'
    END order by 2 desc;






WITH date_diff_table AS (
    SELECT 
        customer_key,
        last_contacted_date_cox_app,
        time_key,
        -- Attempt to cast string dates to date format and calculate the difference in days
        -- Using try_cast to safely handle invalid date formats
        date_diff('day', 
                  try_cast(time_key AS date), 
                  try_cast(last_contacted_date_cox_app AS date)) AS days_difference
    FROM ciam_datamodel.account_dim_sum
)

SELECT
    -- Calculate the distinct customer count
    COUNT(DISTINCT customer_key) AS customer_count,
    
    -- Repeating the CASE expression to categorize days_difference
    CASE 
        -- Check for NULL or empty strings for 'never_contacted'
        WHEN "last_contacted_date_cox_app" IS NULL OR "last_contacted_date_cox_app" = '' THEN 'never_contacted'
        WHEN days_difference BETWEEN 0 AND 30 THEN '0-30 days'
        WHEN days_difference BETWEEN 31 AND 90 THEN '31-90 days'
        WHEN days_difference BETWEEN 91 AND 180 THEN '91-180 days'
        WHEN days_difference BETWEEN 181 AND 365 THEN '6-12 months'
        WHEN days_difference BETWEEN 366 AND 1095 THEN '1-3 years'
        ELSE 'Other' -- For cases where the difference is greater than 3 years
    END AS date_range
FROM date_diff_table
GROUP BY 
    -- Grouping by the same CASE expression as in the SELECT statement
    CASE 
        -- Check for NULL or empty strings for 'never_contacted'
        WHEN last_contacted_date_cox_app IS NULL OR last_contacted_date_cox_app = '' THEN 'never_contacted'
        WHEN days_difference BETWEEN 0 AND 30 THEN '0-30 days'
        WHEN days_difference BETWEEN 31 AND 90 THEN '31-90 days'
        WHEN days_difference BETWEEN 91 AND 180 THEN '91-180 days'
        WHEN days_difference BETWEEN 181 AND 365 THEN '6-12 months'
        WHEN days_difference BETWEEN 366 AND 1095 THEN '1-3 years'
        ELSE 'Other' -- For cases where the difference is greater than 3 years
    END


1
2561962
91-180 days
2
2840578
31-90 days
3
4956236
never_contacted
4
333739
1-3 years
5
2894398
0-30 days
6
2141117
6-12 months
7
2994230
Other



(CASE 
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '0') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '0')) THEN 'Data' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '1') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '0')) THEN 'TV' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '0') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '0')) THEN 'phone' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '0') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '1')) THEN 'mobile'
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '1') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '0')) THEN 'TV+Phone' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '1') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '1')) THEN 'TV+mobile'
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '0') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '1')) THEN 'Phone+mobile'
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '1') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '0')) THEN 'Data+TV' 
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '0') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '0')) THEN 'Data+Phone'
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '0') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '1')) THEN 'Data+mobile'
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '1') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '0')) THEN 'Data+TV+Phone' 
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '1') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '1')) THEN 'Data+TV+mobile'
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '0') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '1')) THEN 'Data+Phone+mobile'
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '1') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '1')) THEN 'TV+Phone+mobile'
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '1') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '1')) THEN 'Data+TV+Phone+mobile' 
ELSE 'None' END) product_service_type 





WITH date_diff_table AS (

    SELECT

        customer_key,

        last_contacted_date_cox_app,

        time_key,

        date_diff('day',

            try_cast(last_contacted_date_cox_app AS date),

                  try_cast(time_key AS date)

                  ) AS days_difference

    FROM ciam_datamodel.account_dim_sum)

select COUNT(DISTINCT customer_key) AS customer_count,date_range from (

SELECT

    customer_key,

    time_key,

   CASE

        when days_difference < 0 then 'less than 0 days'

        WHEN "last_contacted_date_cox_app" IS NULL OR "last_contacted_date_cox_app" = '' THEN 'never_contacted'

        WHEN days_difference BETWEEN 0 AND 30 THEN '0-30 days'

        WHEN days_difference BETWEEN 31 AND 90 THEN '31-90 days'

        WHEN days_difference BETWEEN 91 AND 180 THEN '91-180 days'

        WHEN days_difference BETWEEN 181 AND 365 THEN '6-12 months'

        WHEN days_difference BETWEEN 366 AND 1095 THEN '1-3 years'

        ELSE 'Other'

    END AS date_range

FROM date_diff_table

GROUP BY

customer_key,

time_key,

    CASE

        when days_difference < 0 then 'less than 0 days'

        WHEN last_contacted_date_cox_app IS NULL OR last_contacted_date_cox_app = '' THEN 'never_contacted'

        WHEN days_difference BETWEEN 0 AND 30 THEN '0-30 days'

        WHEN days_difference BETWEEN 31 AND 90 THEN '31-90 days'

        WHEN days_difference BETWEEN 91 AND 180 THEN '91-180 days'

        WHEN days_difference BETWEEN 181 AND 365 THEN '6-12 months'

        WHEN days_difference BETWEEN 366 AND 1095 THEN '1-3 years'

        ELSE 'Other' 

    END) where date_format(CAST(time_key as date),'%Y-%m')='2024-10' group by date_range

 







SELECT DISTINCT     COALESCE(w.evar61_coxcust_guid, a.coxcust_guid_v61) AS User_guid,    COALESCE(w.evar75_marketing_cloud_id) AS Adobe_ECID,    COALESCE(w.visits, a.visits) AS Adobe_Visit_Id,    COALESCE(w.date_time, a.date_time) AS Activity_Date,    a2.create_dt as Registration_Date,    b.inception_dt as Inception_date,    CASE         WHEN w.pagename LIKE 'cox:res:myprofile:communication%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:contacts%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:disability%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:email:confirmation%' THEN 'Email Verification'        WHEN w.pagename LIKE 'cox:res:myprofile:forgot-password:%' THEN 'Forgot Password'        WHEN w.pagename LIKE 'cox:res:myprofile:forgot-userid:%' THEN 'Forgot UserID'        WHEN w.pagename LIKE 'cox:res:myprofile:forgotuserid:%' THEN 'Forgot UserID'        WHEN w.pagename LIKE 'cox:res:myprofile:fuid:%' THEN 'Forgot UserID'        WHEN w.pagename LIKE 'cox:res:myprofile:home%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:mailing-address%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:mailingaddress%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:manageusers%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:notifications%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:number-lock-protection%' THEN 'Number Lock Protection'        WHEN w.pagename LIKE 'cox:res:myprofile:password%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:privacy%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:reg:%' THEN 'Registration'        WHEN w.pagename LIKE 'cox:res:myprofile:security%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:updateprofile%' THEN 'Manage Profile'        WHEN w.pagename LIKE 'cox:res:myprofile:syncupaccount%' THEN 'Sync Account'        WHEN w.pagename LIKE 'cox:res:myprofile:tsv' THEN 'TSV Enrollment'        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:email:%' THEN 'TSV Verification'        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:call:%' THEN 'TSV Verification'        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:text:%' THEN 'TSV Verification'        WHEN w.pagename LIKE 'cox:res:myprofile:tsv:reset:%' THEN 'TSV Reset'        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:confirmation%' THEN 'Verify Contact'        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:email%' THEN 'Verify Contact'        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:landing%' THEN 'Verify Contact'        WHEN w.pagename LIKE 'cox:res:myprofile:verify-identity:phone%' THEN 'Verify Contact'        ELSE 'Unknown'    END AS Activity_Name,    COALESCE(w.pagename, a.pagename) AS Activity_Page,    COALESCE(w.mvvar3, a.server_form_error_p13) AS Server_Error,    NULL AS Client_Error,    COALESCE(w.campaign, a.post_evar40) AS Traffic_Source_Detail_sc_idfrom edw.customer_guid_dtl_dim a2LEFT JOIN edw.customer_dim b ON a2.customer_key = b.customer_keyLEFT JOIN webanalytics.web_contact_history w ON a2.household_member_guid = w.evar61_coxcust_guidLEFT JOIN mobile_data_temp.app_contact_history a on a2.household_member_guid = a.coxcust_guid_v61WHERE    DATE_PARSE(SUBSTR(CAST(a.dt AS varchar), 1, 19), '%Y-%m-%d') >= DATE_ADD('day', -365, CURRENT_DATE)    AND DATE_PARSE(SUBSTR(CAST(w.dt AS varchar), 1, 19), '%Y-%m-%d') >= DATE_ADD('day', -365, CURRENT_DATE)    AND (a.pagename LIKE 'coxapp:reg:%' OR a.pagename LIKE 'coxapp:myaccount%')    AND w.pagename LIKE 'cox:res:myprofile%'











(CASE 
WHEN ((ads.data_flag = '1') AND (ads.tv_flag = '0') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '0')) THEN 'Data' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '1') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '0')) THEN 'TV' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '0') AND (ads.phone_flag = '1') AND (ads.mobile_flag = '0')) THEN 'phone' 
WHEN ((ads.data_flag = '0') AND (ads.tv_flag = '0') AND (ads.phone_flag = '0') AND (ads.mobile_flag = '1')) THEN 'mobile'
ELSE 'None' END) product_service_type 















