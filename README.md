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

 









