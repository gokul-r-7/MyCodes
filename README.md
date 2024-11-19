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


WITH churn_check AS (
    SELECT
        "Customer_Key",
        MAX(CASE 
                WHEN EXTRACT(MONTH FROM CAST("time_key" AS DATE)) = EXTRACT(MONTH FROM DATE_ADD('month', -1, CAST("time_key" AS DATE))) 
                     AND EXTRACT(YEAR FROM CAST("time_key" AS DATE)) = EXTRACT(YEAR FROM DATE_ADD('month', -1, CAST("time_key" AS DATE))) 
                THEN 1 
                ELSE 0 
            END) AS prev_month_active,
        MAX(CASE 
                WHEN EXTRACT(MONTH FROM CAST("time_key" AS DATE)) = EXTRACT(MONTH FROM CAST("time_key" AS DATE)) 
                     AND EXTRACT(YEAR FROM CAST("time_key" AS DATE)) = EXTRACT(YEAR FROM CAST("time_key" AS DATE)) 
                THEN 1 
                ELSE 0 
            END) AS current_month_active
    FROM ciam_datamodel.account_dim_sum
    GROUP BY "Customer_Key"
)

SELECT 
    "Customer_Key",
    CASE 
        WHEN prev_month_active = 1 AND current_month_active = 0 THEN 'Churned'
        ELSE 'Active'
    END AS status
FROM churn_check
