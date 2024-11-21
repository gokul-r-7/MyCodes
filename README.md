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
