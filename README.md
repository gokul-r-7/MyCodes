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
 
