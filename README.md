
https://teams.microsoft.com/l/meetup-join/19%3ameeting_N2JiNWU1ZTctOWViMi00NWFkLTlhNjktM2Q1ZWFhZjdiOGFh%40thread.v2/0?context=%7b%22Tid%22%3a%229feebc97-ff04-42c9-a152-767073872118%22%2c%22Oid%22%3a%22d643c3ed-5ac2-4a06-9626-fce002cfd0c9%22%7d













s3://cci-dig-aicoe-data-sb/processed/ciam_prod/


https://www.credly.com/badges/69c40bc6-26f4-43fd-8436-2345d92e4a64/public_url








import pandas as pd

# Example data
first_df = pd.DataFrame({
    'metric_id': [1, 2, 3, 4, 5, 6, 7],
    'display_name': ['A', 'B', 'C', 'D', 'E', 'F', 'G'],
    'feature_name': ['X', 'Y', 'Z', 'X', 'Y', 'Z', 'X']
})

second_df = pd.DataFrame({
    'id': [10, 11, 12, 13],
    'primary_intent': ['Intent1', 'Intent2', 'Intent3', 'Intent4'],
    'feature_name': ['X', 'Y', 'Z', 'X']
})

# Step 1: Create a version of first_df where columns from second_df are null
first_with_nulls = first_df.copy()
first_with_nulls['id'] = None
first_with_nulls['primary_intent'] = None

# Step 2: Create a version of second_df where columns from first_df are null
second_with_nulls = second_df.copy()
second_with_nulls['metric_id'] = None
second_with_nulls['display_name'] = None
second_with_nulls['feature_name'] = None  # Remove if you don't want duplicate 'feature_name' column

# Step 3: Concatenate both DataFrames
final_result = pd.concat([first_with_nulls, second_with_nulls], ignore_index=True)

# Display the result
print(final_result)








































# Get unique values from the columns
primary_intent_values = df['primary_intent'].unique()
primary_intent_detail_values = df['primary_intent_detail'].unique()

# Convert arrays to strings formatted for SQL IN clauses
primary_intent_str = "', '".join(primary_intent_values)
primary_intent_detail_str = "', '".join(primary_intent_detail_values)

second_table_query = f"""
SELECT 
    primary_intent,initial_channel,lob,  
    CAST(contact_dt AS DATE) AS contact_dt, 
    COUNT(DISTINCT sub_contact_id) AS sub_contact_id, 
    COUNT(DISTINCT CASE WHEN selfservice_containment = 1 THEN sub_contact_id END) AS selfservice_containment,
    CASE 
        WHEN COUNT(DISTINCT sub_contact_id) > 0 THEN
            ROUND(CAST(SUM(CASE WHEN selfservice_containment = 1 THEN 1 ELSE 0 END) AS DOUBLE) 
            / COUNT(DISTINCT sub_contact_id) * 100, 2)
        ELSE
            0
    END AS containment_rate
FROM 
    ota_data_assets_temp.omni_intent_cntct_fact 
WHERE 
    CAST(contact_dt AS DATE) BETWEEN 
        date_add((SELECT max(CAST(contact_dt AS DATE)) FROM ota_data_assets_temp.omni_intent_cntct_fact), -60) 
        AND (SELECT max(CAST(contact_dt AS DATE)) FROM ota_data_assets_temp.omni_intent_cntct_fact)
    AND primary_intent IN ('{primary_intent_str}')
    AND initial_channel = 'CoxApp'
    AND lob = 'R'
    AND primary_intent_detail IN ('{primary_intent_detail_str}')
GROUP BY 
    primary_intent, contact_dt,initial_channel,lob
ORDER BY 
    contact_dt DESC
"""

second_table_df = spark.sql(second_table_query)
second_table_df.show()




























































select 
primary_intent,
--primary_intent_detail,
CAST(contact_dt AS DATE) AS contact_dt,
COUNT(DISTINCT sub_contact_id) AS sub_contact_id, 		
    COUNT(DISTINCT CASE WHEN selfservice_containment = 1 THEN sub_contact_id END) AS contained
    from omni_intent_cntct_fact
    where primary_intent = 'Billing', 
    and primary_intent_detail IN ('Billing Concern', 'Billing General', 'Billing Preferences', 'EasyPay','Non-Pay Suspension/Disconnect','Pay Bill')
    and initial_channel = 'CoxApp'		
    and lob = 'R'
    --and CAST(contact_dt AS DATE) BETWEEN date_add('day', -90, DATE '2024-08-27') AND DATE '2024-08-27' 
   and CAST(contact_dt AS DATE) BETWEEN date_add('day', -60, (SELECT max(CAST(contact_dt AS DATE)) FROM  omni_intent_cntct_fact)) 
                            AND (SELECT max(CAST(contact_dt AS DATE)) FROM  omni_intent_cntct_fact)
    group by 1,2
    order by contact_dt























select da.*,count(ivr.customer_key) from digital_adoption_all_channels da JOIN "call"."call_ivr_fact" ivr
    ON da.customer_key = ivr.customer_key where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3--IVR_call_total_customers
select da.*,count(mch.customer_key) from digital_adoption_all_channels da JOIN mobile_data.app_contact_history mch
    ON da.customer_key = cast(mch.customer_key as double) where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3 --COX_app_total_customers
select da.*,count(wch.customer_key) from digital_adoption_all_channels da JOIN webanalytics.web_contact_history wch
    ON da.customer_key = wch.customer_key where month like '2024-11' and da.customer_key=7.0346127E7 
    group by 1,2,3 --COX_com_total_customers





---- Healthscore Smarthelp containment metric 

SELECT 
    primary_intent_detail,  
    CAST(contact_dt AS DATE) AS contact_dt, 
    COUNT(DISTINCT sub_contact_id) AS sub_contact_id, 
    COUNT(DISTINCT CASE WHEN selfservice_containment = 1 THEN sub_contact_id END) AS contained, 
    CASE 
        WHEN COUNT(DISTINCT sub_contact_id) > 0 THEN
            ROUND(CAST(SUM(CASE WHEN selfservice_containment = 1 THEN 1 ELSE 0 END) AS DOUBLE) 
            / COUNT(DISTINCT sub_contact_id) * 100, 2)
        ELSE
            0
    END AS containment_rate
FROM 
    ota_data_assets_temp.omni_intent_cntct_fact 
WHERE 
    CAST(contact_dt AS DATE) BETWEEN date_add('day', -90, DATE '2024-08-27') AND DATE '2024-08-27' 
    AND primary_intent = 'Equipment Support'
    AND initial_channel = 'CoxApp'
    AND lob = 'R'
    AND primary_intent_detail IN ('PnP', 'SmartHelp')
GROUP BY 
    primary_intent_detail, contact_dt
ORDER BY 
    contact_dt DESC;


---Logic

metric name = containment rate
calc = sum(contained)/sum(sub_contact_id)


-- Database : ota_data_assets_temp

-- Table name: omni_intent_cntct_fact









test_query = """
SELECT 
    primary_intent_detail,  
    CAST(contact_dt AS DATE) AS contact_dt, 
    COUNT(DISTINCT sub_contact_id) AS sub_contact_id, 
    COUNT(DISTINCT CASE WHEN selfservice_containment = 1 THEN sub_contact_id END) AS contained, 
    CASE 
        WHEN COUNT(DISTINCT sub_contact_id) > 0 THEN
            ROUND(CAST(SUM(CASE WHEN selfservice_containment = 1 THEN 1 ELSE 0 END) AS DOUBLE) 
            / COUNT(DISTINCT sub_contact_id) * 100, 2)
        ELSE
            0
    END AS containment_rate
FROM 
    ota_data_assets_temp.omni_intent_cntct_fact 
WHERE 
    CAST(contact_dt AS DATE) BETWEEN date_add(DATE '2024-08-27', -90) AND DATE '2024-08-27' 
    AND primary_intent = 'Equipment Support'
    AND initial_channel = 'CoxApp'
    AND lob = 'R'
    AND primary_intent_detail IN ('PnP', 'SmartHelp')
GROUP BY 
    primary_intent_detail, contact_dt
ORDER BY 
    contact_dt DESC
"""









