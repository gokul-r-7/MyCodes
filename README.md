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









