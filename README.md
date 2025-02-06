select da.*,count(ivr.customer_key) from digital_adoption_all_channels da JOIN "call"."call_ivr_fact" ivr

    ON da.customer_key = ivr.customer_key where month like '2024-11' and --da.customer_key=7.0346127E7 

    group by 1,2,3
 
