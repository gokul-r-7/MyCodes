
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
 
glueContext._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
spark._jsc.hadoopConfiguration().set("fs.s3.useRequesterPaysHeader","true") ## this is needed for permissions
 
spark = glueContext.spark_session
spark.catalog.clearCache()
job = Job(glueContext)
dyf = glueContext.create_dynamic_frame.from_catalog(database='mobile_data', table_name='app_contact_history')
dyf.printSchema()
import sys
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when, round, countDistinct, to_date, date_add, lit, sum
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
demoquery = """select * from mobile_data.app_contact_history limit 10"""
demodf = spark.sql(demoquery)
demodf.show()
account_dim_sum_new_1 = """
WITH customer_data AS (
    SELECT distinct c.customer_key,
           c.account_nbr,
           c.site_id,
           c.res_comm_ind,
           c.customer_status_cd,
           c.account_guid,
           c.prim_account_holder_guid,
           c.test_account_key AS test_account_key,
           c.inception_dt
    FROM edw.customer_dim c
    WHERE test_account_key=2
),
revenue_data AS (
    SELECT distinct r.customer_key,
           r.site_id,
           r.dwelling_type_key,
           r.easy_pay_flag,
           CASE WHEN r.mobile_gross_mrc > 0 THEN '1' ELSE '0' END AS Mobile_Flag,
           r.customer_substatus_key,
           r.time_key
    FROM edw.customer_revenue_fact r
    WHERE r.bill_type_key != 2 
      AND to_date(r.time_key, 'yyyy-MM-dd') >= add_months(CURRENT_DATE, -1)
),
customer_status AS (
    SELECT distinct customer_substatus_key, sub_status_desc 
    FROM edw.customer_substatus_dim
),
dwelling_data AS (
    SELECT distinct d.dwelling_type_key,
           d.dwelling_type_desc
    FROM edw.dwelling_type_dim d
),
account_summary AS (
    SELECT s.customer_key,
           s.employee_flag,
           s.data_flag,
           s.site_id,
           s.cable_flag,
           s.wireless_flag,
           s.do_not_call_flag,
           s.do_not_email_flag,
           s.do_not_mail_flag,
           s.telephony_flag ,
           s.do_not_market_flag,
           s.time_key,
           ROW_NUMBER() OVER (PARTITION BY s.customer_key ORDER BY s.time_key DESC) AS rn
    FROM edw.cust_acct_sum s
),
guid_data AS (
    SELECT g.customer_key,
           g.create_dt,
           g.household_member_guid,
           ROW_NUMBER() OVER (PARTITION BY g.customer_key ORDER BY g.create_dt ASC) AS rn
    FROM edw.customer_guid_dtl_dim g
),
account_details AS (
    SELECT distinct a.customer_key,
           ac.do_not_email AS Email_Opt_Out,
           ac.do_not_call AS Phone_Opt_Out,
           em.email_address,
           ROW_NUMBER() OVER (PARTITION BY a.customer_key ORDER BY ac.do_not_email) AS rn
    FROM edw.customer_dim a 
    LEFT JOIN camp_mgmt.accounts ac ON a.account_nbr = ac.account_nbr
    LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
),
notification_flags AS (
    SELECT distinct a.customer_key,
    MAX(CASE WHEN cmmeth = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Notification_Email_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' THEN 'Y' ELSE 'N' END) AS Notification_Phone_Flag,
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
                WHEN cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS phone_verified_date

FROM edw.customer_dim a 
LEFT JOIN pstage.stg_all_cust_notification_methods n ON a.account_nbr=n.cmcnbr
GROUP BY a.customer_key
),
web_contact AS (
    SELECT DISTINCT
        campaign,
        evar61_coxcust_guid,
        ROW_NUMBER() OVER (PARTITION BY evar61_coxcust_guid ORDER BY MIN(dt)) AS rn
    FROM webanalytics.web_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM webanalytics.web_contact_history
        WHERE pagename = 'cox:res:myprofile:reg:confirmation')
    GROUP BY campaign, evar61_coxcust_guid
),
app_contact AS (
    SELECT DISTINCT
        coxcust_guid_v61,
        post_evar40,
        ROW_NUMBER() OVER (PARTITION BY coxcust_guid_v61 ORDER BY MIN(dt)) AS rn
    FROM mobile_data_temp.app_contact_history
    WHERE visits IN (
        SELECT DISTINCT visits
        FROM mobile_data_temp.app_contact_history
        WHERE pagename = 'coxapp:reg:confirmation')
    GROUP BY coxcust_guid_v61, post_evar40
),
ivr_contact AS (
    SELECT distinct 
        i.customer_key,
        DATE_FORMAT(CAST(i.time_key AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(i.time_key AS DATE)) AS Last_Contacted_Date_IVR_Call
    FROM `call`.call_ivr_fact i
    GROUP BY i.customer_key, DATE_FORMAT(CAST(i.time_key AS DATE), 'yyyy-MM')
),
web_data AS (
    SELECT distinct
        d.customer_key,
        DATE_FORMAT(CAST(d.dt AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(d.dt AS DATE)) AS Last_Contacted_Date_Cox_com
    FROM webanalytics.web_contact_history d
    GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS DATE), 'yyyy-MM')
),
mob_data AS (
    SELECT distinct
        mob.customer_key,
        DATE_FORMAT(CAST(mob.dt AS DATE), 'yyyy-MM') AS Contact_Month,
        MAX(CAST(mob.dt AS DATE)) AS Last_Contacted_Date_Cox_App
    FROM mobile_data_temp.app_contact_history mob
    GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS DATE), 'yyyy-MM')
)
SELECT distinct
    r.customer_key AS Customer_Key,
    c.account_nbr AS Account_Nbr,
    r.site_id AS Site_Id,
    c.res_comm_ind AS Res_Com_Ind,
    c.customer_status_cd AS Customer_Status,
    d.dwelling_type_desc AS House_Type,
    c.account_guid AS Account_GUID,
    c.prim_account_holder_guid AS User_GUID_Primary,
    s.employee_flag AS Employee_Flag,
    c.test_account_key AS Test_Account_Flag,
    c.inception_dt AS Inception_Date,
    CAST(NULL AS varchar(255)) AS Sale_Acquisition_Channel,
    g.create_dt AS Registration_Date,
    COALESCE(web.campaign, app.post_evar40) AS Registration_Traffic_Source_Detail,
    CASE
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_cns_ocall_event255' THEN 'Email-Order'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em_z_acct_onb%' THEN 'Email-Onboarding'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_em%' THEN 'Email-Others'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_sms%' THEN 'sms'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE 'cr_dm%' THEN 'direct mail'
        WHEN SUBSTRING(LOWER(COALESCE(web.campaign, app.post_evar40)), LENGTH(COALESCE(web.campaign, app.post_evar40)) - 5,6) = 'vanity' THEN 'Vanity URL'
        WHEN LOWER(COALESCE(web.campaign, app.post_evar40)) LIKE '%panoapp%' THEN 'panoapp'
        WHEN COALESCE(web.campaign, app.post_evar40) IS NOT NULL AND LENGTH(COALESCE(web.campaign, app.post_evar40)) > 0 THEN 'organic'
        ELSE 'null'  
    END AS Registration_Traffic_Source,
    s.data_flag AS Data_Flag,
    s.cable_flag AS TV_Flag,
    s.telephony_flag AS Phone_Flag,
    CAST(NULL AS varchar(255)) AS Homelife_Flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    COUNT(DISTINCT ad.email_address) AS email_count,
    nf.Notification_Email_Flag,
    nf.Notification_Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,
    CAST(NULL AS varchar(255)) AS Pano_Flag,
    CAST(NULL AS varchar(255)) AS Pano_Device,
    r.easy_pay_flag AS Easy_Pay_Flag,
    s.do_not_call_flag AS Do_Not_Call_Flag,
    s.do_not_email_flag AS Do_Not_Email_Flag,
    s.do_not_mail_flag AS Do_Not_Mail_Flag,
    s.do_not_market_flag AS Do_Not_Market_Flag,
    MAX(ivr.Last_Contacted_Date_IVR_Call) AS Last_Contacted_Date_IVR_Call,
    MAX(w.Last_Contacted_Date_Cox_com) AS Last_Contacted_Date_Cox_com,
    MAX(mob.Last_Contacted_Date_Cox_App) AS Last_Contacted_Date_Cox_App,
    CAST(NULL AS varchar(255)) AS Cox_Segment,
    CAST(NULL AS varchar(255)) AS Demographic_Info1,
    CAST(NULL AS varchar(255)) AS Demographic_Info2,
    r.time_key
FROM revenue_data r 
LEFT JOIN dwelling_data d ON r.dwelling_type_key = d.dwelling_type_key
LEFT JOIN (SELECT * FROM account_summary WHERE rn=1) s ON r.customer_key = CAST(s.customer_key AS double)
LEFT JOIN customer_status cd ON r.customer_substatus_key = cd.customer_substatus_key 
LEFT JOIN customer_data c ON r.customer_key = CAST(c.customer_key AS double)
LEFT JOIN (SELECT * FROM account_details WHERE rn=1) ad ON r.customer_key = ad.customer_key
LEFT JOIN notification_flags nf ON r.customer_key = nf.customer_key
LEFT JOIN (SELECT * FROM guid_data WHERE rn=1) g ON r.customer_key = CAST(g.customer_key AS double)
LEFT JOIN (SELECT * FROM web_contact WHERE rn=1) web ON g.household_member_guid = web.evar61_coxcust_guid
LEFT JOIN (SELECT * FROM app_contact WHERE rn=1) app ON g.household_member_guid = app.coxcust_guid_v61
LEFT JOIN ivr_contact ivr ON r.customer_key = CAST(ivr.customer_key AS double) AND ivr.Contact_Month <= r.time_key
LEFT JOIN web_data w ON r.customer_key = CAST(w.customer_key AS double) AND w.Contact_Month <= r.time_key
LEFT JOIN mob_data mob ON r.customer_key = CAST(mob.customer_key AS double) AND mob.Contact_Month <= r.time_key
GROUP BY
    r.customer_key,
    c.account_nbr,
    r.site_id,
    c.res_comm_ind,
    c.customer_status_cd,
    d.dwelling_type_desc,
    c.account_guid,
    c.prim_account_holder_guid,
    s.employee_flag,
    c.test_account_key,
    c.inception_dt,
    g.create_dt,
    COALESCE(web.campaign, app.post_evar40),
    s.data_flag,
    s.cable_flag,
    s.telephony_flag,
    r.Mobile_Flag,
    cd.sub_status_desc,
    nf.Notification_Email_Flag,
    nf.Notification_Phone_Flag,
    nf.Email_Verified_Flag,
    nf.Phone_Verified_Flag,
    nf.Email_Verified_Date,
    nf.Phone_Verified_Date,
    ad.Email_Opt_Out, 
    ad.Phone_Opt_Out,
    r.easy_pay_flag,
    s.do_not_call_flag,
    s.do_not_email_flag,
    s.do_not_mail_flag,
    s.do_not_market_flag,
    r.time_key
"""

account_dim_sum_new_1_df = spark.sql(account_dim_sum_new_1)
account_dim_sum_new_1_df.show()
account_dim_sum_new_1_df.count()
account_dim_sum_new_1_df.persist(StorageLevel.MEMORY_AND_DISK)
account_dim_sum_new_1_df.printSchema()
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_data AS
SELECT DISTINCT 
    c.customer_key,
    c.account_nbr,
    c.site_id,
    c.res_comm_ind,
    c.customer_status_cd,
    c.account_guid,
    c.prim_account_holder_guid,
    c.test_account_key AS test_account_key,
    c.inception_dt
FROM edw.customer_dim c
WHERE test_account_key=2
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW revenue_data AS
SELECT DISTINCT 
    r.customer_key,
    r.site_id,
    r.dwelling_type_key,
    r.easy_pay_flag,
    CASE WHEN r.mobile_gross_mrc > 0 THEN '1' ELSE '0' END AS Mobile_Flag,
    r.customer_substatus_key,
    r.time_key
FROM edw.customer_revenue_fact r
WHERE r.bill_type_key != 2 
    AND to_date(r.time_key, 'yyyy-MM-dd') >= add_months(CURRENT_DATE, -1)
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_status AS
SELECT DISTINCT 
    customer_substatus_key, 
    sub_status_desc 
FROM edw.customer_substatus_dim
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW dwelling_data AS
SELECT DISTINCT 
    d.dwelling_type_key,
    d.dwelling_type_desc
FROM edw.dwelling_type_dim d
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW account_summary AS
SELECT 
    s.customer_key,
    s.employee_flag,
    s.data_flag,
    s.site_id,
    s.cable_flag,
    s.wireless_flag,
    s.do_not_call_flag,
    s.do_not_email_flag,
    s.do_not_mail_flag,
    s.telephony_flag,
    s.do_not_market_flag,
    s.time_key,
    ROW_NUMBER() OVER (PARTITION BY s.customer_key ORDER BY s.time_key DESC) AS rn
FROM edw.cust_acct_sum s
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW guid_data AS
SELECT 
    g.customer_key,
    g.create_dt,
    g.household_member_guid,
    ROW_NUMBER() OVER (PARTITION BY g.customer_key ORDER BY g.create_dt ASC) AS rn
FROM edw.customer_guid_dtl_dim g
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW account_details AS
SELECT DISTINCT 
    a.customer_key,
    ac.do_not_email AS Email_Opt_Out,
    ac.do_not_call AS Phone_Opt_Out,
    em.email_address,
    ROW_NUMBER() OVER (PARTITION BY a.customer_key ORDER BY ac.do_not_email) AS rn
FROM edw.customer_dim a 
LEFT JOIN camp_mgmt.accounts ac ON a.account_nbr = ac.account_nbr
LEFT JOIN camp_mgmt.email_addresses em ON em.vndr_cust_account_key = ac.vndr_cust_account_key
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW notification_flags AS
SELECT
    distinct a.customer_key,
    MAX(CASE WHEN cmmeth = 'EMAIL' THEN 'Y' ELSE 'N' END) AS Notification_Email_Flag,
    MAX(CASE WHEN cmmeth = 'PHONE' THEN 'Y' ELSE 'N' END) AS Notification_Phone_Flag,
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
                WHEN cmcfdt != 0 THEN 
                    CONCAT('20', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 2, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 4, 2), '-', 
                        SUBSTRING(CAST(cmcfdt AS STRING), 6, 2)
                    )
                ELSE NULL 
            END
    END) AS phone_verified_date

FROM edw.customer_dim a 
LEFT JOIN pstage.stg_all_cust_notification_methods n ON a.account_nbr=n.cmcnbr
GROUP BY a.customer_key
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW web_contact AS
SELECT DISTINCT
    campaign,
    evar61_coxcust_guid,
    ROW_NUMBER() OVER (PARTITION BY evar61_coxcust_guid ORDER BY MIN(dt)) AS rn
FROM webanalytics.web_contact_history
WHERE visits IN (
    SELECT DISTINCT visits
    FROM webanalytics.web_contact_history
    WHERE pagename = 'cox:res:myprofile:reg:confirmation')
GROUP BY campaign, evar61_coxcust_guid
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW app_contact AS
SELECT DISTINCT
    coxcust_guid_v61,
    post_evar40,
    ROW_NUMBER() OVER (PARTITION BY coxcust_guid_v61 ORDER BY MIN(dt)) AS rn
FROM mobile_data_temp.app_contact_history
WHERE visits IN (
    SELECT DISTINCT visits
    FROM mobile_data_temp.app_contact_history
    WHERE pagename = 'coxapp:reg:confirmation')
GROUP BY coxcust_guid_v61, post_evar40
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW ivr_contact AS
SELECT DISTINCT 
    i.customer_key,
    DATE_FORMAT(CAST(i.time_key AS DATE), '%Y-%m') AS Contact_Month,
    MAX(CAST(i.time_key AS DATE)) AS Last_Contacted_Date_IVR_Call
FROM `call`.call_ivr_fact i
GROUP BY i.customer_key, DATE_FORMAT(CAST(i.time_key AS DATE), '%Y-%m')
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW web_data AS
SELECT DISTINCT
    d.customer_key,
    DATE_FORMAT(CAST(d.dt AS DATE), '%Y-%m') AS Contact_Month,
    MAX(CAST(d.dt AS DATE)) AS Last_Contacted_Date_Cox_com
FROM webanalytics.web_contact_history d
GROUP BY d.customer_key, DATE_FORMAT(CAST(d.dt AS DATE), '%Y-%m')
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW mob_data AS
SELECT DISTINCT
    mob.customer_key,
    DATE_FORMAT(CAST(mob.dt AS DATE), '%Y-%m') AS Contact_Month,
    MAX(CAST(mob.dt AS DATE)) AS Last_Contacted_Date_Cox_App
FROM mobile_data_temp.app_contact_history mob
GROUP BY mob.customer_key, DATE_FORMAT(CAST(mob.dt AS DATE), '%Y-%m')
""")
df = dyf.toDF()
df.show()
import matplotlib.pyplot as plt

# Set X-axis and Y-axis values
x = [5, 2, 8, 4, 9]
y = [10, 4, 8, 5, 2]
  
# Create a bar chart 
plt.bar(x, y)
  
# Show the plot
sample_query1 = """
select da.customer_key, da.customer_type, da.month, count(ivr.customer_key) as ivr_total_customer_count
from `ciam_datamodel`.`digital_adoption_cox_app_and_ivr_table` da 
JOIN `call`.`call_ivr_fact` ivr
ON da.customer_key = ivr.customer_key
group by 1, 2, 3
"""
sample_query2 = """
select da.customer_key, da.customer_type, da.month, count(ivr.customer_key) as ivr_total_customer_count
from `ciam_datamodel`.`digital_adoption_cox_com_and_ivr_table` da 
JOIN `call`.`call_ivr_fact` ivr
ON da.customer_key = ivr.customer_key
group by 1, 2, 3
"""
sample_query11 = """
select da.customer_key, da.customer_type, da.month,count(mch.customer_key) as mch_total_customer_count from ciam_datamodel.digital_adoption_cox_app_and_ivr_table da JOIN mobile_data.app_contact_history mch
    ON da.customer_key = cast(mch.customer_key as double) 
    group by 1,2,3 --COX_app_total_customers
"""
sample_query22 = """
select da.customer_key, da.customer_type, da.month,count(mch.customer_key) as mch_total_customer_count from ciam_datamodel.digital_adoption_cox_com_and_ivr_table da JOIN mobile_data.app_contact_history mch
    ON da.customer_key = cast(mch.customer_key as double) 
    group by 1,2,3 --COX_app_total_customers
"""
sample_query33 = """
select da.customer_key, da.customer_type, da.month,count(mch.customer_key) as mch_total_customer_count from ciam_datamodel.digital_adoption_all_channels_table da JOIN mobile_data.app_contact_history mch
    ON da.customer_key = cast(mch.customer_key as double) 
    group by 1,2,3 --COX_app_total_customers
"""
sample_query111 = """
select da.customer_key, da.customer_type, da.month, count(wch.customer_key) as wch_total_customer_count from ciam_datamodel.digital_adoption_cox_app_and_ivr_table da JOIN webanalytics.web_contact_history wch
    ON da.customer_key = wch.customer_key
    group by 1,2,3 --COX_com_total_customers
"""
sample_query222 = """
select da.customer_key, da.customer_type, da.month, count(wch.customer_key) as wch_total_customer_count from ciam_datamodel.digital_adoption_cox_com_and_ivr_table da JOIN webanalytics.web_contact_history wch
    ON da.customer_key = wch.customer_key
    group by 1,2,3 --COX_com_total_customers
"""
df1 = spark.sql(sample_query33)
df1.show(5)
def write_to_s3(df,output_path,partitionkey):
    write_df = df.write \
    .partitionBy(partitionkey) \
    .format("parquet") \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .save(output_path)
    return write_df
output_s3_path = "s3://cci-dig-aicoe-data-sb/processed/digital_adoption_mch/"
write_df = write_to_s3(df1,output_s3_path,"month")
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()