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

























from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round, countDistinct, date_add, to_date

# Initialize Spark session (if not already initialized)
spark = SparkSession.builder \
    .appName("AthenaQueryExample") \
    .getOrCreate()

# Read the data from the table into a DataFrame (assuming it's a Hive table or a data source in your environment)
df = spark.read.table("ota_data_assets_temp.omni_intent_cntct_fact")

# Apply the filters
start_date = '2024-05-29'  # Adjusted start date as per the SQL query
end_date = '2024-08-27'  # End date

filtered_df = df.filter(
    (to_date(col('contact_dt')) >= date_add(to_date(lit(end_date)), -90)) & 
    (to_date(col('contact_dt')) <= to_date(lit(end_date))) &
    (col('primary_intent') == 'Equipment Support') &
    (col('initial_channel') == 'CoxApp') &
    (col('lob') == 'R') &
    (col('primary_intent_detail').isin('PnP', 'SmartHelp'))
)

# Calculate the required columns as per the SQL query
result_df = filtered_df.groupBy("primary_intent_detail", "contact_dt").agg(
    countDistinct("sub_contact_id").alias("sub_contact_id"),
    countDistinct(when(col('selfservice_containment') == 1, col('sub_contact_id'))).alias("contained"),
    # Calculating containment rate
    round(
        when(countDistinct("sub_contact_id") > 0,
             (sum(when(col('selfservice_containment') == 1, 1).otherwise(0)) / countDistinct("sub_contact_id")) * 100)
        .otherwise(0), 2).alias("containment_rate")
)

# Show the final result
result_df.orderBy(col("contact_dt").desc()).show()









import sys
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when, round, countDistinct, to_date, date_add, lit
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize GlueContext
sc = SparkContext()
sqlContext = SQLContext(sc)
glueContext = GlueContext(sc)

# Set the dynamic frame
spark = glueContext.spark_session

# Read from the Glue catalog table (assuming you have a catalog entry for the table)
# Replace `database_name` and `table_name` with your actual Glue database and table names
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="your_database_name", 
    table_name="omni_intent_cntct_fact"
)

# Convert to DataFrame for SQL operations
df = dynamic_frame.toDF()

# Apply the filters, similar to the SQL query in PySpark
start_date = '2024-05-29'  # Adjusted start date as per the SQL query
end_date = '2024-08-27'  # End date

filtered_df = df.filter(
    (to_date(col('contact_dt')) >= date_add(to_date(lit(end_date)), -90)) & 
    (to_date(col('contact_dt')) <= to_date(lit(end_date))) &
    (col('primary_intent') == 'Equipment Support') &
    (col('initial_channel') == 'CoxApp') &
    (col('lob') == 'R') &
    (col('primary_intent_detail').isin('PnP', 'SmartHelp'))
)

# Perform the group by and aggregations
result_df = filtered_df.groupBy("primary_intent_detail", "contact_dt").agg(
    countDistinct("sub_contact_id").alias("sub_contact_id"),
    countDistinct(when(col('selfservice_containment') == 1, col('sub_contact_id'))).alias("contained"),
    # Calculating containment rate
    round(
        when(countDistinct("sub_contact_id") > 0,
             (sum(when(col('selfservice_containment') == 1, 1).otherwise(0)) / countDistinct("sub_contact_id")) * 100)
        .otherwise(0), 2).alias("containment_rate")
)

# Convert back to Glue DynamicFrame
final_dynamic_frame = DynamicFrame.fromDF(result_df, glueContext, "final_dynamic_frame")

# Write the result to S3 or any other destination you want
# Replace with the correct S3 path or destination
output_path = "s3://your-bucket-name/output/results/"
glueContext.write_dynamic_frame.from_options(
    final_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"  # You can also use other formats like "csv" or "json"
)

# Optionally, log completion
print("Glue job completed successfully.")









