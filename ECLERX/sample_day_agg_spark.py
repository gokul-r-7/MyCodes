from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, date_add, current_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AthenaQueryReplication") \
    .enableHiveSupport()  # Enable Glue Catalog support in EMR
    .getOrCreate()

# Read the Athena tables using Glue Catalog

# 1. filtered_auth - Reading the successful_authentications_okta table from Athena via Glue Catalog
filtered_auth_df = spark.read.table("ciam.successful_authentications_okta") \
    .filter(
        (col("event_date") >= date_add(current_date(), -365))
    ) \
    .select(
        col("actor_alternateid").alias("user_id"),
        col("event_date").cast("date").alias("event_date"),
        col("host"),
        col("uuid"),
        col("outcome_result")
    )

# 2. filtered_signon - Reading the single_signon_okta table from Athena via Glue Catalog
filtered_signon_df = spark.read.table("ciam.single_signon_okta") \
    .filter(
        (col("event_date") >= date_add(current_date(), -365))
    ) \
    .select(
        col("username").alias("user_id"),
        col("application"),
        col("event_date").cast("date").alias("event_date")
    )

# 3. customer_guid_dtl_dim - Reading the customer_guid_dtl_dim table from Athena via Glue Catalog
customer_guid_dtl_dim_df = spark.read.table("edw.customer_guid_dtl_dim") \
    .select("user_id")

# 4. mfa_total_mfa_users - Reading the mfa_total_mfa_users table from Athena via Glue Catalog
mfa_total_mfa_users_df = spark.read.table("ciam.mfa_total_mfa_users") \
    .select(
        col("username"),
        col("eventtype")
    )

# Step 1: Aggregating filtered_auth_df - Authentication attempts and successes
aggregated_auth_df = filtered_auth_df.groupBy(
    "user_id", "event_date", "host"
).agg(
    count("uuid").alias("Authentication_Attempt"),
    count(when(col("outcome_result") == "SUCCESS", col("uuid")).otherwise(None)).alias("authentication_success_result")
)

# Step 2: Aggregating filtered_signon_df
aggregated_signon_df = filtered_signon_df.groupBy(
    "user_id", "event_date", "application"
).agg(
    lit(None).alias("Authentication_Error"),  # Placeholder for Authentication_Error
    lit("Login Credentials").alias("Authentication_method")
)

# Step 3: Perform the Joins

# Join aggregated_auth_df with customer_guid_dtl_dim_df
join_df = aggregated_auth_df.join(
    customer_guid_dtl_dim_df, aggregated_auth_df.user_id == customer_guid_dtl_dim_df.user_id, "left"
)

# Join the result with aggregated_signon_df
join_df = join_df.join(
    aggregated_signon_df, (customer_guid_dtl_dim_df.user_id == aggregated_signon_df.user_id) &
                           (aggregated_auth_df.event_date == aggregated_signon_df.event_date), "left"
)

# Join with mfa_total_mfa_users_df
join_df = join_df.join(
    mfa_total_mfa_users_df, customer_guid_dtl_dim_df.user_id == mfa_total_mfa_users_df.username, "left"
)

# Step 4: Add Activity_type and Final Transformations

# Add Activity_type based on eventtype
final_df = join_df.withColumn(
    "Activity_type", when(col("eventtype") == "group.user_membership.add", lit("tsv enrolled")).otherwise(lit(None))
)

# Select the final columns as per the query
final_df = final_df.select(
    col("event_date").alias("Event_Date"),
    col("host").alias("Authentication_Host"),
    col("application").alias("Authentication_Channel"),
    col("Authentication_Attempt"),
    col("authentication_success_result"),
    col("Authentication_Error"),
    col("Authentication_method"),
    col("Activity_type")
)

# Show the final result (can be replaced with write to S3 or any other destination)
final_df.show()

# Optionally, write the result to S3 (Parquet or CSV)
# final_df.write.format("parquet").save("s3://your-bucket/output/")
# final_df.write.format("csv").save("s3://your-bucket/output/")
