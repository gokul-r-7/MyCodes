import sys
from pyspark.sql import SparkSession

# Initialize Spark session for EMR
spark = SparkSession.builder \
    .appName("EMR Spark Job") \
    .getOrCreate()

# Define the input and output S3 paths
csv_path = "s3://test-gokul-220797/CSVFiles/cb_adoption_type_merge.csv"
parquet_path = "s3://test-gokul-220797/Parquet_files/"

# Read the CSV file
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Show the DataFrame and its schema
df.show()
df.printSchema()

# Write the DataFrame to Parquet format in the specified S3 path
df.write.format("parquet").mode("append").save(parquet_path)

# Stop the Spark session
spark.stop()
