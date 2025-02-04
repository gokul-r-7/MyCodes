import pandas as pd
import numpy as np

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Athena Spark SQL Example") \
    .config("spark.jars", "path/to/athena-jdbc-driver.jar") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:awsathena://AwsRegion=your-region;S3OutputLocation=s3://your-s3-bucket/path/"
connection_properties = {
    "user": "YOUR_AWS_ACCESS_KEY",
    "password": "YOUR_AWS_SECRET_KEY",
}

# Define the query
query = "SELECT * FROM your_database.your_table"

# Read from Athena using Spark
df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS subquery", properties=connection_properties)

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
