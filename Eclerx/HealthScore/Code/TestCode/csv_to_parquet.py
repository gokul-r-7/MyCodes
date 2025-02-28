import os
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("CSV to Parquet Conversion") \
    .getOrCreate()


# Read CSV file into DataFrame
df = spark.read.csv("D:\\shalini\\HS_Omni_Perc.csv", header=True, inferSchema=True)

# Show the DataFrame (optional, for debugging)
df.show()
df.printSchema()
# Write the DataFrame as a Parquet file
df.write.parquet("D:\\shalini\\parquetformat\\outputttttttttfile.parquet", mode="overwrite")

# Stop the Spark session
#
# spark.stop()


