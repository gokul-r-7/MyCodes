from pyspark.sql import SparkSession

# Create Spark session with the Athena JDBC driver
spark = SparkSession.builder \
    .appName("Athena JDBC Example") \
    .config("spark.jars", "path/to/athena-jdbc-driver.jar") \
    .getOrCreate()

# Define the JDBC URL
jdbc_url = "jdbc:awsathena://AwsRegion=us-east-1;S3OutputLocation=s3://my-athena-results/;UID=YOUR_AWS_ACCESS_KEY;PWD=YOUR_AWS_SECRET_KEY;SessionToken=YOUR_AWS_SESSION_TOKEN;"

# Define your query
query = "SELECT * FROM your_database.your_table"

# Read from Athena using Spark
df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS subquery", properties={"driver": "com.simba.athena.jdbc.Driver"})

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()
