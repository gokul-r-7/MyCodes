import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def list_s3_buckets():
    # Replace these values with your actual credentials
    aws_access_key_id = "ASIA3FLDY5EDPPDGQX5W"
    aws_secret_access_key = "qtidqOJfI0pW2/ehMbgRo0RXqEIoFo84tlqxxDxD"
    aws_session_token = "IQoJb3JpZ2luX2VjEFgaCXVzLWVhc3QtMSJHMEUCIQCT8aXDP1VV1PSqNTkBOi0ov403yG3q1qr0Vi7oCXAOkQIgdgeOkwWA4ftdNvqevVKntIeZhJIpkmTo2e2VI0v2OL8qlAMIQBAAGgw3NjczOTc3ODM4MTQiDE9a1Jkwd7vkiNJ5xCrxAmp07NyY+FaPRw8cv8KWtFo6RdG8NLN2TWI28cRACZyja3B/62/YjR07OGn9wjW5TzsC8Il9qzYFc1yeI6ycGs6M4F+qMK9ReNkdkL7sZaLXBj2wYws2g+M4Umjf07Z65b2X5BbHvQVlv/xMr2Nsu6D+d7Rwm+qmjJ6b9cauYBXnz+W48okqx9fSz9R08QV5UIKnNFd9JidGhGl6E2smgtufKCAFMlM6EAk3CMNSLPRC0yqsahg8kgM+1e2qcJw6PUkMcVGjtqal0qZVD6vluQiPuXVOjCgpDCG0aJ+NWuQ2J56uNnYaQKdDMzDIaJwyIFLLYgSdxyDPlx10IKdWtgNkl6fXXTMIhgYm0ecWUpPKenRyUAAIJ82OfSLB84xi9LNCdY2b7aSRTfjnIlwJwq7c1hqbVxIip2OZu4hTwtOyWPzRK4ijt989dDG0mu8kMCJ4UeosUckBp1AVUbkZ5XBltFiix3gUbmbcI3YW/1+YJjCagu67BjqmAV1jWPOoTxghvMUxVvp822j8MAGGfzL6wPXRM7EwXfWVEOfsVyOPg+31BcURriL6D3FeyKNkDsR+Q346kMLPdc0Y91H59oTTrM0RreVKjDvoPymd5CXvg9BTviPDXQGKmID1ICjRMYgCNxQfRyWTGfmf/kRIhpAu3XoB8AA3OHal3zV80KyRL9VEEjfN51wAdPHsbs1brvgpyKHIUrWuDrIesUELQrs="  # Optional: If using temporary credentials

    try:
        # Initialize a session using the provided credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token  # Add this line only if you're using temporary credentials
        )

        # List the S3 buckets
        response = s3_client.list_buckets()
        
        # Print the names of all the buckets
        print("List of S3 Buckets:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")

    except NoCredentialsError:
        print("No credentials provided.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    list_s3_buckets()






""""
from pyspark.sql import SparkSession
import boto3

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Athena Query") \
    .config("spark.jars", "path-to-your-athena-jdbc-driver/athena-jdbc-driver.jar") \
    .config("spark.hadoop.fs.s3a.access.key", "ASIA3FLDY5EDPPDGQX5W") \
    .config("spark.hadoop.fs.s3a.secret.key", "qtidqOJfI0pW2/ehMbgRo0RXqEIoFo84tlqxxDxD") \
    .config("spark.hadoop.fs.s3a.session.token", "IQoJb3JpZ2luX2VjEFgaCXVzLWVhc3QtMSJHMEUCIQCT8aXDP1VV1PSqNTkBOi0ov403yG3q1qr0Vi7oCXAOkQIgdgeOkwWA4ftdNvqevVKntIeZhJIpkmTo2e2VI0v2OL8qlAMIQBAAGgw3NjczOTc3ODM4MTQiDE9a1Jkwd7vkiNJ5xCrxAmp07NyY+FaPRw8cv8KWtFo6RdG8NLN2TWI28cRACZyja3B/62/YjR07OGn9wjW5TzsC8Il9qzYFc1yeI6ycGs6M4F+qMK9ReNkdkL7sZaLXBj2wYws2g+M4Umjf07Z65b2X5BbHvQVlv/xMr2Nsu6D+d7Rwm+qmjJ6b9cauYBXnz+W48okqx9fSz9R08QV5UIKnNFd9JidGhGl6E2smgtufKCAFMlM6EAk3CMNSLPRC0yqsahg8kgM+1e2qcJw6PUkMcVGjtqal0qZVD6vluQiPuXVOjCgpDCG0aJ+NWuQ2J56uNnYaQKdDMzDIaJwyIFLLYgSdxyDPlx10IKdWtgNkl6fXXTMIhgYm0ecWUpPKenRyUAAIJ82OfSLB84xi9LNCdY2b7aSRTfjnIlwJwq7c1hqbVxIip2OZu4hTwtOyWPzRK4ijt989dDG0mu8kMCJ4UeosUckBp1AVUbkZ5XBltFiix3gUbmbcI3YW/1+YJjCagu67BjqmAV1jWPOoTxghvMUxVvp822j8MAGGfzL6wPXRM7EwXfWVEOfsVyOPg+31BcURriL6D3FeyKNkDsR+Q346kMLPdc0Y91H59oTTrM0RreVKjDvoPymd5CXvg9BTviPDXQGKmID1ICjRMYgCNxQfRyWTGfmf/kRIhpAu3XoB8AA3OHal3zV80KyRL9VEEjfN51wAdPHsbs1brvgpyKHIUrWuDrIesUELQrs=") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Execute your Athena query
athena_query = SELECT * FROM healthscore.healthscore_master

# To read from Athena, use the spark.sql method
df = spark.sql(athena_query)

# Show the results
df.show()
"""