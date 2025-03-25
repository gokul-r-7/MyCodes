Old Framework

1. CloudWatch triggers Lambda Function, which fetches Process IDs from the Config table to start the Glue job.
2. Glue job calls the Adobe API, loads the JOBID_DATE.Done file into S3, and updates job status in audit/config tables.
3. Lambda Fucntion retrieves Job_ID from Config table and triggers the next Glue job for further transformations.
4. Glue job fetches data from S3 and performs necessary transformations & write it into target s3 paths
5. API credentials are stored in Secrets Manager, with SNS notifications for failed jobs and CloudWatch logs for Glue and Lambda.
