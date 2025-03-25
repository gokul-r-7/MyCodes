Here’s a more concise and technical version of the architecture description:

1. **Job Triggers**: Historical jobs are triggered on-demand, while incremental jobs are scheduled to run at defined intervals.
2. **Configuration Files**: Both jobs reference configuration files containing parameter lists for Adobe API calls.
3. **API Integration**: Python module establishes connection to Adobe API, retrieves data, and writes it to S3.
4. **Data Transformation**: Adobe data is retrieved, transformed via Glue jobs, and written to designated S3 paths.
5. **Logging & Monitoring**: CloudWatch monitors and logs activity for Glue jobs and Lambda functions..
![image](https://github.com/user-attachments/assets/7cd4c642-cef3-4ee6-af8f-c8c7b3b50519)
