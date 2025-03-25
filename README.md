Historical job is designed & run based on On demand trigger
Incremental job is designed & run based on scheduled time
Both the jobs has a config file, it has written some lists of items to pass while calling Adobe API.
Python module used to make Adobe API connection, get date & write data to s3
Both the Glue jobs calling the API by passing the parameters used in config file
Retrieving the Adobe data by applying various transformations .
Transformed data is written to target s3 path & created Hive table directly in athena.
Cloud Watch stores logs for both the Glue jobs and the lambda functions.
![image](https://github.com/user-attachments/assets/7cd4c642-cef3-4ee6-af8f-c8c7b3b50519)
