# Iceberg to Aurora Data load.
## 1. What the Glue Script Does

This Glue script performs an incremental data load from an Apache Iceberg table to an Amazon Aurora PostgreSQL database. It compares the current state of the Iceberg table with the last processed snapshot ID and identifies new, modified, and deleted rows. Based on the identified changes, it performs the necessary inserts, updates, and deletes in the target Aurora database table.

## 2. Main Features

- **Incremental Data Load**: The script performs an incremental data load instead of a full overwrite, reducing the processing time and resource consumption.
- **Identification of Changes**: It identifies new, modified, and deleted rows in the Iceberg table since the last processed snapshot.
- **Handling Different Operations**: The script handles inserts, updates, and deletes in the target Aurora database table based on the identified changes.
- **Primary Key Support**: The script supports identifying the primary key column automatically or through configuration.
- **Logging**: The script includes logging capabilities to track the progress and any errors that occur during the data load process.
- **Error Handling**: Basic error handling is implemented, and transactions are used to ensure data consistency during updates and deletes.
- **Configuration via DynamoDB**: The script retrieves configuration parameters from a DynamoDB table, allowing for easy management and updates.

## 3. Required Parameters in DynamoDB

The script expects the following parameters to be configured in the specified DynamoDB table:

- `target.iceberg_properties.iceberg_configuration.iceberg_catalog_warehouse`: The Iceberg warehouse location in Amazon S3.
- `target.iceberg_properties.database_name`: The name of the Iceberg database.
- `target.iceberg_properties.table_name`: The name of the Iceberg table.
- `target.db_load.aurora_host`: The hostname of the Aurora PostgreSQL instance.
- `target.db_load.aurora_port`: The port number of the Aurora PostgreSQL instance.
- `target.db_load.aurora_secret`: The name of the AWS Secrets Manager secret containing the Aurora PostgreSQL username and password.
- `target.db_load.aurora_db_name`: The name of the Aurora PostgreSQL database.
- `target.db_load.aurora_db_target_table_name`: The name of the target table in the Aurora PostgreSQL database.
- `target.db_load.aurora_db_target_schema`: The schema name of the target table in the Aurora PostgreSQL database.
- `target.db_load.aurora_data_load_type`: The data load type (e.g., incremental, full).
- `target.db_load.snapshot_s3_bucket`: The S3 bucket name to store the last processed snapshot ID.
- `target.db_load.snapshot_s3_key`: The S3 key to store the last processed snapshot ID.
- `target.db_load.snapshot_s3_kms_key_id`: The KMS key ID for encrypting the snapshot ID in S3 (optional).
- `target.db_load.primary_key`: The name of the primary key column (optional).

## 4. How to Invoke the Glue Job

The Glue job can be invoked through the AWS Glue console, AWS CLI, or programmatically using the AWS SDK. When invoking the job, you need to provide the following parameters:

- `JOB_NAME`: The name of the Glue job.
- `metadata_table_name`: The name of the DynamoDB table containing the configuration parameters.
- `source_system_id`: The ID of the source system for which the data load is being performed.
- `metadata_type`: The type of metadata (e.g., incremental, full).

## 5. How to Add or Remove Features

To add or remove features, you can modify the Glue script directly. Here are some examples of potential enhancements:

| Note:
   If you prefer a full overwrite (for a new load or schema changes) instead of an incremental load, you can either remove the logic related to identifying changes (new, modified, deleted rows) and perform a full overwrite of the target Aurora database table or remove the reference of target.db_load.snapshot_s3_key.

## Future features to consider:
- **Partition Support**: Implement support for partitioned Iceberg tables by modifying the data loading logic accordingly.
- **Schema Evolution**: Add support for handling schema changes in the Iceberg table and propagating them to the target Aurora database table.