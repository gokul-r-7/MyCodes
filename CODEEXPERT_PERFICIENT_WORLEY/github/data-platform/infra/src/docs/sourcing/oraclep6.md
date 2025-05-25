# Overview of Oracle P6

Oracle P6 is an API based datasource. Oracle P6 has 3 API Endpoints: Extract API, Activity Spread API and the Resource Spread API. All the 3 APIs are Auth protected. The OAuth endpoint expects a base64 encoded username and password that is stored in the Secrets Manager. This operation is ClickOps and is not part of Terraform automation. The OAuth endpoint returns a token which is then used by the 3 APIs. The Oracle P6 endpoint is scoped to different projects, such as VG, DAC, etc. The Extract API returns XML payload and the Spread APIs return JSON payload.

The Extract API is a HTTP PUT API and expects a JSON body payload. The payload is maintained on a S3 bucket: `<bucketname>/config/oracle_p6/P6ProjectExportInput.json`. The payload needs to be uploaded to the S3 bucket via ClickOps.

Each project (such as projects in VG or DAC) can have multiple Project Ids. The orchestration of project Ids is managed by Airflow DAG. The Project Ids are mantained in a dynamodb table (also referred to as metadata framework).

The Oracle P6 Airflow DAG initially invokes the Extract API Gluejob that parses the XML payload and tokenizes various nodes within the XML payload. Each node translates to an entity (also a table in the Glue Catalogue). Project, ActivityCode etc are some examples of the XML nodes. The XML nodes that need to be parsed and converted to Parquet format are defined in the raw layer metadata configuration. Once the Extract API is executed successfully, the DAG trigers the Spread API GlueJob (Activity and Resource Assignment) concurrently. These Spread APIs receive JSON payload as input and the GlueJob converts them to Parquet files. In the current implementation, any failures or timeouts arised out of the Oracle P6 API endpoints are retried by the GlueJob retry settings.

## Metadata files

The table below explains the purpose of some the important parameters.

| **Attribute name**                                  | **Description**                                                                                                                                                      |
|:----------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sampling_fraction and sampling_seed`          | These parameters are used to collect a sample set of data which is further used to detect schema changes.                                                          |
| `source_tables` `renamed_tables`     | Indicates the nodes within the XML payload of Extract API that needs to be parsed and the corresponding table naming convention for such nodes.                                                        |
| `input_path` | The location of the GZIPPED XML data of the Extract API.        |
| `schema_output_s3`                                   | The location of sampled data on S3 for each of the sampled XML nodes.                                                               |



Below is a sample raw layer metadata file.


### Raw Layer Metadata Example

```yaml
---
---
SourceSystemId: oracle_p6
MetadataType: api#oracle_p6#extract_api
name: Oracle_P6_ExtractAPI
aws_region: ap-southeast-2
is_active: "y"
auth_api_parameter:
  auth_method: post
  endpoint: https://p6.oraclecloud.com/worley/p6ws/oauth/token
  secret_key: Worley-datalake-sydney-dev-db-p6
  auth_retry: 3
  auth_timeout: 600
  auth_exponential_backoff: 5
  auth_headers:
    Content-Type: application/json
    token_exp: '3600'
  dynamic_auth_headers:
  - AuthToken
  auth_query_params: 
  auth_body: 
api_parameter:
  api_method: post
  endpoint: https://p6.oraclecloud.com/worley/p6ws/restapi/export/exportProject
  api_response_type: binary
  api_retry: 3
  api_timeout: 360
  api_exponential_backoff: 3
  auth_type: Bearer
  api_headers:
    Content-Type: application/json
  dynamic_api_headers:
  - Authorization
  api_query_params: 
  dynamic_api_query_params: 
  api_body: 
  custom_config:
    export_api_input_body: config/oracle_p6/P6ProjectExportInput.json
job_parameter:
  bucket_name: worley-datalake-sydney-dev-bucket-raw-xd5ydg
  bucket_data_source_prefix: oracle_p6
  kms_key_id: 3a4345c9-617f-4e55-8a27-70814760f56a
  input_path: oracle_p6/temp/exportapi/project_gzip_data/
  temp_output_path: s3://worley-datalake-sydney-dev-bucket-raw-xd5ydg/oracle_p6/temp/exportapi/relationalized_data/
  output_s3: oracle_p6
  schema_output_s3: "/oracle_p6/data_sampling/"
  export_api_root_node: APIBusinessObjects
  sampling_fraction: '0.5'
  sampling_seed: 42
  activity_column: root_Project.Activity
  activity_code_column: root_Project.Activity.val.Code
  activity_code_join_condition: id
  activity_join_condition: index
  activitycodeassignment_table_name: activitycodeassignment
  full_incr: i/f
  source_tables:
  - root
  - root_Project.Activity
  - root_ActivityCode
  - root_ActivityCodeType
  - root_Project.ResourceAssignment
  - root_Project.ActivityCode
  - root_Resource
  - root_EPS
  - root_Project.ActivityCodeType
  renamed_tables:
  - project
  - project_activity
  - activitycode
  - activitycodetype
  - project_resourceassignment
  - project_activitycode
  - resource
  - eps
  - project_activitycodetype


```

### Curation Layer Metadata Example

```yaml
---
SourceSystemId: curated_oracle_p6
MetadataType: curated#oracle_p6#activitycode#job#iceberg
source:
  name: oracle_p6_activitycode
  compute_engine: spark
  spark_options:
    format: parquet
  glue_options:
    connection_type: s3
    connection_options: s3://worley-datalake-sydney-dev-bucket-raw-xd5ydg/oracle_p6/activitycode/
    format: parquet
    transformation_ctx: oracle_p6-activitycode
transforms:
  - transform: rename_columns
    rename_column: true
  - transform: select_columns_from_config_file
    select_columns: true
  - transform: change_data_types
    change_types: true
  - transform: add_run_date
    column_name: EXECUTION_DATE
    date_format: yyyy-MM-dd
  - transform: custom_sql
    sql: SELECT *, 'oracle_p6' as SOURCE_SYSTEM_NAME FROM temp_df_static
    temp_view_name: temp_df_static
target:
  name: curated_oracle_p6_activitycode
  compute_engine: spark
  iceberg_properties:
    database_name: worley_datalake_sydney_dev_glue_catalog_database_oracle_p6
    table_name: curated_activitycode
    iceberg_configuration:
      create_table: true
      iceberg_catalog_warehouse: worley-datalake-sydney-dev-bucket-curated-xd5ydg/oracle_p6/activitycode/
      table_properties:
        write.format.default: parquet
        format-version: '2'
  load_type: incremental
  spark_options:
    format: iceberg
    options:
      path: s3://worley-datalake-sydney-dev-bucket-curated-xd5ydg/oracle_p6/activitycode
table_schema:
  schema_properties:
    enforce: true
    rename_columns: true
  columns:
    - column_name: CODECONCATNAME
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_codeconcatname
    - column_name: CODETYPENAME
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_codetypename
    - column_name: CODETYPEOBJECTID
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_codetypeobjectid
    - column_name: CODETYPESCOPE
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_codetypescope
    - column_name: CODEVALUE
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_codevalue
    - column_name: COLOR
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_color
    - column_name: CREATEDATE
      column_data_type: timestamp
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_createdate
    - column_name: CREATEUSER
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_createuser
    - column_name: DESCRIPTION
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_description
    - column_name: LASTUPDATEDATE
      column_data_type: timestamp
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_lastupdatedate
    - column_name: LASTUPDATEUSER
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_lastupdateuser
    - column_name: OBJECTID
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_objectid
    - column_name: PARENTOBJECTID
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_parentobjectid
    - column_name: PROJECTOBJECTID
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_projectobjectid__xsi_nil
    - column_name: SEQUENCENUMBER
      column_data_type: string
      nullable: true
      comment: null
      data_classification: null
      raw_column_name: activitycode_val_sequencenumber
    - column_name:  projectid 
      column_data_type:   long 
      nullable: true
      comment:  projectid 
      data_classification: null
      raw_column_name:   projectid   


```

## Oracle P6 Pipeline




The Oracle P6 pipeline consists of 3 important tasks:

- Triggers the raw layer Glue job to process Extract API.
- Triggers the raw layer Glue job to process Activity Spread API and Resource Spread API.
- Triggers the schema change detection Glue job to compare schema between current and previous versions.
- Triggers the curation layer Glue job that creates iceberg tables.


## Future Scope

- The JSON payload used by Extract API (HTTP PUT Body) can be uploaded to the S3 bucket as part of CICD pipeline automation.

