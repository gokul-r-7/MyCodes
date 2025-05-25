module "metadata_framework_dynamodb_table_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.metadata_framework_label.context
  attributes = ["metadata", "table"]
}

resource "aws_s3_object" "job_script_raw_curated_generic_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "raw_curated_generic.py"

  source             = "${path.root}/src/data_pipelines/1_curation/raw_curated_generic.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/1_curation/raw_curated_generic.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}


module "glue_job_raw_curated_generic_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that Converts a Dataset to Iceberg"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.4X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/raw_curated_generic/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : ""

  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.glue_connection[1].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/raw_curated_generic.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "raw", "to", "curated", "generic"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_dataops_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "dataops_utility_job.py"

  source             = "${path.root}/src/data_pipelines/3_dataops/dataops_utility_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/3_dataops/dataops_utility_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_dataops_generic_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Dataops Utility Job"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/raw_curated_generic/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : "",
    "--operation_type" : ""
  }

  # The job timeout in minutes
  timeout                = 60
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/dataops_utility_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "dataops", "utility", "generic"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_p6_extract_spread_data" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "oracle_p6_sourcing_extract_spread_data.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/oracle_p6/jobs/oracle_p6_sourcing_extract_spread_data.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/oracle_p6/jobs/oracle_p6_sourcing_extract_spread_data.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_p6_extract_spread_data" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Activity Spread or Resource Spread Data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 1
  default_arguments = {
    "--project_id" : "",
    "--source_name" : "oracle_p6",
    "--function_name" : "activity_spread",
    "--start_date" : "",
    "--end_date" : "",
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-continuous-log-filter" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--TempDir" : format("s3://%s/jobs/oracle_p6_sourcing_extract_spread_data/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null

  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 120

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/oracle_p6_sourcing_extract_spread_data.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "p6", "extract", "spread", "data"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_p6_extract_export_api_data" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "oracle_p6_sourcing_extract_export_api_data.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/oracle_p6/jobs/oracle_p6_sourcing_extract_export_api_data.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/oracle_p6/jobs/oracle_p6_sourcing_extract_export_api_data.py")
  bucket_key_enabled = true
  kms_key_id         = module.kms_key.key_arn

  force_destroy = true
}

module "glue_job_p6_extract_export_api_data" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"


  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 1
  default_arguments = {
    "--project_id" : "",
    "--source_name" : "oracle_p6",
    "--function_name" : "extract_api",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--start_date" : "",
    "--end_date" : "",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/oracle_p6_sourcing_extract_export_api_data/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }

  execution_property = {
    max_concurrent_runs = 20
  }

  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 60

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/oracle_p6_sourcing_extract_export_api_data.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  attributes = ["glue", "job", "p6", "extract", "export", "api", "data"]
  context    = module.label.context
}

####new P6 Json based api job

resource "aws_s3_object" "job_script_p6_sourcing_json_api_data" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "oracle_p6_sourcing_json_api_data.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/oracle_p6/jobs/oracle_p6_sourcing_json_api_data.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/oracle_p6/jobs/oracle_p6_sourcing_json_api_data.py")
  bucket_key_enabled = true
  kms_key_id         = module.kms_key.key_arn

  force_destroy = true
}

module "glue_job_p6_sourcing_json_api_data" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"


  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--project_id" : "",
    "--source_name" : "oracle_p6",
    "--function_name" : "",
    "--data_type" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/oracle_p6_sourcing_json_api_data/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }

  execution_property = {
    max_concurrent_runs = 20
  }

  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 60

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/oracle_p6_sourcing_json_api_data.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  attributes = ["glue", "job", "p6", "sourcing", "json", "api", "data"]
  context    = module.label.context
}




## CSV and XLSX handling ##

### Begin XLSX Parsing GlueJob ###
resource "aws_s3_object" "job_script_convert_csv_and_xlsx_to_parquet" {
  bucket             = module.bucket_glue_jobs_scripts.s3_bucket_id
  key                = "convert_csv_and_xlsx_to_parquet.py"
  source             = "${path.root}/src/data_pipelines/0_sourcing/csv_xlsx/jobs/convert_csv_and_xlsx_to_parquet.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/csv_xlsx/jobs/convert_csv_and_xlsx_to_parquet.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_convert_csv_and_xlsx_to_parquet" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Convert CSV and XLSX files to Parquet format"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 3
  max_retries       = 1
  default_arguments = {
    "--source_name" : "e3d",
    "--function_name" : "csv_xlsx#vg",
    "--connector_file_path" : "",
    "--start_date" : "",
    "--end_date" : "",
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--metadata_table_name" : "worley-mf-sydney-${var.environment}-metadata-table",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-continuous-log-filter" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--TempDir" : format("s3://%s/jobs/convert_csv_and_xlsx_to_parquet/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "openpyxl,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null

  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/convert_csv_and_xlsx_to_parquet.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  execution_property = {
    max_concurrent_runs = 8
  }

  attributes = ["glue", "job", "csv_xlsx", "data"]
  context    = module.label.context
}

## CSV and XLSX handling ##

resource "aws_s3_object" "job_script_ecosys_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "ecosys_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_ecosys_ecosys_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--source_name" : "",
    "--function_name" : "",
    "--start_date" : "",
    "--end_date" : "",
    "--secorg_id" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/ecosys_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null,
    "--topic_arn" : "${module.sns_topic_data_platform_notification.sns_topic_arn}"
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 3600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/ecosys_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "ecosys", "workflow", "api", "sourcing", "job"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_aconex_workflow_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_workflow_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_workflow_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_workflow_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

resource "aws_s3_object" "job_script_aconex_mail_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_mail_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_mail_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_mail_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

resource "aws_s3_object" "job_script_aconex_UserDirectory_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_UserDirectory_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserDirectory_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserDirectory_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

resource "aws_s3_object" "job_script_aconex_UserProject_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_UserProject_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProject_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProject_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

resource "aws_s3_object" "job_script_aconex_UserProjectRole_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_UserProjectRole_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProjectRole_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_UserProjectRole_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

resource "aws_s3_object" "job_script_erm_api_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "erm_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/erm/jobs/erm_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/erm/jobs/erm_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}


resource "aws_s3_object" "job_script_circuit_breaker_sharepoint_api_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "circuit_breaker_sharepoint_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/circuit_breaker/jobs/circuit_breaker_sharepoint_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/circuit_breaker/jobs/circuit_breaker_sharepoint_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_circuit_breaker_sharepoint_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--source_name" : "circuit_breaker",
    "--source_system_id" : "circuit_breaker",
    "--function_name" : "circuit",
    "--datalake-formats" : "iceberg",
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/circuit_breaker_sharepoint_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/circuit_breaker_sharepoint_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "circuit" , "breaker", "sharepoint", "api", "sourcing", "job"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_circuit_breaker_api_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "circuit_breaker_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/circuit_breaker/jobs/circuit_breaker_xls_to_parquet.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/circuit_breaker/jobs/circuit_breaker_xls_to_parquet.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_circuit_breaker_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--source_name" : "circuit_breaker",
    "--function_name" : "csv_xlsx#circuit_breaker",
    "--connector_file_path" : "circuit_breaker/raw/",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/circuit_breaker_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "openpyxl,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/circuit_breaker_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "circuit" , "breaker", "api", "sourcing", "job"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_aconex_document_register_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_document_register_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_document_register_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_document_register_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

resource "aws_s3_object" "job_script_aconex_project_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_project_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_project_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_project_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_aconex_workflow_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "workflow",
    "--endpoint_host" : "",
    "--ProjectId" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_ohio_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_workflow_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_workflow_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "workflow", "api", "sourcing", "job"]
  context    = module.label.context
}

module "glue_job_erm_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--source_name" : "erm",
    "--table_name" : "",
    "--queryID" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/erm_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/erm_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "erm", "api", "sourcing", "job"]
  context    = module.label.context
}


module "glue_job_aconex_mail_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "Mail",
    "--batch_run_start_time" : "",
    "--endpoint_host" : "",
    "--ProjectId" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_mail_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_mail_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "mail", "api", "sourcing", "job"]
  context    = module.label.context
}

module "glue_job_aconex_UserDirectory_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "UserDirectory",
    "--endpoint_host" : "",
    "--ProjectId" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_UserDirectory_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_UserDirectory_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "UserDirectory", "api", "sourcing", "job"]
  context    = module.label.context
}

module "glue_job_aconex_UserProject_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "UserProject",
    "--endpoint_host" : "",
    "--ProjectId" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_UserProject_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_UserProject_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "UserProject", "api", "sourcing", "job"]
  context    = module.label.context
}

module "glue_job_aconex_UserProjectRole_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "UserProjectRole",
    "--endpoint_host" : "",
    "--ProjectId" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_UserProjectRole_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_UserProjectRole_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "UserProjectRole", "api", "sourcing", "job"]
  context    = module.label.context
}

module "glue_job_aconex_document_register_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "docregister",
    "--endpoint_host" : "",
    "--ProjectId" : ""
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_document_register_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_document_register_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "documentregister", "api", "sourcing", "job"]
  context    = module.label.context
}

module "glue_job_aconex_project_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "aconex",
    "--function_name" : "project",
    "--endpoint_host" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_project_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_project_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "project", "api", "sourcing", "job"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_db_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "db_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/db/jobs/db_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/db/jobs/db_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_db_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--source_name" : "",
    "--table_name" : "",
    "--start_date" : "",
    "--end_date" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-disable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/db_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 90

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/db_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 50
  }

  attributes = ["glue", "job", "db", "sourcing", "job"]
  context    = module.label.context
}

resource "aws_s3_object" "job_script_schema_change_detection_generic_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "schema_change_detection_generic.py"

  source             = "${path.root}/src/data_pipelines/2_schema_check/schema_change_detection_generic.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/2_schema_check/schema_change_detection_generic.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}



module "glue_job_schema_change_detection_generic_job" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description = "Generic Job that detects Table Schema Change"
  role_arn        = module.glue_service_iam_role.arn
  max_capacity    = 1
  #number_of_workers = 1
  #worker_type       = "Standard"
  max_retries = 0
  default_arguments = {
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--catalog_db" : "",
    "--table_name" : "",
    "--topic_arn" : "${module.sns_topic_data_platform_notification.sns_topic_arn}"
  }

  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
  # The job timeout in minutes
  timeout = 20


  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "pythonshell"
    script_location = format("s3://%s/schema_change_detection_generic.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = "3.9"
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "schema", "change", "detection", "generic"]
  context    = module.label.context
}

# sftp connector 
resource "aws_s3_object" "job_sftp_and_unzipping" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "sftp_and_unzipping.py"

  source             = "${path.root}/src/sftp_connector/sftp_and_unzipping.py"
  source_hash        = filemd5("${path.root}/src/sftp_connector/sftp_and_unzipping.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_sftp_and_unzipping_job" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description = "Job that Connects to SFTP Server, Unzips Any Zip files and Loads to S3"
  role_arn        = module.glue_service_iam_role.arn
  max_capacity    = 1
  max_retries     = 0
  default_arguments = {
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--additional-python-modules" : "paramiko,pysftp,boto3,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-spark-ui" : "true",
    "--function_name": "",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--job-language" : "python",
    "--metadata_table_name" : "module.metadata_framework_dynamodb_table_label.id",
    "--source_name" : "oraclegbs",
    "--TempDir" : format("s3://%s/jobs/sftp_and_unzipping/", module.bucket_temp.s3_bucket_id)    
  }

  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
  # The job timeout in minutes
  timeout = 20

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "pythonshell"
    script_location = format("s3://%s/sftp_and_unzipping.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = "3.9"
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  connections = module.glue_connection[*].name
  attributes  = ["glue", "job", "sftp", "and", "unzipping"]
  context     = module.label.context
}

################################################################################
# Glue Job CSP Salesforce
################################################################################

resource "aws_s3_object" "job_script_source_csp_salesforce_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "csp_salesforce/csp_salesforce_ingestion.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/csp_salesforce/csp_salesforce_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/csp_salesforce/csp_salesforce_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "glue_job_source_csp_salesforce_job" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from CSP Salesforce"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--application_name" : "csp",
    "--end_date" : "2024-07-22",
    "--function_name" : "opportunity",
    "--metadata_table_name" : "sf_metadata",
    "--project_name" : "",
    "--source_name" : "csp_salesforce",
    "--start_date" : "2024-07-22",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-disable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/csp_salesforce_ingestion/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : "",
  }

  # The job timeout in minutes
  timeout                = 2880
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/csp_salesforce/csp_salesforce_ingestion.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "csp", "salesforce", "workflow", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# GenAI Glue Job Sharepoint
################################################################################

resource "aws_s3_object" "job_script_source_sharepoint_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "genai/sharepoint_ingestion.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/genai/sharepoint_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/genai/sharepoint_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "glue_job_source_sharepoint_job" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from Sharepoint"
  role_arn          = module.genai_sharepoint_glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/genai/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : ""
  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.genai_glue_connection.name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/genai/sharepoint_ingestion.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["genai", "glue", "job", "sharepoint", "workflow", "api", "sourcing", "job"]
  context    = module.label.context
}


################################################################################
# GenAI Glue Job PED-Dataverse
################################################################################
resource "aws_s3_object" "job_script_source_ped_dataverse_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "genai/ped_dataverse_ingestion.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/genai/ped_dataverse_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/genai/ped_dataverse_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

#checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
module "glue_job_source_ped_dataverse_job" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from PED Dataverse"
  role_arn          = module.genai_dataverse_glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/genai/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--start_date" : "2024-07-22",
    "--end_date" : "2024-07-22",
    "--source_name" : "ped",
    "--function_name" : "pedprojects"
  }

  # The job timeout in minutes
  timeout                = 60
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/genai/ped_dataverse_ingestion.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["genai", "glue", "job", "ped", "dataverse", "workflow", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# Glue Job Iceberg to Aurora data ingestion generic
################################################################################

resource "aws_s3_object" "job_script_iceberg_aurora_ingestion_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aurora_ingestion/iceberg_aurora_ingestion.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aurora/iceberg_aurora_ingestion.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aurora/iceberg_aurora_ingestion.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_iceberg_aurora_ingestion_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from Iceberg and ingects to Aurora DB"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--additional-python-modules" : "psycopg2-binary,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--application_name" : "aurora_ingestion_generic",
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--datalake-formats" : "iceberg",
    "--project_name" : "",
    "--source_name" : "",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-disable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/iceberg_aurora_ingestion/", module.bucket_temp.s3_bucket_id),
    "--source_system_id" : "",
    "--metadata_type" : ""
  }

  # The job timeout in minutes
  timeout                = 2880
  connections            = [module.glue_connection[1].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aurora_ingestion/iceberg_aurora_ingestion.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "iceberg", "to", "aurora", "generic"]
  context    = module.label.context
}


################################################################################
# O3 API Data Jobs
################################################################################

resource "aws_s3_object" "job_script_o3_api_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "o3_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/o3/o3_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/o3/o3_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}


module "glue_job_o3_api_data_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from O3 APIs and ingests to S3 raw bucket"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--application_name" : "o3_api_sourcing",
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--datalake-formats" : "iceberg",
    "--project_name" : "",
    "--source_name" : "o3",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-disable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/o3_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--source_system_id" : "",
    "--metadata_type" : "",
    "--ProjectId" : "",
    "--function_name" : "",
    "--EndPoint" : "",
    "--masking_metadata_table_name" : "worley-mf-rbac-sydney-dev-database-permissions-metadata",
    "--database_name" : "worley_datalake_sydney_dev_glue_catalog_database_construction_o3_raw",
    "--ApiType" : ""
  }

  # The job timeout in minutes
  timeout                = 2880
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/o3_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 26 #based on number of endpoints being called for o3 apis
  }

  attributes = ["glue", "job", "o3", "workflow", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# Document Control Aconex Sourcing Glue Jobs
################################################################################

# Mail Document Sourcing Glue
resource "aws_s3_object" "job_script_aconex_mail_document_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_mail_document_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_mail_document_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_mail_document_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_aconex_mail_document_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Aconex mail_document API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--domain_name" : "document_control",
    "--source_name" : "aconex",
    "--function_name" : "mail_document",
    "--endpoint_host" : "",
    "--ProjectId" : "",
    "--batch_run_start_time" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_mail_document_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pyarrow,awswrangler==3.9.0,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null,
    "--user-jars-first": "true",
    "--extra-jars": "s3://${module.bucket_raw.s3_bucket_id}/jars/spark-xml_2.12-0.15.0.jar" 
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 3600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_mail_document_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "maildocument", "api", "sourcing", "job"]
  context    = module.label.context
}

# Mail Document Sourcing Glue ends ---

################################################################################
# Generic Sharepoint Sourcing  API Glue Jobs
################################################################################

# Generic Sharepoing Sourcing Glue
resource "aws_s3_object" "job_script_sharepoint_document_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "sharepoint_document_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/SharePoint_Document/sharepoint_document_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/SharePoint_Document/sharepoint_document_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_sharepoint_document_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the sharepoint document API data - generic"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "SharePointHexagonOFE",
    "--batch_run_start_time" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/sharepoint_document_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[0].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/sharepoint_document_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 2 # As it is generic so concurrency will be higher end
  }

  attributes = ["glue", "job", "sharepoint", "document", "api", "sourcing", "job"]
  context    = module.label.context
}

# SharePoint Document Sourcing Glue ends ---
################################################################################
# Package Aconex Sourcing Glue Jobs
################################################################################

# Mail Document Sourcing Glue
resource "aws_s3_object" "job_script_aconex_package_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_package_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_package_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_package_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_aconex_package_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Aconex package API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--domain_name" : "document_control",
    "--source_name" : "aconex",
    "--function_name" : "package",
    "--endpoint_host" : "",
    "--ProjectId" : "",
    "--batch_run_start_time" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_package_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pyarrow,awswrangler==3.9.0,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_package_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 5
  }

  attributes = ["glue", "job", "aconex", "package", "api", "sourcing", "job"]
  context    = module.label.context
}

# Aconex Package Sourcing Glue ends ---


################################################################################
# Generic sftp Sourcing Glue job
################################################################################

# Generic sftp Sourcing Glue
resource "aws_s3_object" "job_script_sftp_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "sftp_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/sftp/sftp_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/sftp/sftp_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_sftp_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Job that Connects to SFTP Server, Unzips Any Zip files, converts txt to csv files and Loads to S3"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "oraclegbs",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-spark-ui" : "true",
    "--function_name": "finance",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/sftp_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "paramiko,pysftp,boto3,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/sftp_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20 # As it is generic so concurrency will be higher end
  }

  attributes = ["glue", "job", "sftp", "sourcing", "job"]
  context    = module.label.context
}

# Generic sftp sourcing Glue ends ---

################################################################################
# Generic Ecosys Glue job for xml api
################################################################################

# Generic glue job for xml api
resource "aws_s3_object" "job_script_ecosys_xml_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "ecosys_api_xml_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_xml_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/ecosys/jobs/ecosys_api_xml_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_ecosys_ecosys_api_xml_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API XML data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "",
    "--function_name" : "",
    "--start_date" : "",
    "--end_date" : "",
    "--secorg_id" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/ecosys_api_xml_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null,
    "--topic_arn" : "${module.sns_topic_data_platform_notification.sns_topic_arn}"
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 2880

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/ecosys_api_xml_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 50
  }

  attributes = ["glue", "job", "ecosys", "workflow", "api", "xml", "sourcing", "job"]
  context    = module.label.context
}
################################################################################
# Generic Ecosys Glue job for xml api -- End
################################################################################

################################################################################
# SMB connector 
################################################################################

resource "aws_s3_object" "job_smb_connector" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "smb_connector.py"

  source             = "${path.root}/src/smb_connector/smb_connector.py"
  source_hash        = filemd5("${path.root}/src/smb_connector/smb_connector.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true  # Changed to prevent accidental deletions

  tags = {
    Terraform = "true"
  }
}

module "glue_job_smb_connector_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"  # Ensure the latest stable version is used

  job_description = "Job that Connects to SMB Server, and Loads to S3"
  role_arn        = module.glue_service_iam_role.arn
  glue_version    = var.glue_version
  worker_type     = "G.1X"
  number_of_workers = 2
  max_retries     = 0
  default_arguments = {
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region_name" : var.aws_region,
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-spark-ui" : "true",
    "--function_name" : "",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--job-language" : "python",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--batch_run_start_time" : "",
    "--source_name" : "",
    "--TempDir" : format("s3://%s/jobs/smb_connector/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : join(",", [
      "pyspark",
      "smbprotocol",
      "paramiko",
      "boto3",
      "pydantic",
      "dynamodb_json",
      format("s3://%s/wheel/worley_helper-0.0.1-py3-none-any.whl", module.bucket_glue_jobs_scripts.s3_bucket_id)
    ])
  }

  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
  timeout = 600

  command = {
    name            = "glueetl"
    script_location = format("s3://%s/smb_connector.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  execution_property = {
    max_concurrent_runs = 10  # Reduced to avoid overloading
  }

  connections = module.glue_connection[*].name
  attributes  = ["glue", "job", "smb", "connector"]
  context     = module.label.context
}

################################################################################
# SMB connector Glue Job ends
################################################################################

################################################################################
# SMB connector for Document Control Bot files
################################################################################
resource "aws_s3_object" "job_smb_connector_bot_files" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "smb_connector_bot_files.py"

  source             = "${path.root}/src/smb_connector/smb_connector_bot_files.py"
  source_hash        = filemd5("${path.root}/src/smb_connector/smb_connector_bot_files.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true  # Changed to prevent accidental deletions

  tags = {
    Terraform = "true"
  }
}

module "glue_job_smb_connector_bot_files_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"  # Ensure the latest stable version is used

  job_description = "Job that Connects to SMB Server, and Loads to S3"
  role_arn        = module.glue_service_iam_role.arn
  glue_version    = var.glue_version
  worker_type     = "G.1X"
  number_of_workers = 2
  max_retries     = 0
  default_arguments = {
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region_name" : var.aws_region,
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-spark-ui" : "true",
    "--function_name" : "",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--job-language" : "python",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--batch_run_start_time" : "",
    "--source_name" : "",
    "--TempDir" : format("s3://%s/jobs/smb_connector/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : join(",", [
      "pyspark",
      "smbprotocol",
      "paramiko",
      "boto3",
      "pydantic",
      "dynamodb_json",
      format("s3://%s/wheel/worley_helper-0.0.1-py3-none-any.whl", module.bucket_glue_jobs_scripts.s3_bucket_id)
    ])
  }

  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
  timeout = 600

  command = {
    name            = "glueetl"
    script_location = format("s3://%s/smb_connector_bot_files.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  execution_property = {
    max_concurrent_runs = 10  # Reduced to avoid overloading
  }

  connections = module.glue_connection[*].name
  attributes  = ["glue", "job", "smb", "connector","bot","files"]
  context     = module.label.context
}

################################################################################
# SMB connector for Document Control Bot files Glue Job ends
################################################################################

################################################################################
# BOT File CSV, XLSX to Parquet JOB
################################################################################

### Begin CSV Parsing GlueJob ###
resource "aws_s3_object" "job_script_convert_csv_to_parquet" {
  bucket             = module.bucket_glue_jobs_scripts.s3_bucket_id
  key                = "convert_aconex_csv_and_xlsx_to_parquet.py"
  source             = "${path.root}/src/data_pipelines/0_sourcing/csv/jobs/convert_aconex_csv_and_xlsx_to_parquet.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/csv/jobs/convert_aconex_csv_and_xlsx_to_parquet.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_convert_csv_to_parquet" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Convert CSV files to Parquet format"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 3
  max_retries       = 1
  default_arguments = {
    "--source_name" : "",
    "--function_name" : "",
    "--connector_file_path" : "",
    "--start_date" : "",
    "--end_date" : "",
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--metadata_table_name" : "worley-mf-sydney-${var.environment}-metadata-table",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-continuous-log-filter" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--TempDir" : format("s3://%s/jobs/convert_csv_to_parquet_converter/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "openpyxl,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null

  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/convert_aconex_csv_and_xlsx_to_parquet.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  execution_property = {
    max_concurrent_runs = 2
  }

  attributes = ["glue", "job", "aconex", "csv", "xlsx", "to", "parquet", "converter"]
  context    = module.label.context
}
################################################################################
# BOT File CSV, XLSX to Parquet JOB ends
################################################################################

################################################################################
# Glue Data Quality Glue Job starts
################################################################################

resource "aws_s3_object" "job_script_curated_data_quality_generic_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "curated_data_quality_generic.py"

  source             = "${path.root}/src/data_pipelines/4_data_quality/curated_data_quality_generic.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/4_data_quality/curated_data_quality_generic.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}


module "glue_job_curated_data_quality_generic_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that runs Data Quality checks"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/curated_data_quality_generic/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : ""

  }

  # The job timeout in minutes
  timeout                = 60
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/curated_data_quality_generic.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "curated", "data", "quality", "generic"]
  context    = module.label.context
}
################################################################################
# Glue Data Quality Glue Job ends
################################################################################

################################################################################
# Generic Sharepoint_List Sourcing  API Glue Jobs
################################################################################

# Generic Sharepoint Sourcing Glue
resource "aws_s3_object" "job_script_sharepoint_list_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "sharepointlist-api-sourcing-job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/SharePoint_List/sharepointlist-api-sourcing-job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/SharePoint_List/sharepointlist-api-sourcing-job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_sharepoint_list_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the sharepoint list API data - generic"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--source_name" : "SharePointList",
    "--batch_run_start_time" : "",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/sharepointlist-api-sourcing-job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/sharepointlist-api-sourcing-job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 2 # As it is generic so concurrency will be higher end
  }

  attributes = ["glue", "job", "SharePointList", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# Glue SharePoint List Sourcing Glue ends
################################################################################

module "glue_job_raw_curated_generic_job_upgraded" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that Converts a Dataset to Iceberg"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.8X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/raw_curated_generic/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : ""

  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.glue_connection[1].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/raw_curated_generic.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 5
  }

  attributes = ["glue", "job", "raw", "to", "curated", "generic","upgraded"]
  context    = module.label.context
}

#################################################################################
# Glue curated upgraded generic job ends
#################################################################################

################################################################################
# Glue ERM MIN Sourcing Glue
################################################################################

# ERM MIN Register Sourcing Glue
resource "aws_s3_object" "job_script_erm_min_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "erm_min_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/erm/jobs/erm_min_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/erm/jobs/erm_min_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}


module "glue_job_erm_min_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the ERM MIN Register API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--source_name" : "erm",
    "--function_name" : "registerprocess",
    "--drl_query_id" : "Q1991",
    "--picklist_query_id": "Q1989",
    "--project_id": "IN_NEH_RIL_POLY",
    "--bucket_name" : format("%s", module.bucket_raw.s3_bucket_id),
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/erm_min_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/erm_min_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "erm", "min", "register", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# Glue ERM MIN Sourcing Glue ends
################################################################################

#################################################################################
# Project Mapping Service Glue Job
#################################################################################

resource "aws_s3_object" "job_script_project_mapping" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "project_mapping_generic_service.py"

  source             = "${path.root}/src/data_pipelines/5_project_mapping/project_mapping_generic_service.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/5_project_mapping/project_mapping_generic_service.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}

module "glue_job_project_mapping_generic_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job for Project Mapping Service"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.4X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.dynamodb_project_mapping_table_label.id,
    "--job-bookmark-option" : "job-bookmark-disable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/project_mapping/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--transformed_s3_bucket" : module.bucket_transformed.s3_bucket_id,
    "--domain_name" : " ",
    "--target_table" : " "
  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.glue_connection[1].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/project_mapping_generic_service.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "project", "mapping", "service", "generic"]
  context    = module.label.context
}
    
################################################################################
# GenAI Glue Job Sharepoint document library API Start
################################################################################

resource "aws_s3_object" "job_script_genai_source_sharepoint_document_library_api_job" {
  count = contains(["dev"], var.genai_sharepoint_document_library_job) ? 1 : 0

  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "genai/sharepoint_document_library_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/genai/sharepoint_document_library_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/genai/sharepoint_document_library_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_source_sharepoint_document_library_api_job" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  count = contains(["dev"], var.genai_sharepoint_document_library_job) ? 1 : 0
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from Sharepoint using sharepoint v1 api"
  role_arn          = module.genai_sharepoint_glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "psycopg2-binary,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/genai/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system" : "",
    "--function_name" : ""
  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.genai_glue_connection.name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/genai/sharepoint_document_library_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["genai", "glue", "job", "sharepoint", "document","library", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# GenAI Glue Job Sharepoint document library API End
################################################################################
# GenAI Glue Job Sharepoint document list API start
################################################################################

resource "aws_s3_object" "job_script_genai_source_sharepoint_list_library_api_job" {
  count = contains(["dev"], var.genai_sharepoint_list_library_job) ? 1 : 0
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "genai/sharepoint_list_library_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/genai/sharepoint_list_library_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/genai/sharepoint_list_library_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_source_sharepoint_list_library_api_job" {
  #checkov:skip=CKV_TF_1:This is a new release by checkov and it requires changes at repo level.
  count = contains(["dev"], var.genai_sharepoint_list_library_job) ? 1 : 0
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that sources data from Sharepoint using sharepoint v1 api"
  role_arn          = module.genai_sharepoint_glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "psycopg2-binary,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/genai/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system" : "",
    "--function_name" : ""
  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.genai_glue_connection.name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/genai/sharepoint_list_library_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["genai", "glue", "job", "sharepoint", "list","library", "api", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# GenAI Glue Job Sharepoint list library API End
################################################################################

################################################################################
# ADT Glue Job Start
################################################################################

resource "aws_s3_object" "job_script_adt_api_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "adt_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/adt/jobs/adt_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/adt/jobs/adt_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}



module "glue_job_adt_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the Export API data"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--source_name" : "adt",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/adt_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/adt_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "adt", "api", "sourcing", "job"]
  context    = module.label.context
}


################################################################################
# ADT Glue Job end
#################################################################################

################################################################################
# Glue PeopleLink Sourcing Glue
################################################################################

resource "aws_s3_object" "job_script_people_link_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "people_link_hcm_extracts_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/people_link/jobs/people_link_hcm_extracts_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/people_link/jobs/people_link_hcm_extracts_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}


module "glue_job_people_link_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Gets the People Link data from OCI to S3 to parquet"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 4
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--source_name" : "people_link",
    "--function_name" : "hcm_extracts",
    "--bucket_name" : format("%s", module.bucket_raw.s3_bucket_id),
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/people_link_hcm_extarcts_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,PyJWT,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/people_link_hcm_extracts_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "people", "link", "hcm", "extracts", "sourcing", "job"]
  context    = module.label.context
}

################################################################################
# Glue PeopleLink Sourcing Glue ends
################################################################################

#################################################################################
# Jplus O3 Integration job
################################################################################
resource "aws_s3_object" "job_script_jplus_o3_int_job" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "jplus_o3_import.py"

  source             = "${path.root}/src/data_pipelines/6_transmit/jplus_o3_import.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/6_transmit/jplus_o3_import.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}


module "glue_job_jplus_o3_int_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Generic Job that uploads jplus data to o3"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.4X"
  number_of_workers = 2
  max_retries       = 0
  default_arguments = {
    "--datalake-formats" : "iceberg",
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--environment" : var.environment,
    "--region_short_name" : var.stage,
    "--region_name" : var.aws_region,
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/jplus_o3_import/", module.bucket_temp.s3_bucket_id),
    "--conf" : "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "--source_system_id" : "",
    "--metadata_type" : ""

  }

  # The job timeout in minutes
  timeout                = 600
  connections            = [module.glue_connection[1].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/jplus_o3_import.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 20
  }

  attributes = ["glue", "job", "jplus", "o3", "import", "api"]
  context    = module.label.context
}

################################################################################
# CSP Salesforce File CSV, XLSX to Parquet JOB
################################################################################

### Begin CSV Parsing GlueJob ###
resource "aws_s3_object" "job_script_convert_csv_to_parquet_and_archieve" {
  bucket             = module.bucket_glue_jobs_scripts.s3_bucket_id
  key                = "convert_csv_to_parquet_salesforce.py"
  source             = "${path.root}/src/data_pipelines/0_sourcing/csv/jobs/convert_csv_to_parquet_salesforce.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/csv/jobs/convert_csv_to_parquet_salesforce.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true
}

module "glue_job_convert_csv_to_parquet_and_archieve" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "Convert CSV files to Parquet format and archieve the source csv file"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 3
  max_retries       = 1
  default_arguments = {
    "--source_name" : "",
    "--function_name" : "",
    "--connector_file_path" : "",
    "--start_date" : "",
    "--end_date" : "",
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--metadata_table_name" : "worley-mf-sydney-${var.environment}-metadata-table",
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-continuous-log-filter" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--TempDir" : format("s3://%s/jobs/convert_csv_to_parquet_salesforce/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "openpyxl,pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null

  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/convert_csv_to_parquet_salesforce.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }

  execution_property = {
    max_concurrent_runs = 8
  }

  attributes = ["glue", "job", "csp", "salesforce", "csv", "xlsx", "to", "parquet", "converter"]
  context    = module.label.context
}
################################################################################
# CSP Salesforce File CSV, XLSX to Parquet JOB ends
################################################################################


################################################################################
# Source System: Contract
# Glue Job Start
################################################################################

resource "aws_s3_object" "job_script_contract_api_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "contract_api_sourcing_job.py"

  source             = "${path.root}/src/data_pipelines/0_sourcing/contract/jobs/contract_api_sourcing_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/contract/jobs/contract_api_sourcing_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }
}



module "glue_job_contract_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  job_description   = "API Extract Job - Source System - Contract"
  role_arn          = module.glue_service_iam_role.arn
  glue_version      = var.glue_version
  worker_type       = "G.2X"
  number_of_workers = 3
  max_retries       = 0
  default_arguments = {
    "--source_name" : "contract",
    "--metadata_table_name" : module.metadata_framework_dynamodb_table_label.id,
    "--environment" : var.environment,
    "--region-short-name" : var.stage,
    "--region-name" : var.aws_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/contract_api_sourcing_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "requests,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.glue_connection[2].name]
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    name            = "glueetl"
    script_location = format("s3://%s/contract_api_sourcing_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "contract", "api", "sourcing", "job"]
  context    = module.label.context
}


################################################################################
# Source System: Contract
# Glue Job Ends
################################################################################