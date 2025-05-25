resource "aws_s3_object" "ohio_job_script_aconex_workflow_sourcing" {
  bucket = module.bucket_glue_jobs_scripts.s3_bucket_id
  key    = "aconex_workflow_api_sourcing_multi_region_job.py"

  # provider = aws.us-east-2

  source             = "${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_workflow_api_sourcing_multi_region_job.py"
  source_hash        = filemd5("${path.root}/src/data_pipelines/0_sourcing/aconex/jobs/aconex_workflow_api_sourcing_multi_region_job.py")
  kms_key_id         = module.kms_key.key_arn
  bucket_key_enabled = true

  force_destroy = true

  # override default tags
  tags = {
    Terraform = "true"
  }

}


module "ohio_glue_job_aconex_workflow_api_sourcing_job" {
  source  = "cloudposse/glue/aws//modules/glue-job"
  version = "0.4.0"

  providers = { aws = aws.us-east-2 }


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
    "--region-short-name" : var.ohio_stage,
    "--region-name" : var.aws_ohio_region,
    "--job-bookmark-option" : "job-bookmark-enable",
    "--enable-continuous-cloudwatch-log" : "true",
    "--enable-glue-datacatalog" : "true",
    "--enable-job-insights" : "true",
    "--enable-metrics" : "true",
    "--enable-observability-metrics" : "true",
    "--enable-continuous-log-filter" : "true",
    "--TempDir" : format("s3://%s/jobs/aconex_workflow_api_sourcing_multi_region_job/", module.bucket_temp.s3_bucket_id),
    "--additional-python-modules" : "pydantic,dynamodb_json,s3://${module.bucket_glue_jobs_scripts.s3_bucket_id}/wheel/worley_helper-0.0.1-py3-none-any.whl",
    "--extra-py-files" : null
  }
  connections            = [module.ohio_glue_connection.name]
  security_configuration = aws_glue_security_configuration.ohio_buckets_kms_security_config.id

  # The job timeout in minutes
  timeout = 600

  command = {
    # The name of the job command. Defaults to `glueetl`.
    # Use `pythonshell` for Python Shell Job Type, or `gluestreaming` for Streaming Job Type.
    name            = "glueetl"
    script_location = format("s3://%s/aconex_workflow_api_sourcing_multi_region_job.py", module.bucket_glue_jobs_scripts.s3_bucket_id)
    python_version  = 3
  }
  execution_property = {
    max_concurrent_runs = 10
  }

  attributes = ["glue", "job", "aconex", "workflow", "api", "sourcing", "job"]
  context    = module.ohio_label.context
}

