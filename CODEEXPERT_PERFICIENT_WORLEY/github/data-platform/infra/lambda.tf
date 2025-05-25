locals {
  python_version = "python3.12"
  lambda_timeout = 900
}

variable "lambda_root" {
  type        = string
  description = "The relative path to the source of the lambda"
  default     = "src/lambda"
}

module "lambda_sg_label" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["lambda", "sg"]
}

# resource "null_resource" "install_python_dev_tools" {
#   provisioner "local-exec" {
#     command = "apt-get install python3-dev"
#   }
#   triggers = {
#     always_run = timestamp()
#   }
# }

# Create a new security group with source sg ID 
module "lambda_security_group" {
  #checkov:skip=CKV_TF_1:Terraform module sources do not use a git url with a commit hash revision
  source  = "cloudposse/security-group/aws"
  version = "2.2.0"

  name                  = module.lambda_sg_label.id
  security_group_name   = [module.lambda_sg_label.id]
  vpc_id                = var.vpc_id
  create_before_destroy = true
  allow_all_egress      = true

  rules = [
    {
      type        = "ingress"
      from_port   = 0
      to_port     = 65535
      protocol    = "tcp"
      cidr_blocks = []
      self        = true
      description = "All all ports from self"
    }
  ]

}


resource "aws_cloudwatch_event_rule" "weekly_friday_trigger" {
  name                = "worley-${var.environment}-weekly-friday-trigger"
  description         = "Triggers Lambda function every Friday"
  schedule_expression = "cron(0 0 ? * FRI *)"  # Runs at 00:00 UTC every Friday

  tags = merge(local.account.tags, {
    Namespace   = "worley",
    Stage       = "sydney",
    Terraform   = "true"
  }
  )
}

resource "aws_lambda_layer_version" "latest-boto3-layer" {
  layer_name = "${module.label.id}-boto3-layer"
  filename            = "${var.lambda_root}/boto3-layer/boto3_layer.zip"
  compatible_runtimes = ["python3.11"]
  source_code_hash    = filebase64sha256("${var.lambda_root}/boto3-layer/boto3_layer.zip")
}

module "iceberg_optimizer_function" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"
  function_name = "worley-${var.environment}-iceberg-optimisation-function"
  description   = "Enables Optimization of Iceberg Tables"
  handler       = "iceberg_optimization_enabler.lambda_handler"
  runtime       = local.python_version
  publish       = true
  layers        = [aws_lambda_layer_version.latest-boto3-layer.arn]

  cloudwatch_logs_retention_in_days  = local.retention_in_days

  source_path = "${var.lambda_root}/iceberg_optimization_enabler"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.iceberg_optimizer_enabler_lambda_policy.arn


  environment_variables = {
    ENVIRONMENT = var.environment,
    OPTIMIZER_ROLE_ARN = module.iceberg_optimizer_iam_role.arn,
    GLUE_CONNECTION_NAME = module.glue_connection[0].name
  }

  tags = merge(local.account.tags, {
    Namespace   = "worley",
    Stage       = "sydney",
    Terraform   = "true"
  }
  )


  allowed_triggers = {
      event_rule = {
        principal  = "events.amazonaws.com"
        source_arn = aws_cloudwatch_event_rule.weekly_friday_trigger.arn
      }
    }
}



################################################################################
# Transport Lambda function of Event driven pattern to refresh data(replacing worleySIM)
###################################################################################

data "archive_file" "event_driven_sys_int_project_delivery_zip" {
  type        = "zip"
  source_dir  = "src/lambda/event_trigger_project_delivery"
  output_path = "src/lambda/event_trigger_project_delivery/event_trigger_project_delivery.zip"
}

# Create a lambda function
resource "aws_lambda_function" "event_driven_sys_int_project_delivery" {
  function_name = "worley-${var.environment}-event-trigger-sysint-lambda-function"
  description   = "Lambda function to create an event to trigger the system int platform"
  handler       = "event_gen_consumer_model.lambda_handler"
  architectures = ["x86_64"]
  memory_size   = 1024
  timeout       = 600
  role          = module.lambda_iam_role.arn
  runtime       = "python3.11"
  layers      = [aws_lambda_layer_version.latest-boto3-layer.arn]

  tracing_config {
    mode = "PassThrough"
  }

  vpc_config {
    subnet_ids         = data.aws_subnets.private_glue.ids
    security_group_ids = [module.lambda_security_group.id]
  }

  kms_key_arn = module.kms_key.key_arn

  environment {
    variables = {
      ENVN = "${var.environment}",
      DYNAMODB_TABLE = "sydney-int-mf-${var.environment}-operational-table"
    }
  }

  filename         = data.archive_file.event_driven_sys_int_project_delivery_zip.output_path
  source_code_hash = filebase64sha256("src/lambda/event_trigger_project_delivery/event_trigger_project_delivery.zip")
  tags = merge(local.account.tags, {
    Namespace   = "worley",
    Stage       = "sydney",
    Terraform   = "true"
  }
  )
}

