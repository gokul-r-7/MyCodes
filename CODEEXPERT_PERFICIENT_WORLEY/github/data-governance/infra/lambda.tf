locals {
  python_version = "python3.12"
  lambda_timeout = 900
}

# resource "null_resource" "install_python_dev_tools" {
#   provisioner "local-exec" {
#     command = "apt-get install python3-dev"
#   }
#   triggers = {
#     always_run = timestamp()
#   }
# }


module "lakeformation_setup_function" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "worley-${var.environment}-rbac-lakeformation-setup"
  description   = "Sets up lakeformmation"
  handler       = "lakeformation_setup.lambda_handler"
  runtime       = local.python_version
  publish       = true

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.lakeformation_setup_lambda_policy.arn

  # layers = [
  #   module.lambda_layer_s3.lambda_layer_arn,
  # ]

  environment_variables = {
    ENVIRONMENT  = var.environment
    RBAC_VERSION = "v2"
  }

  tags = {
    Module = "lambda-with-layer"
  }
}

module "lakeformation_manage_permissions_function" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "worley-rbac-${var.environment}-lakeformation-manage-permissions"
  description   = "Manages LF permissions"
  handler       = "lakeformation_manage_permissions.lambda_handler"
  runtime       = local.python_version
  publish       = true

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.lakeformation_setup_lambda_policy.arn


  environment_variables = {
    ENVIRONMENT  = var.environment
    RBAC_VERSION = "v2"
  }

  tags = {
    Module = "lambda-with-layer"
  }

  allowed_triggers = {
    event_rule = {
      principal  = "events.amazonaws.com"
      source_arn = aws_cloudwatch_event_rule.glue_db_state_change.arn
    }
  }
}


module "redshift_setup_function" {
  # TODO Remove this resource after v2 cutover

  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"
  function_name = "worley-rbac-redshift-setup"
  description   = "Sets up redshift"
  handler       = "redshift_setup.lambda_handler"
  runtime       = local.python_version
  publish       = true

  source_path = "src"
  timeout     = local.lambda_timeout
  attach_policy = true
  policy        = aws_iam_policy.redshift_setup_lambda_policy.arn

  attach_network_policy = true

  vpc_subnet_ids         = var.vpc_subnet_ids
  vpc_security_group_ids = [module.rbac_redshift_lambda_sg.security_group_id]

  # layers = [
  #   module.lambda_layer_s3.lambda_layer_arn,
  # ]

  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }
}

module "external_user_loader_domain_splitter" {
  # TODO Remove this resource after v2 cutover
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "external_user_loader_domain_splitter"
  description   = "Reads user file from s3 and splits in to domains"
  handler       = "domain_user_splitter.lambda_handler"
  runtime       = local.python_version
  publish       = true
  memory_size   = 512

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.redshift_setup_lambda_policy.arn

  attach_network_policy = true

  # layers = [
  #   module.lambda_layer_s3.lambda_layer_arn,
  # ]

  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }


}

module "external_user_loader_domain_loader" {
  # TODO Remove this resource after v2 cutover
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "${var.environment}_external_user_loader_domain_loader"
  description   = "Reads user file from s3 and splits in to domains"
  handler       = "domain_user_loader.lambda_handler"
  runtime       = local.python_version
  publish       = true
  memory_size   = 512

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.redshift_setup_lambda_policy.arn

  attach_network_policy = true

  # layers = [
  #   module.lambda_layer_s3.lambda_layer_arn,
  # ]

  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }


}

module "data_modelling_sns_notification_function" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "worley-data-modelling-sns-notification"
  description   = "Sends sns notification for failed data modelling dags"
  handler       = "data_modelling_sns_notification.lambda_handler"
  runtime       = local.python_version
  publish       = true

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.data_modelling_sns_notification_policy.arn

  # layers = [
  #   module.lambda_layer_s3.lambda_layer_arn,
  # ]

  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }
}


module "idc_apps_ad_group_onboader" {
  # TODO Remove this resource after v2 cutover
  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "${var.environment}_onboard_adgroup_aws_services"
  description   = "onboards ad groups to aws services"
  handler       = "onboard_adgroups_apps.lambda_handler"
  runtime       = local.python_version
  publish       = true

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.lakeformation_setup_lambda_policy.arn

  # layers = [
  #   module.lambda_layer_s3.lambda_layer_arn,
  # ]

  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }
}

#########################################################
#     Lambda invokes                                    #
########################################################

resource "aws_lambda_invocation" "run_lakeformation_lambda" {
  function_name = module.lakeformation_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    key1 = "value1"
    key2 = "value2"
  })
}

output "result_run_lakeformation_lambda" {
  value = jsondecode(aws_lambda_invocation.run_lakeformation_lambda.result)
}

resource "aws_lambda_invocation" "run_idc_apps_onboard_lambda" {
  # TODO Remove this resource after v2 cutover
  function_name = module.idc_apps_ad_group_onboader.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    key1 = "value1"
    key2 = "value2"
  })
}

output "result_run_idc_apps_lambd" {
  # TODO Remove this resource after v2 cutover
  value = jsondecode(aws_lambda_invocation.run_idc_apps_onboard_lambda.result)
}

resource "aws_lambda_invocation" "run_idc_apps_onboard_lambda_v2" {
  function_name = module.idc_apps_ad_group_onboader.lambda_function_arn

  triggers = {
    yaml_files = join("", [for f in fileset("${path.module}/src/configs/${var.environment}/redshift_permissions", "{*.yml,*.yaml}") :
      filebase64sha256("${path.module}/src/configs/${var.environment}/redshift_permissions/${f}")
    ])
  }

  input = jsonencode({
    version = "2"
  })
}

output "result_run_idc_apps_lambd_v2" {
  value = jsondecode(aws_lambda_invocation.run_idc_apps_onboard_lambda.result)
}


###############################################################
#    Apply Permissions Lambda Invoke                          #
#  NOTE : if you are adding a new domain please add an invoke # 
# block below for the new domain                              #
###############################################################


# Construction
resource "aws_lambda_invocation" "run_redshift_lambda_construction" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "construction"
  })
}

output "result_run_redshift_lambda_construction" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_construction.result)
}

# project_control
resource "aws_lambda_invocation" "run_redshift_lambda_project_control" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "project_control"
  })
}

output "result_run_redshift_lambda_project_control" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_project_control.result)
}


# document_control
resource "aws_lambda_invocation" "run_redshift_lambda_document_control" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "document_control"
  })
}

output "result_run_redshift_lambda_document_control" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_document_control.result)
}

# supply_chain
resource "aws_lambda_invocation" "run_redshift_lambda_supply_chain" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "supply_chain"
  })
}

output "result_run_redshift_lambda_supply_chain" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_supply_chain.result)
}

# circuit_breaker
resource "aws_lambda_invocation" "run_redshift_lambda_circuit_breaker" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "circuit_breaker"
  })
}

output "result_run_redshift_lambda_circuit_breaker" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_circuit_breaker.result)
}

# finance
resource "aws_lambda_invocation" "run_redshift_lambda_finance" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "finance"
  })
}

output "result_run_redshift_lambda_finance" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_finance.result)
}

# engineering
resource "aws_lambda_invocation" "run_redshift_lambda_engineering" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "engineering"
  })
}

output "result_run_redshift_lambda_engineering" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_engineering.result)
}


# customer
resource "aws_lambda_invocation" "run_redshift_lambda_customer" {
  function_name = module.redshift_setup_function.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/governance_config.json")
  }

  input = jsonencode({
    init   = "true",
    domain = "customer"
  })
}

output "result_run_redshift_lambda_customer" {
  value = jsondecode(aws_lambda_invocation.run_redshift_lambda_customer.result)
}