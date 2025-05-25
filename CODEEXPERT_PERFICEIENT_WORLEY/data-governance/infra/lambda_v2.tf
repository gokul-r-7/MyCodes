# locals {
#   python_version = "python3.8"
#   lambda_timeout = 900
# }


module "lambda_layer" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "${var.environment}_rbac_framework_layert"
  description         = "rbac-layer)"
  compatible_runtimes = [local.python_version]

  source_path = "src"

  # store_on_s3 = true
  # s3_bucket   = "my-bucket-id-with-lambda-builds"
}



module "external_user_loader_database_schema_splitter" {

  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "${var.environment}_external_user_loader_database_schema_splitter"
  description   = "Reads user file from s3 and splits in to databases and schema version 2"
  handler       = "database_schema_user_splitter.lambda_handler"
  runtime       = local.python_version
  publish       = true
  memory_size   = 512


  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.redshift_setup_lambda_policy.arn

  attach_network_policy = true

  environment_variables = {
    ENVIRONMENT = var.environment
  }

  # layers = [
  #   module.lambda_layer.lambda_layer_arn,
  # ]

  tags = {
    Module = "lambda-with-layer"
  }


}

module "external_user_loader_database_schema_loader" {

  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "${var.environment}_external_user_loader_database_schema_loader"
  description   = "Reads user file from s3 and loads in to databases and schema version 2"
  handler       = "database_schema_user_loader.lambda_handler"
  runtime       = local.python_version
  publish       = true
  memory_size   = 512

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.redshift_setup_lambda_policy.arn

  attach_network_policy = true


  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }


}

module "redshift_setup_db_structure" {

  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "worley-rbac-redshift-setup-db-structure"
  description   = "Sets up redshift db structure based on /configs/<env>/database_structure.yml"
  handler       = "redshift_operations.setup_db_structure_handler"
  runtime       = local.python_version
  publish       = true

  source_path = "src"
  timeout     = local.lambda_timeout

  attach_policy = true
  policy        = aws_iam_policy.redshift_setup_lambda_policy.arn

  attach_network_policy = true

  vpc_subnet_ids         = var.vpc_subnet_ids
  vpc_security_group_ids = [module.rbac_redshift_lambda_sg.security_group_id]



  environment_variables = {
    ENVIRONMENT = var.environment
  }

  tags = {
    Module = "lambda-with-layer"
  }
}

module "redshift_apply_database_permissions" {

  source  = "terraform-aws-modules/lambda/aws"
  version = "7.10.0"

  function_name = "worley-rbac-redshift-apply-database-permissions"
  description   = "Create roles and apply redshift permisions"
  handler       = "redshift_operations.create_and_apply_permissions_handler"
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





#########################################################
#     Lambda invokes                                    #
########################################################

resource "aws_lambda_invocation" "run_setup_redshift_db_structure_lambda" {
  function_name = module.redshift_setup_db_structure.lambda_function_arn

  triggers = {
    source_code_hash = filebase64sha256("${path.module}/src/configs/${var.environment}/database_structure.yml")
  }

  input = jsonencode({})
}

output "result_run_setup_redshift_db_structure_lambda" {
  value = jsondecode(aws_lambda_invocation.run_setup_redshift_db_structure_lambda.result)
}


resource "aws_lambda_invocation" "run_setup_redshift_db_permissions_lambda" {
  function_name = module.redshift_apply_database_permissions.lambda_function_arn

  triggers = {
    yaml_files = join("", [for f in fileset("${path.module}/src/configs/${var.environment}/redshift_permissions", "{*.yml,*.yaml}") :
      filebase64sha256("${path.module}/src/configs/${var.environment}/redshift_permissions/${f}")
    ])
  }

  input = jsonencode({
    create_roles = true
  })
}

output "run_setup_redshift_db_permissions_lambda" {
  value = jsondecode(aws_lambda_invocation.run_setup_redshift_db_structure_lambda.result)
}



