resource "aws_s3_bucket" "external_user_store_bucket" {
  bucket = "worley-mf-${var.stage}-${var.environment}-external-user-store"
}

resource "aws_lambda_permission" "allow_landing" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = module.external_user_loader_domain_splitter.lambda_function_arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.external_user_store_bucket.arn
}

resource "aws_lambda_permission" "allow_domain" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = module.external_user_loader_domain_loader.lambda_function_arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.external_user_store_bucket.arn
}

resource "aws_lambda_permission" "allow_landing_v2" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = module.external_user_loader_database_schema_splitter.lambda_function_arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.external_user_store_bucket.arn
}

resource "aws_lambda_permission" "allow_domain_v2" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = module.external_user_loader_database_schema_loader.lambda_function_arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.external_user_store_bucket.arn
}


resource "aws_s3_bucket_notification" "bucket_notification" {

  bucket = aws_s3_bucket.external_user_store_bucket.id

  lambda_function {
    lambda_function_arn = module.external_user_loader_domain_loader.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "domain/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = module.external_user_loader_domain_splitter.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/"
    filter_suffix       = ".xlsx"
  }

  lambda_function {
    lambda_function_arn = module.external_user_loader_database_schema_loader.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "global_standard_reporting/processed/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = module.external_user_loader_database_schema_splitter.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "global_standard_reporting/landing/"
    filter_suffix       = ".xlsx"
  }
  lambda_function {
    lambda_function_arn = module.external_user_loader_database_schema_loader.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "integrations/processed/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = module.external_user_loader_database_schema_splitter.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "integrations/landing/"
    filter_suffix       = ".xlsx"
  }

  depends_on = [
    aws_lambda_permission.allow_domain,
    aws_lambda_permission.allow_landing,
    aws_lambda_permission.allow_domain_v2,
    aws_lambda_permission.allow_landing_v2,
  ]
}