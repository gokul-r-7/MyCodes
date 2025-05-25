
data "aws_iam_role" "glue_role" {
  name = var.glue_role_name
}

data "aws_iam_role" "mwaa_role" {
  name = var.mwaa_role_name
}

data "aws_iam_role" "tfc_role" {
  name = var.tfc_role_name
}

data "aws_iam_role" "lakeformation_account_admin_role" {
  name = var.account_admin_role
}


resource "aws_lakeformation_data_lake_settings" "lakeformation_lambda_admin_grant" {
  admins = [
    module.lakeformation_manage_permissions_function.lambda_role_arn,
    module.lakeformation_setup_function.lambda_role_arn,
    data.aws_iam_role.glue_role.arn,
    data.aws_iam_role.lakeformation_account_admin_role.arn,
    data.aws_iam_role.mwaa_role.arn,
    data.aws_iam_role.tfc_role.arn
  ]
}
