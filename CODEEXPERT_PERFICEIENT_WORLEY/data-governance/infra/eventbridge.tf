resource "aws_cloudwatch_event_rule" "glue_db_state_change" {
  name        = "glue_db_state_change"
  description = "Capture each Glue DB Change"

  event_pattern = jsonencode(
    {
      "source" : ["aws.glue"],
      "detail-type" : ["Glue Data Catalog Database State Change"]
    }
  )
}

resource "aws_cloudwatch_event_target" "trigger_lf_rbac_manager" {
  rule      = aws_cloudwatch_event_rule.glue_db_state_change.name
  target_id = "SendToLFLambda"
  arn       = module.lakeformation_manage_permissions_function.lambda_function_arn
}
