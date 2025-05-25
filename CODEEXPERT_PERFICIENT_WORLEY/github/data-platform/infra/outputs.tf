output "arn" {
  value = tolist(data.aws_ssoadmin_instances.idc.arns)[0]
}

output "identity_store_id" {
  value = tolist(data.aws_ssoadmin_instances.idc.identity_store_ids)[0]
}

