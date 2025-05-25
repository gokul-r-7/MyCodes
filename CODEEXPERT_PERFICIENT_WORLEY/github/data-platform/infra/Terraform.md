# Terraform
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.8.1 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 5.45.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | 5.46.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_bucket_curated"></a> [bucket\_curated](#module\_bucket\_curated) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_curated_label"></a> [bucket\_curated\_label](#module\_bucket\_curated\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_bucket_glue_job_outputs"></a> [bucket\_glue\_job\_outputs](#module\_bucket\_glue\_job\_outputs) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_glue_job_outputs_label"></a> [bucket\_glue\_job\_outputs\_label](#module\_bucket\_glue\_job\_outputs\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_bucket_glue_job_scripts_label"></a> [bucket\_glue\_job\_scripts\_label](#module\_bucket\_glue\_job\_scripts\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_bucket_glue_jobs_scripts"></a> [bucket\_glue\_jobs\_scripts](#module\_bucket\_glue\_jobs\_scripts) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_landing"></a> [bucket\_landing](#module\_bucket\_landing) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_landing_label"></a> [bucket\_landing\_label](#module\_bucket\_landing\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_bucket_quarantine"></a> [bucket\_quarantine](#module\_bucket\_quarantine) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_quarantine_label"></a> [bucket\_quarantine\_label](#module\_bucket\_quarantine\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_bucket_raw"></a> [bucket\_raw](#module\_bucket\_raw) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_raw_label"></a> [bucket\_raw\_label](#module\_bucket\_raw\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_bucket_transformed"></a> [bucket\_transformed](#module\_bucket\_transformed) | terraform-aws-modules/s3-bucket/aws | 4.1.2 |
| <a name="module_bucket_transformed_label"></a> [bucket\_transformed\_label](#module\_bucket\_transformed\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_dynamodb_landing_table"></a> [dynamodb\_landing\_table](#module\_dynamodb\_landing\_table) | terraform-aws-modules/dynamodb-table/aws | 4.0.1 |
| <a name="module_dynamodb_landing_table_label"></a> [dynamodb\_landing\_table\_label](#module\_dynamodb\_landing\_table\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_glue_catalog_database_curated"></a> [glue\_catalog\_database\_curated](#module\_glue\_catalog\_database\_curated) | cloudposse/glue/aws//modules/glue-catalog-database | 0.4.0 |
| <a name="module_glue_catalog_database_raw"></a> [glue\_catalog\_database\_raw](#module\_glue\_catalog\_database\_raw) | cloudposse/glue/aws//modules/glue-catalog-database | 0.4.0 |
| <a name="module_glue_catalog_table_curated"></a> [glue\_catalog\_table\_curated](#module\_glue\_catalog\_table\_curated) | cloudposse/glue/aws//modules/glue-catalog-table | 0.4.0 |
| <a name="module_glue_catalog_table_raw"></a> [glue\_catalog\_table\_raw](#module\_glue\_catalog\_table\_raw) | cloudposse/glue/aws//modules/glue-catalog-table | 0.4.0 |
| <a name="module_glue_crawler_curated"></a> [glue\_crawler\_curated](#module\_glue\_crawler\_curated) | cloudposse/glue/aws//modules/glue-crawler | 0.4.0 |
| <a name="module_glue_crawler_raw"></a> [glue\_crawler\_raw](#module\_glue\_crawler\_raw) | cloudposse/glue/aws//modules/glue-crawler | 0.4.0 |
| <a name="module_glue_job_raw"></a> [glue\_job\_raw](#module\_glue\_job\_raw) | cloudposse/glue/aws//modules/glue-job | 0.4.0 |
| <a name="module_glue_service_iam_role"></a> [glue\_service\_iam\_role](#module\_glue\_service\_iam\_role) | cloudposse/iam-role/aws | 0.19.0 |
| <a name="module_glue_workflow_raw"></a> [glue\_workflow\_raw](#module\_glue\_workflow\_raw) | cloudposse/glue/aws//modules/glue-workflow | 0.4.0 |
| <a name="module_kms_curated"></a> [kms\_curated](#module\_kms\_curated) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_curated_label"></a> [kms\_curated\_label](#module\_kms\_curated\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_kms_glue_job_outputs"></a> [kms\_glue\_job\_outputs](#module\_kms\_glue\_job\_outputs) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_glue_job_outputs_label"></a> [kms\_glue\_job\_outputs\_label](#module\_kms\_glue\_job\_outputs\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_kms_glue_job_scripts"></a> [kms\_glue\_job\_scripts](#module\_kms\_glue\_job\_scripts) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_glue_job_scripts_label"></a> [kms\_glue\_job\_scripts\_label](#module\_kms\_glue\_job\_scripts\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_kms_landing"></a> [kms\_landing](#module\_kms\_landing) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_landing_label"></a> [kms\_landing\_label](#module\_kms\_landing\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_kms_quarantine"></a> [kms\_quarantine](#module\_kms\_quarantine) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_quarantine_label"></a> [kms\_quarantine\_label](#module\_kms\_quarantine\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_kms_raw"></a> [kms\_raw](#module\_kms\_raw) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_raw_label"></a> [kms\_raw\_label](#module\_kms\_raw\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_kms_transformed"></a> [kms\_transformed](#module\_kms\_transformed) | terraform-aws-modules/kms/aws | 2.2.1 |
| <a name="module_kms_transformed_label"></a> [kms\_transformed\_label](#module\_kms\_transformed\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_label"></a> [label](#module\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_redshift"></a> [redshift](#module\_redshift) | terraform-aws-modules/redshift/aws | 5.4.0 |
| <a name="module_redshift_label"></a> [redshift\_label](#module\_redshift\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_redshift_security_group"></a> [redshift\_security\_group](#module\_redshift\_security\_group) | terraform-aws-modules/security-group/aws//modules/redshift | ~> 5.0 |
| <a name="module_redshift_security_group_label"></a> [redshift\_security\_group\_label](#module\_redshift\_security\_group\_label) | cloudposse/label/null | 0.25.0 |
| <a name="module_secret_landing"></a> [secret\_landing](#module\_secret\_landing) | terraform-aws-modules/secrets-manager/aws | 1.1.2 |
| <a name="module_secret_landing_label"></a> [secret\_landing\_label](#module\_secret\_landing\_label) | cloudposse/label/null | 0.25.0 |

## Resources

| Name | Type |
|------|------|
| [aws_lakeformation_permissions.curated](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lakeformation_permissions) | resource |
| [aws_lakeformation_permissions.raw](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lakeformation_permissions) | resource |
| [aws_s3_object.job_script](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_object) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | AWS region to create resources in | `string` | `"us-east-2"` | no |
| <a name="input_business_unit"></a> [business\_unit](#input\_business\_unit) | ID element. Usually the team name, owners of the resources, e.g. 'finance' or 'analytics'. | `string` | `"analytics"` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | ID element. Usually used for AWS Region e.g. 'ue1' from 'us-east-2', OR role 'prd', 'tst', or 'dev' | `string` | `"dev"` | no |
| <a name="input_glue_version"></a> [glue\_version](#input\_glue\_version) | The version of glue to use | `string` | `"4.0"` | no |
| <a name="input_name"></a> [name](#input\_name) | ID element. Usually the component or solution name, e.g. 'app' or 'jenkins'.<br>This is the only ID element not also included as a `tag`.<br>The "name" tag is set to the full `id` string. There is no tag with the value of the `name` input. | `string` | `"datap"` | no |
| <a name="input_namespace"></a> [namespace](#input\_namespace) | ID element. Usually an abbreviation of the organization name, e.g. 'aws' or 'amz', to help ensure generated IDs are globally unique | `string` | `"wo"` | no |
| <a name="input_redshift_subnets"></a> [redshift\_subnets](#input\_redshift\_subnets) | List of redshift subnets | `list(string)` | <pre>[<br>  "subnet-01f4a101b9837d65b",<br>  "subnet-0e4916ddf091ba8b3",<br>  "subnet-02c514951d6a5fea0"<br>]</pre> | no |
| <a name="input_region"></a> [region](#input\_region) | ID element. Usually used for AWS Regions e.g. 'ue1' from 'us-east-2' (North Virginia), or 'uw2' from 'us-west-2' (Ohio) | `string` | `"ue2"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Additional tags (e.g. `{'BusinessUnit': 'XYZ'}`).<br>Neither the tag keys nor the tag values will be modified by this module. | `map(string)` | <pre>{<br>  "BusinessCostUnit": "1234"<br>}</pre> | no |
| <a name="input_vpc_cidr"></a> [vpc\_cidr](#input\_vpc\_cidr) | CIDR block for VPC | `string` | `"10.83.192.0/24"` | no |
| <a name="input_vpc_id"></a> [vpc\_id](#input\_vpc\_id) | VPC ID | `string` | `"vpc-0362f5bbee93ea82a"` | no |

## Outputs

No outputs.
