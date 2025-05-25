variable "aws_region" {
  description = "AWS region to create resources in"
  type        = string
  validation {
    condition     = contains(["ap-southeast-2", "us-east-2"], var.aws_region)
    error_message = "Invalid AWS region value. Allowed values are ap-southeast-2 and us-east-2."
  }
}

variable "aws_ohio_region" {
  description = "AWS region to create resources in"
  type        = string
  validation {
    condition     = contains(["ap-southeast-2", "us-east-2"], var.aws_ohio_region)
    error_message = "Invalid AWS region value. Allowed values are ap-southeast-2 and us-east-2."
  }
}


variable "tfc_aws_dynamic_credentials" {
  description = "Object containing AWS dynamic credentials configuration"
  type = object({
    default = object({
      shared_config_file = string
    })
    aliases = map(object({
      shared_config_file = string
    }))
  })
}


variable "namespace" {
  type        = string
  description = "ID element. Usually an abbreviation of the organization name, e.g. 'aws' or 'amz', to help ensure generated IDs are globally unique"
  default     = "worley"
}

variable "stage" {
  description = "ID element. Usually used for AWS Regions e.g. 'ohio' from 'us-east-2' (Ohio), or 'sydney' from 'ap-southeast-2' (Sydney)"
  type        = string

  validation {
    condition     = contains(["ohio", "sydney"], var.stage)
    error_message = "Invalid AWS region value. Allowed values are ohio, sydney."
  }
}

variable "ohio_stage" {
  description = "ID element. Usually used for AWS Regions e.g. 'ohio' from 'us-east-2' (Ohio), or 'sydney' from 'ap-southeast-2' (Sydney)"
  type        = string

  validation {
    condition     = contains(["ohio", "sydney"], var.ohio_stage)
    error_message = "Invalid AWS region value. Allowed values are ohio, sydney."
  }
}

variable "environment" {
  description = "ID element. Usually where resources will be deployed 'prd', 'qa', or 'dev'"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "prd"], var.environment)
    error_message = "Invalid environment value. Allowed values are dev, qa, or prd."
  }
}

variable "business_unit" {
  type        = string
  default     = "analytics"
  description = "ID element. Usually the team name, owners of the resources, e.g. 'finance' or 'analytics'."
}

variable "name" {
  type        = string
  default     = "datalake"
  description = <<-EOT
    ID element. Usually the component or solution name, e.g. 'app' or 'jenkins'.
    This is the only ID element not also included as a `tag`.
    The "name" tag is set to the full `id` string. There is no tag with the value of the `name` input.
    EOT
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = <<-EOT
    Additional tags (e.g. `{'BusinessUnit': 'XYZ'}`).
    Neither the tag keys nor the tag values will be modified by this module.
    EOT
}

# VPC

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "ohio_vpc_id" {
  type        = string
  description = "VPC ID"
}


# GLUE

variable "glue_version" {
  type        = string
  description = "The version of glue to use"
  default     = "4.0"
}

variable "glue_subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs for glue connections."
}

variable "glue_availability_zones" {
  type        = list(string)
  description = "List of subnet availability_zones for glue connections."
}

# QUICKSIGHT

variable "quicksight_account_name" {
  type        = string
  description = "Quicksight Account Name. Must be globally unique."
}

variable "quicksight_email" {
  type        = string
  description = "Email address that you want Amazon QuickSight to send notifications to regarding your Amazon QuickSight account or Amazon QuickSight subscription."
}

variable "quicksight_admin_group" {
  type        = list(string)
  description = "Quicksight Admin group associated with Identity Center"
  default     = ["Test_Data_Consumer"]
}

variable "quicksight_author_group" {
  type        = list(string)
  description = "Quicksight Author group associated with Identity Center"
  default     = ["Test_Data_Consumer"]
}

variable "quicksight_reader_group" {
  type        = list(string)
  description = "Quicksight Reader group associated with Identity Center"
  default     = ["Test_Dac_Business_User", "Test_Vg_Business_User"]
}

variable "emr_studio_admin_group" {
  type = string
}


variable "create_quicksight_vpc_connection" {
  type    = bool
  default = true
}

variable "create_redshift_idc_application" {
  type    = bool
  default = false

}

variable "update_identity_propagation_config" {
  type    = bool
  default = false
}

# datasync
variable "datasync_location_s3_subdirectory" {
  type        = string
  description = "AWS S3 Bucket path"
  default     = "people/hcm_extracts/raw"
}

# variable "file_system_id" {
#   type        = string
#   description = "File system id"
# }

variable "datasync_location_nfs_subdirectory" {
  type        = string
  description = "NFS path"
  default     = "/"
}

variable "datasync_task_options" {
  type        = map(any)
  description = "A map of datasync_task options block"
  default = {
    verify_mode            = "ONLY_FILES_TRANSFERRED"
    posix_permissions      = "NONE"
    preserve_deleted_files = "REMOVE"
    uid                    = "NONE"
    gid                    = "NONE"
    atime                  = "NONE"
    mtime                  = "NONE"
    bytes_per_second       = "-1"
    log_level              = "TRANSFER"
    transfer_mode          = "CHANGED"
    overwrite_mode         = "ALWAYS"
    object_tags            = "NONE"
  }
}

variable "datasync_vpc_endpoint_ip" {
  type        = string
  description = "The IP address of the VPC endpoint the agent should connect to when retrieving an activation key during resource creation"
}

# Uncomment below when locking down S3 bucketm for GenAI.
# variable "tfc_admin_role" {
#   type        = string
#   description = "TFC Admin Role ID to whitelist Terraform activities on GenAI bucket"
# }


variable "redshift_idc_app" {
  type        = bool
  description = "Condition to deploy Redshift IDC app"
  default     = false
}


variable "enable_centralized_observability" {
  type        = bool
  description = "Condition to deploy centralized observability"
  default     = false
}

variable "airflow_openlineage_http_configuration" {
  type        = string
  description = "HTTP endpoint for airflow open lineage configuration"
}

variable "openlineage_url" {
  type        = string
  description = "URL for open lineage glue job configuration"
}

variable "create_redshift_serverless" {
  type    = bool
  default = false
}


variable "redshift_cluster_identifier" {
  description = "The identifier of the Redshift cluster"
  type        = string
  validation {
    condition     = length(var.redshift_cluster_identifier) > 0
    error_message = "Redshift cluster identifier cannot be empty."
  }
}


variable "airflow_size" {
  type        = string
  description = "size of mwaa environment_class"
}

variable "sys_int_account_id" {
  type        = string
  default     = "626635445084"
  description = "Target AWS account ID where System Integration event bus exists Account ID - Dev->626635445084, QA->147997124047, UAT->147997124047, PRD->440744239630"
}


variable "source_event_pattern" {
  description = "Event pattern for source events"
  type = object({
    source      = list(string)
    detail-type = list(string)
  })
  default = {
    source      = ["data-platform"]
    detail-type = ["order-created", "order-updated"]
  }
}

variable "dlq_retention_days" {
  description = "Number of days to retain messages in DLQ"
  type        = number
  default     = 14
}

variable "data_platform_engineer_role" {
  description = "Data platform engineer role name"
  type        = string
}

variable "ami_id" {
  type = string
  description = "ami used by ec2 instance"
}


variable "ec2_subnet_id" {
  type        = string
  description = "Subnet ID for airflow ec2 instance"
}

variable "entra_client_id" {
  type = string
  description = "Client ID for Entra ID"
}

variable "entra_tenant_id" {
  type = string
  description = "Tenant ID for Entra ID"
  default = "73ad6539-b4fe-429c-97b6-fbc1b6ada80b"
}

variable "apigw_domain_name" {
  type = string
  description = "api gateway domain name"
}

variable "apigw_certificate_arn" {
  type = string
  description = "cert ARN for custom domain"
}
variable "genai_sharepoint_document_library_job" {
  type        = string
  description = "GenAI sharepoint document library job"
  default     = "dev"
}
variable "genai_sharepoint_list_library_job" {
  type        = string
  description = "GenAI sharepoint list library job"
  default     = "dev"
}
variable "datasync_location_object_storage_server_name" {
  type        = string
  description = "DataSync source location OCI server name"
  default     = "wrly01.compat.objectstorage.us-ashburn-1.oraclecloud.com"
}
variable "datasync_location_object_storage_bucket_name" {
  type        = string
  description = "DataSync source location OCI Bucket Name"
  default     = "wrly-erpcloudtest-bk"
}
variable "datasync_location_object_storage_subdirectory" {
  type        = string
  description = "DataSync source location OCI Bucket subdirectory"
  default     = "/"
}