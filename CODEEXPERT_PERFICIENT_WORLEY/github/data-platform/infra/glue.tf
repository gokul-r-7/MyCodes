module "glue_connection_security_group" {
  source  = "cloudposse/security-group/aws"
  version = "2.2.0"

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

  attributes = ["glue", "connection", "sg"]
  context    = module.label.context
}

#module "glue_connection" {
#  source  = "cloudposse/glue/aws//modules/glue-connection"
#  version = "0.4.0"

#  connection_description = "Glue connection to Postgres database"
#  connection_type        = "NETWORK"
#  connection_properties  = {}

#  physical_connection_requirements = {
#    security_group_id_list = [module.glue_connection_security_group.id]
#    availability_zone      = data.aws_subnet.private_glue.availability_zone
#    subnet_id              = data.aws_subnet.private_glue.id
#  }

#  attributes = ["glue", "connection"]
#  context    = module.label.context
#}


module "glue_connection" {
  source  = "cloudposse/glue/aws//modules/glue-connection"
  version = "0.4.0"
  count   = length(var.glue_subnet_ids)

  connection_description = count.index == 0 ? "Glue connection to Postgres database" : "Glue connection to Postgres database ${count.index}"
  connection_type        = "NETWORK"
  connection_properties  = {}

  physical_connection_requirements = {
    security_group_id_list = [module.glue_connection_security_group.id]
    availability_zone      = var.glue_availability_zones[count.index]
    subnet_id              = var.glue_subnet_ids[count.index]
  }

  # For the naming: keep the first instance as-is, add a suffix for others
  attributes = count.index == 0 ? ["glue", "connection"] : ["glue", "connection", tostring(count.index)]
  context    = module.label.context
}

module "glue_security_config_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  context    = module.label.context
  attributes = ["glue", "security", "configuration", "kms"]
  delimiter  = "_"
}

resource "aws_glue_security_configuration" "buckets_kms_security_config" {
  name = module.glue_security_config_label.id

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = module.kms_key.key_arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = module.kms_key.key_arn
    }

    s3_encryption {
      kms_key_arn        = module.kms_key_primary.key_arn
      s3_encryption_mode = "SSE-KMS"
    }
  }
}

################################################################################
# Glue Ecosys
################################################################################
# as there is domain specific new version so disabling old one as it is not being used
#module "glue_catalog_database_ecosys_label" {
#  source  = "cloudposse/label/null"
#  version = "0.25.0"
#
#  attributes = ["glue", "catalog", "database", "ecosys"]
#  context    = module.label.context
#  delimiter  = "_"
#}

module "glue_catalog_database_ecosys_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "project", "control", "ecosys"]
  context    = module.label.context
  delimiter  = "_"
}


#module "glue_catalog_database_ecosys" {
  # TODO:: delete this when refactor is completed
#  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
#  version = "0.4.0"
#
#  catalog_database_name        = module.glue_catalog_database_ecosys_label.id
#  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for Ecosys Application"
#  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"
#
#  attributes = ["glue", "catalog", "database", "ecosys"]
#  context    = module.label.context
#
#}

module "glue_catalog_database_ecosys_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_ecosys_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for ecosys Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "project", "control", "ecosys", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_ecosys_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_ecosys_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for ecosys Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "project", "control", "ecosys", "curated"]
  context    = module.label.context

}

#module "glue_crawler_raw_ecosys" {
  #Todo :: delete this after refactor work
#  source  = "cloudposse/glue/aws//modules/glue-crawler"
#  version = "0.4.0"
#
#  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/ecosys and writes the metadata into a Glue Catalog database"
#  database_name          = module.glue_catalog_database_ecosys.name
#  role                   = module.glue_service_iam_role.name
#  schedule               = "cron(0 1 * * ? *)"
#  table_prefix           = "raw_"
#  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
#
#  schema_change_policy = {
#    delete_behavior = "LOG"
#    update_behavior = null
#  }
#
#  s3_target = [{
#    path = "s3://${module.bucket_raw.s3_bucket_id}/ecosys/data_sampling"
#  }]
#
#
#  attributes = ["glue", "crawler", "raw", "ecosys"]
#  context    = module.label.context
#
#}

module "glue_crawler_raw_ecosys_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/ecosys and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_ecosys_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/project_control/ecosys/data_sampling"
  }]


  attributes = ["glue", "crawler", "project", "control", "ecosys", "raw"]
  context    = module.label.context

}


################################################################################
# Glue Oracle P6
################################################################################

module "glue_catalog_database_p6_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "oracle", "p6"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_p6_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "project", "control", "oracle", "p6"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_p6" {
  # TODO:: delete this when refactor is completed
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_p6_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for Oracle P6 Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "oracle", "p6"]
  context    = module.label.context

}

module "glue_catalog_database_p6_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_p6_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for Oracle P6 Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "project", "control", "oracle", "p6", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_p6_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_p6_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for Oracle P6 Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "project", "control", "oracle", "p6", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_p6" {
  # Todo :: delete this after refactor work
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/oracle_p6 and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_p6_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/oracle_p6/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "oracle", "p6"]
  context    = module.label.context

}

module "glue_crawler_raw_p6_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/oracle_p6 and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_p6_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/project_control/oracle_p6/data_sampling"
  }]


  attributes = ["glue", "crawler", "project", "control", "oracle", "p6", "raw"]
  context    = module.label.context

}

################################################################################
# CSV and XLSX File extracts - Begin
################################################################################

module "glue_catalog_database_oracle_gbs_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "oracle_gbs"]
  context    = module.label.context
  delimiter  = "_"
}

#added as part of oracle GBS refactor
module "glue_catalog_database_oracle_gbs_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "finance", "oracle", "gbs"]
  context    = module.label.context
  delimiter  = "_"
}

#delete later
module "glue_catalog_database_oracle_gbs" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_oracle_gbs_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for oracle_gbs Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "oracle_gbs"]
  context    = module.label.context

}

#added as part of oracle GBS refactor
module "glue_catalog_database_oracle_gbs_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_oracle_gbs_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for oracle_gbs Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "finance", "oracle", "gbs", "raw"]
  context    = module.label.context

}

#added as part of oracle GBS refactor
module "glue_catalog_database_oracle_gbs_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_oracle_gbs_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for oracle_gbs Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "finance", "oracle", "gbs", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_oracle_gbs" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/oracle_gbs and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_oracle_gbs.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/oracle_gbs/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "oracle_gbs"]
  context    = module.label.context

}

#added as part of oracle GBS refactor
module "glue_crawler_raw_oracle_gbs_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/oracle_gbs and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_oracle_gbs_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/finance/oracle_gbs/data_sampling"
  }]


  attributes = ["glue", "crawler", "finance", "oracle", "gbs", "raw"]
  context    = module.label.context

}


#added as part of sharepoint GBS refactor
module "glue_crawler_raw_sharepoint_gbs_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/sharepoint_gbs and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_oracle_gbs_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/finance/sharepoint_gbs/data_sampling"
  }]

  configuration = jsonencode(
    {
      Grouping = {
        TableLevelConfiguration = 5
      }
      Version = 1
    }
  )


  attributes = ["glue", "crawler", "finance", "sharepoint", "gbs", "raw"]
  context    = module.label.context

}


module "glue_catalog_database_assurance_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "assurance"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_assurance" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_assurance_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for assurance Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "assurance"]
  context    = module.label.context

}

module "glue_crawler_raw_assurance" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/assurance and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_assurance.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/assurance/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "assurance"]
  context    = module.label.context

}

module "glue_catalog_database_vg_sharepoint_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "vg", "sharepoint"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_vg_sharepoint" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_vg_sharepoint_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for Vg Sharepoint Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "vg", "sharepoint"]
  context    = module.label.context

}

module "glue_crawler_raw_vg_sharepoint" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/vg/sharepoint and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_vg_sharepoint.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/vg/sharepoint/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "vg", "sharepoint"]
  context    = module.label.context

}

module "glue_catalog_database_vg_e3d_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "vg", "e3d"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_vg_e3d" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_vg_e3d_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for Vg E3d Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "vg", "e3d"]
  context    = module.label.context

}

module "glue_crawler_raw_vg_e3d" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/vg/e3d and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_vg_e3d.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/vg/e3d/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "vg", "e3d"]
  context    = module.label.context

}

################################################################################
# CSV and XLSX File extracts - End
################################################################################

################################################################################
# Glue Aconex -> to be replaced with domain specific So disabling below resource
################################################################################

#module "glue_catalog_database_aconex_label" {
#  source  = "cloudposse/label/null"
#  version = "0.25.0"
#
#  attributes = ["glue", "catalog", "database", "aconex"]
#  context    = module.label.context
#  delimiter  = "_"
#}
#
#
#module "glue_catalog_database_aconex" {
#  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
#  version = "0.4.0"
#
#  catalog_database_name        = module.glue_catalog_database_aconex_label.id
#  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for Aconex Application"
#  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"
#
#  attributes = ["glue", "catalog", "database", "aconex"]
#  context    = module.label.context
#
#}
#
#module "glue_crawler_raw_aconex" {
#  source  = "cloudposse/glue/aws//modules/glue-crawler"
#  version = "0.4.0"
#
#  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/aconex and writes the metadata into a Glue Catalog database"
#  database_name          = module.glue_catalog_database_aconex.name
#  role                   = module.glue_service_iam_role.name
#  schedule               = "cron(0 1 * * ? *)"
#  table_prefix           = "raw_"
#  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
#
#  schema_change_policy = {
#    delete_behavior = "LOG"
#    update_behavior = null
#  }
#
#  s3_target = [{
#    path = "s3://${module.bucket_raw.s3_bucket_id}/aconex/data_sampling"
#  }]
#
#
#  attributes = ["glue", "crawler", "raw", "aconex"]
#  context    = module.label.context
#
#}

################################################################################
# Glue OMIE
################################################################################

module "glue_catalog_database_omie_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "omie"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_omie_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "omie"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_omie" {
  # TODO:: delete this when refactor is completed
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_omie_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for OMIE Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "omie"]
  context    = module.label.context

}

module "glue_catalog_database_omie_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_omie_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for omie Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "omie", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_omie_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_omie_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for omie Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "omie", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_omie" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/omie and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_omie.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/omie/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "omie"]
  context    = module.label.context

}

module "glue_crawler_raw_omie_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/omie and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_omie_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/omie/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "omie", "raw"]
  context    = module.label.context

}


################################################################################
# Glue AIM
################################################################################

module "glue_catalog_database_aim_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "aim"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_aim_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "aim"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_aim" {
  # TODO:: delete this when refactor is completed
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_aim_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for AIM Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "aim"]
  context    = module.label.context

}

module "glue_catalog_database_aim_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_aim_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for aim Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "aim", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_aim_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_aim_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for aim Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "aim", "curated"]
  context    = module.label.context

}


module "glue_crawler_raw_aim" {
  # Todo :: delete this after refactor work
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/aim and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_aim.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  configuration = jsonencode(
    {
      Version = 1.0,
      CrawlerOutput = {
        Tables = { AddOrUpdateBehavior = "MergeNewColumns"}
        Partitions = { AddOrUpdateBehavior = "InheritFromTable"}
      },
      Grouping = { TableLevelConfiguration = 5 }
    }
  )

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/aim/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "aim"]
  context    = module.label.context

}

module "glue_crawler_raw_aim_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/aim and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_aim_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  configuration = jsonencode(
    {
      Version = 1.0,
      CrawlerOutput = {
        Tables = { AddOrUpdateBehavior = "MergeNewColumns"}
        Partitions = { AddOrUpdateBehavior = "InheritFromTable"}
      },
      Grouping = { TableLevelConfiguration = 5 }
    }
  )

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/aim/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "aim", "raw"]
  context    = module.label.context

}


################################################################################
# Glue SPEL
################################################################################

module "glue_catalog_database_spel_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "spel"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_spel_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "spel"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_spel" {
  # TODO:: delete this when refactor is completed
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_spel_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for spel Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "spel"]
  context    = module.label.context

}

module "glue_catalog_database_spel_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_spel_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for spel Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "spel", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_spel_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_spel_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for spel Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "spel", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_spel" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/spel and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_spel.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/spel/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "spel"]
  context    = module.label.context

}

module "glue_crawler_raw_spel_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/spel and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_spel_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/spel/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "spel", "raw"]
  context    = module.label.context

}


#################################################################################
# Glue ERM
#################################################################################

module "glue_catalog_database_erm_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "erm"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_erm_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "supply", "chain", "erm"]
  context    = module.label.context
  delimiter  = "_"

}

module "glue_catalog_database_erm" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_erm_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for ERM Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "erm"]
  context    = module.label.context

}

module "glue_catalog_database_erm_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_erm_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for erm Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "supply", "chain", "erm", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_erm_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_erm_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for erm Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "supply", "chain", "erm", "curated"]
  context    = module.label.context

}


module "glue_crawler_raw_erm" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/erm and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_erm.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/erm/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "erm"]
  context    = module.label.context

}

module "glue_crawler_raw_erm_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/erm and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_erm_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/supply_chain/erm/data_sampling"
  }]


  attributes = ["glue", "crawler", "supply", "chain", "erm", "raw"]
  context    = module.label.context

}

#################################################################################
# Glue Data Catalog People Domain
#################################################################################

module "glue_catalog_database_people_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "people", "oracle", "hcm", "extracts"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_people_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_people_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw_people_domain.s3_bucket_id} for people hcm extracts"
  location_uri                 = "s3://${module.bucket_raw_people_domain.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "people", "oracle",  "hcm_extracts", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_people_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_people_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_curated_people_domain.s3_bucket_id} for people hcm extracts"
  location_uri                 = "s3://${module.bucket_curated_people_domain.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "people", "oracle", "hcm_extracts", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_people" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw_people_domain.s3_bucket_id}/people/oracle/hcm_extracts and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_people_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw_people_domain.s3_bucket_id}/people/oracle/hcm_extracts/data_sampling"
  }]


  attributes = ["glue", "crawler", "people", "oracle", "hcm", "extracts", "raw"]
  context    = module.label.context

}

#################################################################################
# Glue Non People Domain
#################################################################################

module "glue_catalog_database_nonpeople_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "people", "nonoracle", "data", "extracts"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_nonpeople_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_nonpeople_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw_people_domain.s3_bucket_id} for non oracle data extracts"
  location_uri                 = "s3://${module.bucket_raw_people_domain.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "people", "nonoracle", "data_extracts", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_nonpeople_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_nonpeople_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_curated_people_domain.s3_bucket_id} for non oracle data extracts"
  location_uri                 = "s3://${module.bucket_curated_people_domain.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "people", "nonoracle", "data_extracts", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_nonpeople" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw_people_domain.s3_bucket_id}/people/non_oracle/data_extracts and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_nonpeople_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw_people_domain.s3_bucket_id}/people/non_oracle/data_extracts/data_sampling"
  }]


  attributes = ["glue", "crawler", "people", "nonoracle", "data_extracts", "raw"]
  context    = module.label.context

}

################################################################################
# GenAI Glue Connection for Aurora Postgres
################################################################################
module "genai_glue_connection_security_group" {
  source  = "cloudposse/security-group/aws"
  version = "2.2.0"

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
      description = "All ports from self"
    }
  ]

  attributes = ["genai", "glue", "connection", "sg"]
  context    = module.label.context
}

module "genai_glue_connection" {
  source  = "cloudposse/glue/aws//modules/glue-connection"
  version = "0.4.0"

  connection_description = "GenAI Glue connection to Postgres database"
  connection_type        = "NETWORK"
  connection_properties  = {}

  physical_connection_requirements = {
    security_group_id_list = [module.genai_glue_connection_security_group.id]
    availability_zone      = data.aws_subnet.private_glue.availability_zone
    subnet_id              = data.aws_subnet.private_glue.id
  }

  attributes = ["genai", "glue", "connection"]
  context    = module.label.context
}


################################################################################
# Glue SPID
################################################################################

module "glue_catalog_database_spid_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "spid"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_spid_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "spid"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_spid" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_spid_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for spid Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "spid"]
  context    = module.label.context

}

module "glue_catalog_database_spid_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_spid_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for spid Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "spid", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_spid_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_spid_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for spid Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "spid", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_spid" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/spid and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_spid.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/spid/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "spid"]
  context    = module.label.context

}

module "glue_crawler_raw_spid_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/spid and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_spid_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/spid/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "spid", "raw"]
  context    = module.label.context

}

################################################################################
# Glue PDM
################################################################################

module "glue_catalog_database_pdm_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "pdm"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_pdm_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "pdm"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_pdm" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_pdm_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for pdm Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "pdm"]
  context    = module.label.context

}

module "glue_catalog_database_pdm_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_pdm_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for pdm Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "pdm", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_pdm_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_pdm_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for pdm Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "pdm", "curated"]
  context    = module.label.context

}


module "glue_crawler_raw_pdm" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/pdm and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_pdm.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/pdm/data_sampling"
  }]

  configuration = jsonencode(
    {
      Version = 1.0,
      CrawlerOutput = {
        Tables = { AddOrUpdateBehavior = "MergeNewColumns"}
        Partitions = { AddOrUpdateBehavior = "InheritFromTable"}
      },
      Grouping = { TableLevelConfiguration = 5 }
    }
  )

  attributes = ["glue", "crawler", "raw", "pdm"]
  context    = module.label.context

}

module "glue_crawler_raw_pdm_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/pdm and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_pdm_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  configuration = jsonencode(
    {
      Version = 1.0,
      CrawlerOutput = {
        Tables = { AddOrUpdateBehavior = "MergeNewColumns"}
        Partitions = { AddOrUpdateBehavior = "InheritFromTable"}
      },
      Grouping = { TableLevelConfiguration = 5 }
    }
  )

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/pdm/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "pdm", "raw"]
  context    = module.label.context

}

################################################################################
# Glue SPI
################################################################################

module "glue_catalog_database_spi_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "spi"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_spi_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "spi"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_spi" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_spi_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for spi Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "spi"]
  context    = module.label.context

}

module "glue_catalog_database_spi_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_spi_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for spi Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "spi", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_spi_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_spi_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for spi Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "spi", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_spi" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/spi and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_spi.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/spi/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "spi"]
  context    = module.label.context

}

module "glue_crawler_raw_spi_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/spi and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_spi_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/spi/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "spi", "raw"]
  context    = module.label.context

}


################################################################################
# Glue MEM
################################################################################

module "glue_catalog_database_mem_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "mem"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_mem_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "mem"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_mem" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_mem_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for mem Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "mem"]
  context    = module.label.context

}

module "glue_catalog_database_mem_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_mem_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for mem Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "mem", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_mem_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_mem_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for mem Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "mem", "curated"]
  context    = module.label.context

}


module "glue_crawler_raw_mem" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/mem and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_mem.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/mem/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "mem"]
  context    = module.label.context

}

module "glue_crawler_raw_mem_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/mem and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_mem_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/mem/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "mem", "raw"]
  context    = module.label.context

}


################################################################################
# Glue MTR
################################################################################

module "glue_catalog_database_mtr_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "mtr"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_mtr" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_mtr_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for mtr Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "mtr"]
  context    = module.label.context

}

module "glue_crawler_raw_mtr" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/mtr and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_mtr.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/mtr/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "mtr"]
  context    = module.label.context

}

################################################################################
# Glue CSP SALESFORCE
################################################################################
#To be deleted after refactor changes
#module "glue_catalog_database_csp_salesforce_label" {
#  source  = "cloudposse/label/null"
#  version = "0.25.0"
#
#  attributes = ["glue", "catalog", "database", "csp", "salesforce"]
#  context    = module.label.context
#  delimiter  = "_"
#}

module "glue_catalog_database_csp_salesforce_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "customer", "csp", "salesforce"]
  context    = module.label.context
  delimiter  = "_"
}

#To be deleted after refactor changes
#module "glue_catalog_database_csp_salesforce" {
#  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
#  version = "0.4.0"
#
#  catalog_database_name        = module.glue_catalog_database_csp_salesforce_label.id
#  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for CSP Salesforce Application"
#  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"
#
#  attributes = ["glue", "catalog", "database", "csp", "salesforce"]
#  context    = module.label.context
#
#}

module "glue_catalog_database_csp_salesforce_raw" {
  source                       = "cloudposse/glue/aws//modules/glue-catalog-database"
  version                      = "0.4.0"
  catalog_database_name        = "${module.glue_catalog_database_csp_salesforce_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for CSP Salesforce Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "customer", "csp", "salesforce"]
  context    = module.label.context

}

module "glue_catalog_database_csp_salesforce_curated" {
  source                       = "cloudposse/glue/aws//modules/glue-catalog-database"
  version                      = "0.4.0"
  catalog_database_name        = "${module.glue_catalog_database_csp_salesforce_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for CSP Salesforce Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "customer", "csp", "salesforce"]
  context    = module.label.context

}

#To be deleted after refactor changes
#module "glue_crawler_raw_csp_salesforce" {
#  source  = "cloudposse/glue/aws//modules/glue-crawler"
#  version = "0.4.0"
#
#  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/oracle_p6 and writes the metadata into a Glue Catalog database"
#  database_name          = module.glue_catalog_database_csp_salesforce.name
#  role                   = module.glue_service_iam_role.name
#  schedule               = "cron(0 1 * * ? *)"
#  table_prefix           = "raw_"
#  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id
#
#  schema_change_policy = {
#    delete_behavior = "LOG"
#    update_behavior = null
#  }
#
#  s3_target = [{
#    path = "s3://${module.bucket_raw.s3_bucket_id}/csp_salesforce/data_sampling"
#  }]
#
#
#  attributes = ["glue", "crawler", "raw", "csp", "salesforce"]
#  context    = module.label.context
#
#}

module "glue_crawler_raw_csp_salesforce_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/customer/csp_salesforce and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_csp_salesforce_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/customer/csp_salesforce/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "customer", "csp", "salesforce"]
  context    = module.label.context

}

################################################################################
# Glue FLA
################################################################################

module "glue_catalog_database_fla_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "fla"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_fla_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "construction", "fla"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_fla" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_fla_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for fla Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "fla"]
  context    = module.label.context

}

module "glue_catalog_database_fla_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_fla_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for fla Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "fla", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_fla_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_fla_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for fla Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "fla", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_fla" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/fla and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_fla.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/fla/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "fla"]
  context    = module.label.context

}

module "glue_crawler_raw_fla_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/fla and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_fla_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/construction/fla/data_sampling"
  }]


  attributes = ["glue", "crawler", "construction", "fla", "raw"]
  context    = module.label.context

}

################################################################################
# Glue O3
################################################################################

module "glue_catalog_database_o3_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "o3"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_o3_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "construction", "o3"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_circuit_breaker_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "circuit" , "breaker", "hexagon"]
  context    = module.label.context
  delimiter  = "_"
}


# module "glue_catalog_database_o3" {
#   # TODO:: delete this when refactor is compleated 
#   source  = "cloudposse/glue/aws//modules/glue-catalog-database"
#   version = "0.4.0"

#   catalog_database_name        = module.glue_catalog_database_o3_label.id
#   catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for O3 Application"
#   location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

#   attributes = ["glue", "catalog", "database", "o3"]
#   context    = module.label.context

# }

module "glue_catalog_database_o3_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_o3_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for O3 Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "o3", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_circuit_breaker_hexagon_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_circuit_breaker_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for circuit breaker Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "circuit" , "breaker", "hexagon", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_o3_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_o3_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for O3 Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "o3", "curated"]
  context    = module.label.context

}

module "glue_catalog_database_circuit_breaker_hexagon_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_circuit_breaker_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for Circuit Breaker Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "circuit", "breaker", "hexagon", "curated"]
  context    = module.label.context

}


module "glue_crawler_raw_o3" {
  # Todo :: delete this after refactor work
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/o3 and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_o3_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/o3/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "o3"]
  context    = module.label.context

}

module "glue_crawler_raw_o3_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/o3 and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_o3_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/construction/o3/data_sampling"
  }]


  attributes = ["glue", "crawler", "construction", "o3", "raw"]
  context    = module.label.context

}

module "glue_crawler_raw_circuit_breaker" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/o3 and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_circuit_breaker_hexagon_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/circuit_breaker/data_sampling"
  }]


  attributes = ["glue", "crawler", "circuit", "breaker", "hexagon", "raw"]
  context    = module.label.context

}

################################################################################
# Glue FTS
################################################################################

module "glue_catalog_database_fts_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "fts"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_fts_label_v2" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "construction", "fts"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_fts" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_fts_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for fts Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "fts"]
  context    = module.label.context

}

module "glue_catalog_database_fts_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_fts_label_v2.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for fts Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "fts", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_fts_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_fts_label_v2.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} for fts Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "fts", "curated"]
  context    = module.label.context

}


module "glue_crawler_raw_fts" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/fts and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_fts.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/fts/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "fts"]
  context    = module.label.context

}

module "glue_crawler_raw_fts_v2" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/fts and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_fts_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/construction/fts/data_sampling"
  }]


  attributes = ["glue", "crawler", "construction", "fts", "raw"]
  context    = module.label.context

}


################################################################################
# Glue HEXAGON
################################################################################

module "glue_catalog_database_vg_hexagon_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "vg", "hexagon"]
  context    = module.label.context
  delimiter  = "_"
}


module "glue_catalog_database_vg_hexagon" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = module.glue_catalog_database_vg_hexagon_label.id
  catalog_database_description = "Glue Catalog database for the data located in ${module.bucket_raw.s3_bucket_id} for VG hexagon Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "vg", "hexagon"]
  context    = module.label.context

}

module "glue_crawler_raw_vg_hexagon" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/vg/hexagon and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_vg_hexagon.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/vg/hexagon/data_sampling"
  }]


  attributes = ["glue", "crawler", "raw", "vg", "hexagon"]
  context    = module.label.context

}

################################################################################
# Glue Document Control Aconex
################################################################################

module "glue_catalog_database_document_control_aconex_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "document", "control", "aconex"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_document_control_aconex_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_aconex_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for Aconex application for Document Control domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "aconex", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_document_control_aconex_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_aconex_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for Aconex application for Document Control domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "aconex", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_document_control_aconex" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/document_control/aconex/data_sampling and writes the metadata into a Glue Catalog Aconex raw database for Document Control domain"
  database_name          = module.glue_catalog_database_document_control_aconex_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/document_control/aconex/data_sampling"
  }]


  attributes = ["glue", "crawler", "document", "control", "aconex", "raw"]
  context    = module.label.context

}

################################################################################
# Glue Document Control Hexagon
################################################################################

module "glue_catalog_database_document_control_hexagon_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "document", "control", "hexagon"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_document_control_hexagon_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_hexagon_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for hexagon application for Document Control domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "hexagon", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_document_control_hexagon_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_hexagon_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for hexagon application for Document Control domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "hexagon", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_document_control_hexagon" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/document_control/hexagon/data_sampling and writes the metadata into a Glue Catalog hexagon raw database for Document Control domain"
  database_name          = module.glue_catalog_database_document_control_hexagon_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/document_control/hexagon/data_sampling"
  }]


  attributes = ["glue", "crawler", "document", "control", "hexagon", "raw"]
  context    = module.label.context

}

################################################################################
# Glue Engineering E3D
################################################################################

module "glue_catalog_database_engineering_e3d_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "e3d"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_engineering_e3d_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_e3d_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for e3d application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "e3d", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_engineering_e3d_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_e3d_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for e3d application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "e3d", "curated"]
  context    = module.label.context

}


module "glue_catalog_database_engineering_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering","transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_engineering_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Engineering Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "transformed"]
  context    = module.label.context

}

#added local transform db for engineering
resource "aws_glue_catalog_database" "glue_catalog_database_engineering_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_engineering_transformed_label.id}_local_development"
  
  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/engineering/"  

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

################################ Lake Formation Permissions ########################################

locals {
  lakeformation_permissions_environments = ["dev"]
  create_lakeformation_permissions   = contains(local.lakeformation_permissions_environments, var.environment) ? 1 : 0
}

resource "aws_lakeformation_permissions" "engineering_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_engineering_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_engineering_transformed_local_development]
}

module "glue_catalog_database_document_control_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "document", "control", "transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_document_control_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Document Control Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "transformed"]
  context    = module.label.context
}

resource "aws_glue_catalog_database" "glue_catalog_database_document_control_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_document_control_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/document_control/"

  create_table_default_permission {

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "document_control_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_document_control_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_document_control_transformed_local_development]
}


#Added transformed db for Supply Chain
module "glue_catalog_database_supply_chain_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "supply", "chain","transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_supply_chain_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_supply_chain_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Supply Chain Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "supply", "chain", "transformed"]
  context    = module.label.context

}

#added local db for supplychain
resource "aws_glue_catalog_database" "glue_catalog_database_supply_chain_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_supply_chain_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/supply_chain/"

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}
  

resource "aws_lakeformation_permissions" "supply_chain_local_dev_permissions" {

  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_supply_chain_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_supply_chain_transformed_local_development]
}

resource "aws_lakeformation_permissions" "supply_chain_local_dev_permissions_data_platform_engineer_role" {
  count = local.create_lakeformation_permissions
  principal = data.aws_iam_role.data_platform_engineer_role.arn

  permissions = ["ALL"]

  table {
    database_name = "${module.glue_catalog_database_supply_chain_transformed_label.id}_local_development"
    wildcard = true
  }

  depends_on = [aws_glue_catalog_database.glue_catalog_database_supply_chain_transformed_local_development]
}


module "glue_crawler_raw_engineering_e3d" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/engineering/e3d/data_sampling and writes the metadata into a Glue Catalog e3d raw database for Engineering domain"
  database_name          = module.glue_catalog_database_engineering_e3d_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/e3d/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "e3d", "raw"]
  context    = module.label.context

}

################################################################################
# Glue Document supplier_package
################################################################################

module "glue_catalog_database_document_control_supplier_package_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "document", "control", "supplier", "package"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_document_control_supplier_package_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_supplier_package_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for supplier_package application for document_control domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "supplier", "package", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_document_control_supplier_package_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_document_control_supplier_package_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for supplier_package application for document_control domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "document", "control", "supplier", "package", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_document_control_supplier_package" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/document_control/supplier_package/data_sampling and writes the metadata into a Glue Catalog supplier_package raw database for document_control domain"
  database_name          = module.glue_catalog_database_document_control_supplier_package_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/document_control/supplier_package/data_sampling"
  }]


  attributes = ["glue", "crawler", "document", "control", "supplier", "package", "raw"]
  context    = module.label.context

}
################################################################################
# Glue Engineering IsoTracker
################################################################################

module "glue_catalog_database_engineering_isotracker_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "isotracker"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_engineering_isotracker_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_isotracker_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for isotracker application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "isotracker", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_engineering_isotracker_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_isotracker_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for isotracker application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "isotracker", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_engineering_isotracker" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/engineering/isotracker/data_sampling and writes the metadata into a Glue Catalog isotracker raw database for Engineering domain"
  database_name          = module.glue_catalog_database_engineering_isotracker_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/isotracker/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "isotracker", "raw"]
  context    = module.label.context

}
################################################################################
# Glue Engineering mel
################################################################################

module "glue_catalog_database_engineering_mel_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "mel"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_engineering_mel_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_mel_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for mel application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "mel", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_engineering_mel_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_mel_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for mel application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "mel", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_engineering_mel" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/engineering/mel/data_sampling and writes the metadata into a Glue Catalog mel raw database for Engineering domain"
  database_name          = module.glue_catalog_database_engineering_mel_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  configuration = jsonencode(
    {
      Version = 1.0,
      CrawlerOutput = {
        Tables = { AddOrUpdateBehavior = "MergeNewColumns"}
        Partitions = { AddOrUpdateBehavior = "InheritFromTable"}
      },
      Grouping = { TableLevelConfiguration = 5 }
    }
  )

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/mel/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "mel", "raw"]
  context    = module.label.context

}
################################################################################
# Glue Engineering Engineering Register
################################################################################

module "glue_catalog_database_engineering_engreg_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "engreg"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_engineering_engreg_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_engreg_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for engreg application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "engreg", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_engineering_engreg_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_engreg_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for engreg application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "engreg", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_engineering_engreg" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/engineering/engreg/data_sampling and writes the metadata into a Glue Catalog engreg raw database for Engineering domain"
  database_name          = module.glue_catalog_database_engineering_engreg_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/engreg/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "engreg", "raw"]
  context    = module.label.context

}
################################################################################
# Glue Engineering Miscellaneous Pipe Support
################################################################################

module "glue_catalog_database_engineering_mps_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "engineering", "mps"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_engineering_mps_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_mps_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for mps application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "mps", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_engineering_mps_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_engineering_mps_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for mps application for Engineering domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "engineering", "mps", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_engineering_mps" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/engineering/mps/data_sampling and writes the metadata into a Glue Catalog mps raw database for Engineering domain"
  database_name          = module.glue_catalog_database_engineering_mps_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/engineering/mps/data_sampling"
  }]


  attributes = ["glue", "crawler", "engineering", "mps", "raw"]
  context    = module.label.context

}


################################################################################
# Glue health_safety_environment :- Assurance 
################################################################################

module "glue_catalog_database_health_safety_environment_assurance_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "health","safety","environment", "assurance"]
  context    = module.label.context
  delimiter  = "_"
} 

module "glue_catalog_database_health_safety_environment_assurance_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_health_safety_environment_assurance_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for assurance application for health_safety_environment domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "health","safety","environment", "assurance", "raw"]
  context    = module.label.context

} 

module "glue_catalog_database_health_safety_environment_assurance_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_health_safety_environment_assurance_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for assurance application for health_safety_environment domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "health","safety","environment", "assurance", "curated"]
  context    = module.label.context
} 

module "glue_crawler_raw_health_safety_environment_assurance" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/health_safety_environment/assurance/data_sampling and writes the metadata into a Glue Catalog assurance raw database for health_safety_environment domain"
  database_name          = module.glue_catalog_database_health_safety_environment_assurance_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  } 

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/health_safety_environment/assurance/data_sampling"
  }]


  attributes = ["glue", "crawler", "health","safety","environment", "assurance", "raw"]
  context    = module.label.context

} 

################################################################################
# DBT Glue  :- Construction
################################################################################

module "glue_catalog_database_construction_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "construction","transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_construction_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_construction_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Construction Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "construction", "transformed"]
  context    = module.label.context
}

#Added transformed local database for construction
resource "aws_glue_catalog_database" "glue_catalog_database_construction_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_construction_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/construction/"
  
  create_table_default_permission {

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "construction_local_dev_permissions" {
  count = local.create_lakeformation_permissions

  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_construction_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_construction_transformed_local_development]
}

################################################################################
# DBT Glue  :- Finance
################################################################################

module "glue_catalog_database_finance_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "finance","transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_finance_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_finance_transformed_label.id}" 
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Finance Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "finance", "transformed"]
  context    = module.label.context

}

resource "aws_glue_catalog_database" "glue_catalog_database_finance_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_finance_transformed_label.id}_local_development"

location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/finance/"

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}


resource "aws_lakeformation_permissions" "finance_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_finance_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_finance_transformed_local_development]
}

resource "aws_lakeformation_permissions" "finance_local_dev_permissions_data_platform_engineer_role" {
  count = local.create_lakeformation_permissions
  principal = data.aws_iam_role.data_platform_engineer_role.arn

  permissions = ["ALL"]

  table {
    database_name = "${module.glue_catalog_database_finance_transformed_label.id}_local_development"
    wildcard = true
  }

  depends_on = [aws_glue_catalog_database.glue_catalog_database_finance_transformed_local_development]
}


################################################################################
# DBT Glue  :- Project Control
################################################################################

module "glue_catalog_database_project_control_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "project", "control", "transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_project_control_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_project_control_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for project Control Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "project", "control", "transformed"]
  context    = module.label.context
}

resource "aws_glue_catalog_database" "glue_catalog_database_project_control_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_project_control_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/project_control/"

  create_table_default_permission {

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "project_control_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_project_control_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_project_control_transformed_local_development]
}


module "glue_catalog_database_dac_document_control_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "dac","document","control","transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_dac_document_control_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_dac_document_control_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for DAC Document Control Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "dac","document","control","transformed"]
  context    = module.label.context

}

#added local transform db for engineering
resource "aws_glue_catalog_database" "glue_catalog_database_dac_document_control_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_dac_document_control_transformed_label.id}_local_development"
  
  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/dac_document_control/"  

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "dac_document_control_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_dac_document_control_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_dac_document_control_transformed_local_development]
}

module "glue_catalog_database_dac_project_control_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "dac","project","control","transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_dac_project_control_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_dac_project_control_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for DAC project Control Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "dac","project","control","transformed"]
  context    = module.label.context

}

#added local transform db for engineering
resource "aws_glue_catalog_database" "glue_catalog_database_dac_project_control_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_dac_project_control_transformed_label.id}_local_development"
  
  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/dac_project_control/"  

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "dac_project_control_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_dac_project_control_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_dac_project_control_transformed_local_development]
}

################################################################################
# DBT Glue  :- Customer CSP Salesforce
################################################################################

module "glue_catalog_database_customer_csp_salesforce_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database","customer", "csp", "salesforce","transformed"]
  context    = module.label.context
  delimiter  = "_"
  
}

module "glue_catalog_database_customer_csp_salesforce_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_customer_csp_salesforce_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Customer CSP Salesforce Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "customer", "csp", "salesforce", "transformed"]
  context    = module.label.context
}

#Added transformed local database for customer csp_salesforce
resource "aws_glue_catalog_database" "glue_catalog_database_customer_csp_salesforce_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_customer_csp_salesforce_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/csp_salesforce1/"

  create_table_default_permission {

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "csp_salesforce_local_dev_permissions" {
  count = local.create_lakeformation_permissions

  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_customer_csp_salesforce_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_customer_csp_salesforce_transformed_local_development]
}

resource "aws_lakeformation_permissions" "csp_salesforce_local_dev_permissions_data_platform_engineer_role" {
  count = local.create_lakeformation_permissions
  principal = data.aws_iam_role.data_platform_engineer_role.arn

  permissions = ["ALL"]

  table {
    database_name = "${module.glue_catalog_database_customer_csp_salesforce_transformed_label.id}_local_development"
    wildcard = true
  }

  depends_on = [aws_glue_catalog_database.glue_catalog_database_customer_csp_salesforce_transformed_local_development]
}

################################################################################
# DBT Glue  :- Health Safety Environment
################################################################################

module "glue_catalog_database_health_safety_environment_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "health", "safety", "environment", "transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_health_safety_environment_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_health_safety_environment_transformed_label.id}" 
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Health Safety Environment Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "health", "safety", "environment", "transformed"]
  context    = module.label.context

}

resource "aws_glue_catalog_database" "glue_catalog_database_health_safety_environment_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_health_safety_environment_transformed_label.id}_local_development"

location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/health_safety_environment/"

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}


resource "aws_lakeformation_permissions" "health_safety_environment_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_health_safety_environment_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_health_safety_environment_transformed_local_development]
}

resource "aws_lakeformation_permissions" "health_safety_environment_local_dev_permissions_data_platform_engineer_role" {
  count = local.create_lakeformation_permissions
  principal = data.aws_iam_role.data_platform_engineer_role.arn

  permissions = ["ALL"]

  table {
    database_name = "${module.glue_catalog_database_health_safety_environment_transformed_label.id}_local_development"
    wildcard = true
  }

  depends_on = [aws_glue_catalog_database.glue_catalog_database_health_safety_environment_transformed_local_development]
}


################################################################################
# DBT Glue  :- People Domain
################################################################################

module "glue_catalog_database_people_link_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "people", "transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_people_link_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_people_link_transformed_label.id}" 
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed_people_domain.s3_bucket_id} for People Domain"
  location_uri                 = "s3://${module.bucket_transformed_people_domain.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "people", "transformed"]
  context    = module.label.context

}

resource "aws_glue_catalog_database" "glue_catalog_database_people_link_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_people_link_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed_people_domain.s3_bucket_id}/local_dev/people/"

  create_table_default_permission {
    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}


resource "aws_lakeformation_permissions" "people_link_local_dev_permissions" {
  count = local.create_lakeformation_permissions
  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_people_link_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_people_link_transformed_local_development]
}

resource "aws_lakeformation_permissions" "people_link_local_dev_permissions_data_platform_engineer_role" {
  count = local.create_lakeformation_permissions
  principal = data.aws_iam_role.data_platform_engineer_role.arn

  permissions = ["ALL"]

  table {
    database_name = "${module.glue_catalog_database_people_link_transformed_label.id}_local_development"
    wildcard = true
  }

  depends_on = [aws_glue_catalog_database.glue_catalog_database_people_link_transformed_local_development]
}


################################################################################
# DBT Glue  :- Circuit database
################################################################################

module "glue_catalog_database_circuit_breaker_transformed_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database","circuit", "breaker", "transformed"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_circuit_breaker_transformed" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_circuit_breaker_transformed_label.id}"
  catalog_database_description = "Glue Catalog database for the DIM data located in ${module.bucket_transformed.s3_bucket_id} for Circuit Breaker Domain"
  location_uri                 = "s3://${module.bucket_transformed.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "circuit", "breaker", "transformed"]
  context    = module.label.context
}

#Added transformed local database for circuit_breaker
resource "aws_glue_catalog_database" "glue_catalog_database_circuit_breaker_transformed_local_development" {
  count = var.environment == "dev" ? 1 : 0
  name = "${module.glue_catalog_database_circuit_breaker_transformed_label.id}_local_development"

  location_uri = "s3://${module.bucket_transformed.s3_bucket_id}/local_dev/circuit_breaker/"

  create_table_default_permission {

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}

resource "aws_lakeformation_permissions" "circuit_breaker_local_dev_permissions" {
  count = local.create_lakeformation_permissions

  principal = "IAM_ALLOWED_PRINCIPALS"

  database {
    name = "${module.glue_catalog_database_circuit_breaker_transformed_label.id}_local_development"
  }

  permissions = ["ALL"]

  depends_on = [aws_glue_catalog_database.glue_catalog_database_circuit_breaker_transformed_local_development]
}


#############################################################
################### Jplus O3 Integration ####################
#############################################################

module "glue_catalog_database_jplus_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "finance", "jplus"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_jplus_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_jplus_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} for jplus Application"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "finance", "jplus", "raw"]
  context    = module.label.context

}

module "glue_catalog_database_jplus_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_jplus_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_curated.s3_bucket_id} for jplus Application"
  location_uri                 = "s3://${module.bucket_curated.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "finance", "jplus", "curated"]
  context    = module.label.context

}

module "glue_crawler_raw_jplus" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data in ${module.bucket_raw.s3_bucket_id}/finance/jplus and writes the metadata into a Glue Catalog database"
  database_name          = module.glue_catalog_database_jplus_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/finance/jplus/data_sampling"
  }]


  attributes = ["glue", "crawler", "finance", "jplus", "raw"]
  context    = module.label.context

}


################################################################################
# Glue health_safety_environment :- adt 
################################################################################

module "glue_catalog_database_health_safety_environment_adt_label" {
  source  = "cloudposse/label/null"
  version = "0.25.0"

  attributes = ["glue", "catalog", "database", "health","safety","environment", "adt"]
  context    = module.label.context
  delimiter  = "_"
}

module "glue_catalog_database_health_safety_environment_adt_raw" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_health_safety_environment_adt_label.id}_raw"
  catalog_database_description = "Glue Catalog database for the raw data located in ${module.bucket_raw.s3_bucket_id} is for adt application for health_safety_environment domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "health","safety","environment", "adt", "raw"]
  context    = module.label.context

}


module "glue_catalog_database_health_safety_environment_adt_curated" {
  source  = "cloudposse/glue/aws//modules/glue-catalog-database"
  version = "0.4.0"

  catalog_database_name        = "${module.glue_catalog_database_health_safety_environment_adt_label.id}_curated"
  catalog_database_description = "Glue Catalog database for the curated data located in ${module.bucket_raw.s3_bucket_id} is for adt application for health_safety_environment domain"
  location_uri                 = "s3://${module.bucket_raw.s3_bucket_id}"

  attributes = ["glue", "catalog", "database", "health","safety","environment", "adt", "curated"]
  context    = module.label.context
}

module "glue_crawler_raw_health_safety_environment_adt" {
  source  = "cloudposse/glue/aws//modules/glue-crawler"
  version = "0.4.0"

  crawler_description    = "Glue crawler that processes data from ${module.bucket_raw.s3_bucket_id}/health_safety_environment/adt/data_sampling and writes the metadata into a Glue Catalog adt raw database for health_safety_environment domain"
  database_name          = module.glue_catalog_database_health_safety_environment_adt_raw.name
  role                   = module.glue_service_iam_role.name
  schedule               = "cron(0 1 * * ? *)"
  table_prefix           = "raw_"
  security_configuration = aws_glue_security_configuration.buckets_kms_security_config.id

  configuration = jsonencode(
    {
      Version = 1.0,
      CrawlerOutput = {
        Tables = { AddOrUpdateBehavior = "MergeNewColumns"}
        Partitions = { AddOrUpdateBehavior = "InheritFromTable"}
      },
      Grouping = { TableLevelConfiguration = 5 }
    }
  )
  
  schema_change_policy = {
    delete_behavior = "LOG"
    update_behavior = null
  }

  s3_target = [{
    path = "s3://${module.bucket_raw.s3_bucket_id}/health_safety_environment/adt/data_sampling"
  }]


  attributes = ["glue", "crawler", "health","safety","environment", "adt", "raw"]
  context    = module.label.context

}