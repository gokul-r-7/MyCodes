locals {
  databases = { for db in local.data_gov_config.databases : db.name => db }
  db_ad_grp_mapping = {
    for combo in flatten([
      for db in local.databases : [
        for ad_grp in db.groups : {
          "db" : db.name,
          "ad_grp" : ad_grp,
          "combo_key" : "${db.name}-${ad_grp}"
        }
      ]
    ]) : "${combo.db}-${combo.ad_grp}" => combo
  }
}


#### NOTE::: the below out until https://github.com/hashicorp/terraform-provider-aws/issues/21539 resolved
### How to grant permissions is documented in ../data-governance/lakeformation administration.md

# resource "aws_lakeformation_permissions" "db_grant" {
#   for_each    = { for i in local.db_ad_grp_mapping : i.combo_key => i }
#   principal   = "arn:aws:identitystore:::group/${each.value.ad_grp}"
#   permissions = ["DESCRIBE"]

#   database {
#     name       = each.value.db
#     catalog_id = data.aws_caller_identity.current.account_id
#   }
# }


# resource "aws_lakeformation_permissions" "table_grant" {
#   for_each    = { for i in local.db_ad_grp_mapping : i.combo_key => i }
#   principal   = "arn:aws:identitystore:::group/${each.value.ad_grp}"
#   permissions = ["DESCRIBE", "SELECT"]

#   table {
#     wildcard      = true
#     database_name = each.value.db
#     catalog_id    = data.aws_caller_identity.current.account_id
#   }
# }
