data "aws_iam_role" "emr_studio_user_role" {
  name = module.emr_studio.user_iam_role_name
}


# resource "aws_iam_role" "updated_role" {
#   name               = module.emr_studio.user_iam_role_name
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Principal = {
#           Service = ["elasticmapreduce.amazonaws.com"]
#         }
#         Action: ["sts:AssumeRole","sts:SetContext"],
#       }
#     ]
#   })
# }
