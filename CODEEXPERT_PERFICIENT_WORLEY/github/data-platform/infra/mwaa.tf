module "mwaa" {
  source  = "cloudposse/mwaa/aws"
  version = "0.6.3"
  #count = var.mwaa_deploy # to prevent creating this resource in dev

  region     = var.aws_region
  vpc_id     = var.vpc_id
  subnet_ids = data.aws_subnets.private_dataware.ids

  airflow_version                  = "2.10.3"
  dag_s3_path                      = "dags"
  requirements_s3_path             = "requirements.txt"
  requirements_s3_object_version   = aws_s3_object.requirements_txt.version_id
  startup_script_s3_path           = "startup.sh"
  startup_script_s3_object_version = aws_s3_object.startup_script.version_id
  plugins_s3_path                  = "plugins.zip"
  plugins_s3_object_version        = aws_s3_object.plugins_files.version_id
  environment_class                = var.airflow_size
  min_workers                      = 1
  max_workers                      = 10
  webserver_access_mode            = "PRIVATE_ONLY"
  dag_processing_logs_enabled      = true
  dag_processing_logs_level        = "INFO"
  scheduler_logs_enabled           = true
  scheduler_logs_level             = "INFO"
  task_logs_enabled                = true
  task_logs_level                  = "INFO"
  webserver_logs_enabled           = true
  webserver_logs_level             = "INFO"
  worker_logs_enabled              = true
  worker_logs_level                = "INFO"
  airflow_configuration_options = {
    "core.default_task_retries"       = 5
    "core.dag_file_processor_timeout" = 100
    "core.dagbag_import_timeout"      = 90
    "openlineage.transport"           = var.airflow_openlineage_http_configuration
    "celery.worker_autoscale"         = "10,10"
  }

  associated_security_group_ids = [module.mwaa_sg.security_group_id]

  context    = module.label.context
  attributes = ["mwaa", "airflow"]
}

module "mwaa_sg" {

  source      = "terraform-aws-modules/security-group/aws"
  version     = "5.1.2"
  name        = "${module.label.id}-mwaa-airflow-sg"
  description = "Security group for MWAA"
  vpc_id      = var.vpc_id

  # allow ingress amazon mwaa traffic
  ingress_with_self = [
    {
      rule = "all-all"
    }
  ]

  ingress_with_cidr_blocks = [
    # allow ec2 instance for RDP into private airflow
    {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      description = "Allow IP for EC2 to Access Airflow"
      cidr_blocks = "${module.instance.private_ip}/32"
    },

    # Allow HTTPS traffic from Worley internal IPs
    {
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      description = "Allow HTTPS access from private IPs for Airflow UI"
      cidr_blocks = "10.0.0.0/8"
    }
  ]

  # allow egress for ipv4
  egress_with_cidr_blocks = [
    {
      from_port   = 0
      to_port     = 0
      protocol    = "-1"
      description = "Allow All Egress"
      cidr_blocks = "10.0.0.0/8"
    },
  ]

}

data "aws_iam_policy_document" "mwaa_kms_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt"]
    resources = ["arn:aws:kms:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:key/*"]
  }
  statement {
    effect = "Allow"
    actions = [
      "glue:CreateJob",
      "glue:GetJob",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:StopJobRun"
    ]
    resources = ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/*"]
  }
  statement {
    effect = "Allow"
    actions = [
      "glue:GetCrawler",
      "glue:GetCrawlers",
      "glue:StartCrawler",
      "glue:StopCrawler",
      "glue:GetCrawlerMetrics"
    ]
    resources = ["arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:crawler/*"]
  }
}

data "aws_iam_policy_document" "mwaa_lambda_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = ["arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:*"]
  }
}
resource "aws_iam_policy" "mwaa_lambda_policy" {
  name   = "lambda-policy"
  policy = data.aws_iam_policy_document.mwaa_lambda_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "mwaa_lambda_policy_attachment" {
  role       = split("/", module.mwaa.execution_role_arn)[1]
  policy_arn = aws_iam_policy.mwaa_lambda_policy.arn
}

resource "aws_iam_policy" "mwaa_kms_policy" {

  name   = "kms-policy"
  policy = data.aws_iam_policy_document.mwaa_kms_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "mwaa_kms_policy_attachment" {
  role       = split("/", module.mwaa.execution_role_arn)[1]
  policy_arn = aws_iam_policy.mwaa_kms_policy.arn
}

resource "aws_iam_role_policy_attachment" "mwaa_ddb_policy_attachment" {
  role       = split("/", module.mwaa.execution_role_arn)[1]
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"
}

resource "aws_iam_role_policy" "mwaa_logs_policy" {
  name = "mwaa-logs-policy"
  role = split("/", module.mwaa.execution_role_arn)[1]

  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
       "Action": [
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
                "logs:GetLogEvents",
                "logs:GetLogRecord",
                "logs:GetLogGroupFields",
                "logs:GetQueryResults",
                "logs:FilterLogEvents"
            ],
        "Resource": ["arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-*",
                    "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue*",
                    "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda*",
                    "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:worley-datalake-sydney-${var.environment}-datasync-people-cw-logs-group:*"
                  ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "logs:DescribeLogGroups"
        ],
        "Resource": ["*"]
      }
    ]
  }
  EOF
}

resource "aws_iam_role_policy" "mwaa_appflow_policy" {
  name = "mwaa-appflow-policy"
  role = split("/", module.mwaa.execution_role_arn)[1]
  #tfsec:ignore:aws-iam-no-policy-wildcards
  #checkov:skip=CKV_AWS_290:IAM policies allow write access without constraints
  #checkov:skip=CKV_AWS_355:IAM policy document allows all resources with restricted actions
  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "appflow:Cancel*",
          "appflow:Describe*",
          "appflow:List*",
          "appflow:RunFlow",
          "appflow:StartFlow",
          "appflow:StopFlow"
        ],
        "Resource": "*"
      }
    ]
  }
  EOF
}

resource "aws_iam_role_policy" "mwaa_datasync_policy" {
  name = "mwaa-datasync-policy"
  role = split("/", module.mwaa.execution_role_arn)[1]

  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "datasync:CancelTaskExecution",
          "datasync:DeleteTask",
          "datasync:DescribeAgent",
          "datasync:DescribeLocationObjectStorage",
          "datasync:DescribeLocationS3",
          "datasync:DescribeTask",
          "datasync:DescribeTaskExecution",
          "datasync:ListTasks",
          "datasync:StartTaskExecution",
          "datasync:UpdateTask",
          "datasync:UpdateTaskExecution"
          
        ],
        "Resource": ["arn:aws:datasync:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:task/*"]
      },
      {
        "Effect": "Allow",
        "Action": [
          "ec2:DescribeNetworkInterfaces"
        ],
        "Resource": ["*"]
      }
    ]
  }
  EOF
}


resource "aws_iam_role_policy" "mwaa_s3_policy" {
  name = "mwaa-s3-policy"
  role = split("/", module.mwaa.execution_role_arn)[1]

  policy = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:ListObject*",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        "Resource": [
            "${module.bucket_raw.s3_bucket_arn}",
            "${module.bucket_raw.s3_bucket_arn}/*"
        ]
      }
    ]
  }
  EOF
}


resource "aws_iam_role_policy" "mwaa_s3_raw_kms" {
  name = "mwaa-s3-raw"
  role = split("/", module.mwaa.execution_role_arn)[1]
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "kms:CreateGrant",
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:Encrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*"
        ],
        "Resource" : [
          "${module.kms_key.key_arn}"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "dbt_glue_client_policy"{
  name = "mwaa-glue-dbt-policy"
  role = split("/", module.mwaa.execution_role_arn)[1]
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid": "CreateSessionPermission",
        "Action": [
              "glue:GetSession",
              "glue:DeleteSession",
              "glue:CreateSession",
              "glue:StopSession",
              "glue:ListSessions",
              "glue:ListStatements",
              "glue:GetStatement",
              "glue:RunStatement",
              "glue:CancelStatement"
        ],
        "Effect": "Allow",
        "Resource": "*",
          
      },
      {
        "Sid": "ReadOnlyDatabases",
        "Effect": "Allow",
        "Action": [
              "lakeformation:ListResources",
              "lakeformation:ListPermissions",
              "glue:SearchTables",
              "glue:GetTables",
              "glue:GetTableVersions",
              "glue:GetTableVersion",
              "glue:GetTable",
              "glue:GetPartitions",
              "glue:GetPartition",
              "glue:GetDatabases",
              "glue:GetDatabase"
            ],
        "Resource": [
                "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/*/*",
                "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/*",
                "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog"
            ],
      },
      {
         "Sid": "PassRole",
         "Effect": "Allow",
         "Action":[
          "iam:PassRole"
         ],
         "Resource": [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${module.glue_service_iam_role.name}"
         ]
      }
    ]
  })
}

resource "aws_s3_object" "requirements_txt" {
  bucket = split(":::", module.mwaa.s3_bucket_arn)[1]
  key    = "requirements.txt"
  source = "${path.root}/code/airflow/requirements.txt"
  etag   = filemd5("${path.root}/code/airflow/requirements.txt")
}

resource "aws_s3_object" "startup_script" {
  bucket = split(":::", module.mwaa.s3_bucket_arn)[1]
  key    = "startup.sh"
  source = "${path.root}/code/airflow/startup.sh"
  etag   = filemd5("${path.root}/code/airflow/startup.sh")
}

resource "aws_s3_object" "plugins_files" {
  bucket = split(":::", module.mwaa.s3_bucket_arn)[1]
  key    = "plugins.zip"
  source = "${path.root}/code/airflow/plugins.zip"
  etag   = filemd5("${path.root}/code/airflow/plugins.zip")
}

# resource "aws_s3_object" "constraints_txt" {
#   bucket = split(":::", module.mwaa.s3_bucket_arn)[1]
#   key    = "constraints-3.11.txt"
#   source = "${path.root}/code/airflow/constraints-3.11.txt"
#   etag   = filemd5("${path.root}/code/airflow/constraints-3.11.txt")
# }


