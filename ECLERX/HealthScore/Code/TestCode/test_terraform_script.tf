provider "aws" {
    access_key = "AKIAWIJIUKIMG6O23RGP"
    secret_key = "dVZyLpa3+vA829EHr59Lmdhhpj4DlprhWpBXwtZH"
    region     = "us-east-1"  # Change to your desired region
  }
  
  resource "aws_s3_bucket" "my_bucket" {
    bucket = "gokul-unique-bucket-name-123456"  # S3 bucket names must be globally unique
    acl    = "private"
  
    versioning {
      enabled = true
    }
  
    tags = {
      Name        = "MyBucket"
      Environment = "Dev"
    }
  }
  