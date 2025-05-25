provider "aws" {
  access_key = "AKIAWIJIUKIMG6O23RGP"
  secret_key = "dVZyLpa3+vA829EHr59Lmdhhpj4DlprhWpBXwtZH"
  region     = "us-east-1"  # Change to your desired region
}

resource "aws_s3_bucket" "example" {
  bucket = "gokul123-tf-test-bucket"

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}

