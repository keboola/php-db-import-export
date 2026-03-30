terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.74"
    }
  }
}

variable "name_prefix" {
  type    = string
  default = "kbc-pkg-ie"
}

variable "aws_account_id" {
  type    = string
  default = "532553470754" # Dev-Connection-Team (default for local dev)
}

variable "aws_profile" {
  type    = string
  default = "Keboola-Dev-Connection-Team-AWSAdministratorAccess"
}

variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

provider "aws" {
  allowed_account_ids = [var.aws_account_id]
  region              = var.aws_region
  profile             = var.aws_profile
}

locals {
  service_name = "${var.name_prefix}-db-import-export"
}
