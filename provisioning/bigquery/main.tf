terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.49"
    }
  }
}

variable "name_prefix" {
  type    = string
  default = "kbc-pkg-ie"
}

variable "folder_id" {
  type        = string
  description = "GCP folder ID from https://console.cloud.google.com/cloud-resource-manager (e.g. KBC Team Dev folder)"
}

variable "billing_account_id" {
  type        = string
  description = "GCP billing account ID from https://console.cloud.google.com/billing/"
}

provider "google" {}

locals {
  project_name       = "${var.name_prefix}-bq-import-export"
  project_id         = "${var.name_prefix}-bq-import-export"
  service_account_id = "${var.name_prefix}-main-svc-acc"
}

variable "services" {
  type = list(string)
  default = [
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "bigquery.googleapis.com",
  ]
}
