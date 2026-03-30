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

variable "project_id" {
  type        = string
  description = "Existing GCP project ID to create resources in (e.g. the BQ project kbc-pkg-ie-bq-import-export)"
}

provider "google" {
  project = var.project_id
}

locals {
  service_account_id = "${var.name_prefix}-gcs-svc-acc"
}
