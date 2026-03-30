terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

variable "name_prefix" {
  type    = string
  default = "kbc-pkg-ie"
}

variable "location" {
  type    = string
  default = "East US"
}

variable "subscription_id" {
  type    = string
  default = "eac4eb61-1abe-47e2-a0a1-f0a7e066f385" # Keboola DEV Connection Team
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

locals {
  # Azure storage account names: lowercase alphanumeric, max 24 chars
  storage_account_name = replace("${var.name_prefix}ielib", "-", "")
  resource_group_name  = "${var.name_prefix}-db-import-export-rg"
}
