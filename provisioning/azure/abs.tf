resource "azurerm_resource_group" "main" {
  name     = local.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "main" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "RAGRS"
  account_kind             = "StorageV2"

  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_container" "main" {
  name                  = "${var.name_prefix}-ie-container"
  storage_account_id    = azurerm_storage_account.main.id
  container_access_type = "private"
}
