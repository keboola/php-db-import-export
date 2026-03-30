output "ABS_ACCOUNT_NAME" {
  value = azurerm_storage_account.main.name
}

output "ABS_ACCOUNT_KEY" {
  value     = azurerm_storage_account.main.primary_access_key
  sensitive = true
}

output "ABS_CONTAINER_NAME" {
  value = azurerm_storage_container.main.name
}
