output "GCS_CREDENTIALS" {
  value     = base64decode(google_service_account_key.gcs.private_key)
  sensitive = true
}

output "GCS_BUCKET_NAME" {
  value = google_storage_bucket.snowflake_gcs.name
}
