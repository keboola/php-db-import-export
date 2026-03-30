output "BQ_KEY_FILE" {
  value     = base64decode(google_service_account_key.main.private_key)
  sensitive = true
}

output "BQ_BUCKET_NAME" {
  value = google_storage_bucket.files.name
}

output "BQ_PROJECT_ID" {
  value = google_project.main.project_id
}
