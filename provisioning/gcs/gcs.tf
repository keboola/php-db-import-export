resource "google_storage_bucket" "snowflake_gcs" {
  name                        = "${var.name_prefix}-snowflake-gcs-files"
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }
}

resource "google_service_account" "gcs" {
  account_id  = local.service_account_id
  description = "Service account for db-import-export Snowflake-GCS tests"
}

resource "google_storage_bucket_iam_member" "gcs_admin" {
  bucket = google_storage_bucket.snowflake_gcs.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.gcs.email}"
}

resource "google_service_account_key" "gcs" {
  service_account_id = google_service_account.gcs.name
}
