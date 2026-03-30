data "google_folder" "parent" {
  folder = "folders/${var.folder_id}"
}

resource "google_project" "main" {
  name            = local.project_name
  project_id      = local.project_id
  folder_id       = data.google_folder.parent.folder_id
  billing_account = var.billing_account_id
}

resource "google_project_service" "apis" {
  for_each                   = toset(var.services)
  project                    = google_project.main.project_id
  service                    = each.key
  disable_dependent_services = false
  disable_on_destroy         = false
  depends_on                 = [google_project.main]
}

resource "google_service_account" "main" {
  account_id  = local.service_account_id
  description = "Service account for db-import-export BigQuery tests"
  project     = google_project.main.project_id
}

resource "google_folder_iam_binding" "project_creator" {
  folder = data.google_folder.parent.folder_id
  role   = "roles/resourcemanager.projectCreator"

  members = [
    "serviceAccount:${google_service_account.main.email}",
  ]
}

resource "google_project_iam_binding" "owner" {
  project = google_project.main.name
  role    = "roles/owner"

  members = [
    "serviceAccount:${google_service_account.main.email}",
  ]
}

resource "google_service_account_key" "main" {
  service_account_id = google_service_account.main.name
}

resource "local_file" "service_account_key" {
  content  = base64decode(google_service_account_key.main.private_key)
  filename = "${path.module}/bq_key.json"
}

resource "google_storage_bucket" "files" {
  name                        = "${var.name_prefix}-bq-files"
  project                     = google_project.main.name
  location                    = "US"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }
}
