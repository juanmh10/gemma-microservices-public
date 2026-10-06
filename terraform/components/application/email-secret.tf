# Bootstrap the container only. Operators add secret versions outside Terraform
# so API key material never enters configuration, plans or state.
resource "google_project_service" "secret_manager" {
  count              = var.provision_email_secret ? 1 : 0
  project            = var.project_id
  service            = "secretmanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_secret_manager_secret" "resend_api_key" {
  count               = var.provision_email_secret ? 1 : 0
  project             = var.project_id
  secret_id           = "poc-gemma-resend-api-key"
  labels              = merge(local.labels, { component = "messaging" })
  deletion_protection = true
  deletion_policy     = "PREVENT"
  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
  lifecycle {
    prevent_destroy = true
  }
  depends_on = [google_project_service.secret_manager]
}

resource "google_secret_manager_secret_iam_member" "notifier" {
  count     = var.enable_email ? 1 : 0
  project   = var.project_id
  secret_id = google_secret_manager_secret.resend_api_key[0].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.messaging["notifier"].email}"
}
