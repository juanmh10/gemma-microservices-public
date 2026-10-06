# Import/refresh access persists independently of all compute teardown switches.
# No service-account key or project-wide data access is provisioned here.
locals {
  # Only presence is public; keep the principal itself sensitive.
  enable_powerbi = nonsensitive(var.powerbi_reader != null)
}

resource "google_project_iam_custom_role" "powerbi" {
  count       = local.enable_powerbi ? 1 : 0
  project     = var.project_id
  role_id     = "pocGemmaPowerBIReader"
  title       = "POC Gemma Power BI queries"
  description = "Connector project discovery, query jobs and quota use; data access is granted separately on analytics."
  permissions = [
    "bigquery.jobs.create",
    "bigquery.readsessions.create",
    "bigquery.readsessions.getData",
    "bigquery.readsessions.update",
    "resourcemanager.projects.get",
    "serviceusage.services.use",
  ]
}

resource "google_project_iam_member" "powerbi" {
  count   = local.enable_powerbi ? 1 : 0
  project = var.project_id
  role    = google_project_iam_custom_role.powerbi[0].name
  member  = var.powerbi_reader
}

resource "google_bigquery_dataset_iam_member" "powerbi" {
  count      = local.enable_powerbi ? 1 : 0
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = var.powerbi_reader
}
