# Persistent analytics outlives the independent compute teardown switch.
locals {
  deploy_api      = var.deploy_data_compute || var.deploy_api
  data_components = var.deploy_data_compute ? toset(["index", "api"]) : (var.deploy_api ? toset(["api"]) : toset([]))
}
resource "google_bigquery_dataset" "analytics" {
  project                    = var.project_id
  dataset_id                 = "poc_gemma_analytics"
  location                   = var.region
  description                = "Versioned projections of completed GCS analyses; no dialogue/evidence bodies."
  delete_contents_on_destroy = false
  labels                     = merge(local.labels, { component = "analytics" })
  lifecycle { prevent_destroy = true }
}
resource "google_bigquery_table" "analytics" {
  for_each            = toset(["analysis_runs", "analysis_reports"])
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = each.key
  schema              = file("${path.module}/../../../schemas/analytics/${each.key}-v1.json")
  deletion_protection = true
  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }
  clustering = ["analysis_id"]
  labels     = merge(local.labels, { component = "analytics" })
  lifecycle { prevent_destroy = true }
}
resource "google_service_account" "data" {
  for_each     = local.data_components
  project      = var.project_id
  account_id   = "poc-gemma-${each.key}"
  display_name = "POC Gemma ${each.key}; no model or GPU permissions"
}
resource "google_project_iam_custom_role" "data_query" {
  count       = local.deploy_api ? 1 : 0
  project     = var.project_id
  role_id     = "pocGemmaDataQuery"
  title       = "POC Gemma data query and quota use"
  description = "BigQuery job creation and fixed-project quota use; no Dataform, IAM, model or Job invocation. "
  permissions = ["bigquery.jobs.create", "serviceusage.services.use"]
}
resource "google_project_iam_member" "data_jobs" {
  for_each = google_service_account.data
  project  = var.project_id
  role     = google_project_iam_custom_role.data_query[0].name
  member   = "serviceAccount:${each.value.email}"
}
resource "google_bigquery_dataset_iam_member" "data" {
  for_each   = google_service_account.data
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = each.key == "index" ? "roles/bigquery.dataEditor" : "roles/bigquery.dataViewer"
  member     = "serviceAccount:${each.value.email}"
}
resource "google_storage_bucket_iam_member" "data_read" {
  for_each = google_service_account.data
  bucket   = "${var.project_id}-results"
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${each.value.email}"
  condition {
    title      = "part3-${each.key}-read"
    expression = "resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/analysis/') || resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/index/')"
  }
}
resource "google_storage_bucket_iam_member" "index_write" {
  count  = var.deploy_data_compute ? 1 : 0
  bucket = "${var.project_id}-results"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.data["index"].email}"
  condition {
    title      = "part3-index-create"
    expression = "resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/index/')"
  }
}
resource "google_cloud_run_v2_job" "index" {
  count               = var.deploy_data_compute ? 1 : 0
  project             = var.project_id
  location            = var.region
  name                = "poc-gemma-index"
  deletion_protection = false
  labels              = merge(local.labels, { component = "index" })
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.data["index"].email
      max_retries     = 0
      timeout         = "300s"
      containers {
        image = var.data_image
        args  = ["--remote", "--mode", "index"]
        resources { limits = { cpu = "1", memory = "512Mi" } }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "INDEX_ANALYSIS_ID"
          value = var.index_analysis_id
        }
      }
    }
  }
  depends_on = [google_bigquery_table.analytics, google_bigquery_dataset_iam_member.data, google_project_iam_member.data_jobs, google_storage_bucket_iam_member.data_read, google_storage_bucket_iam_member.index_write]
}
resource "google_cloud_run_v2_service" "api" {
  count                = local.deploy_api ? 1 : 0
  project              = var.project_id
  location             = var.region
  name                 = "poc-gemma-api"
  ingress              = "INGRESS_TRAFFIC_ALL"
  invoker_iam_disabled = false
  deletion_protection  = false
  labels               = merge(local.labels, { component = "api" })
  scaling {
    min_instance_count = 0
    max_instance_count = 1
  }
  template {
    service_account                  = google_service_account.data["api"].email
    timeout                          = "20s"
    max_instance_request_concurrency = 1
    containers {
      image = var.data_image
      args  = ["--remote", "--mode", "api"]
      ports { container_port = 8080 }
      resources {
        limits            = { cpu = "1", memory = "512Mi" }
        cpu_idle          = true
        startup_cpu_boost = false
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = var.project_id
      }
    }
  }
  depends_on = [google_bigquery_dataset_iam_member.data, google_project_iam_member.data_jobs, google_storage_bucket_iam_member.data_read, google_storage_bucket_iam_member.api_notifications]
}
resource "google_cloud_run_v2_service_iam_member" "operator" {
  count    = local.deploy_api ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api[0].name
  role     = "roles/run.invoker"
  member   = var.api_invoker
}
