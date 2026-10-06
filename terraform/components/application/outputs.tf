output "analysis_job" {
  value = try(google_cloud_run_v2_job.analysis[0].name, null)
}
output "resend_api_key_secret_id" {
  value = try(google_secret_manager_secret.resend_api_key[0].secret_id, null)
}
output "vertex_location" {
  value = var.vertex_location
}
output "analysis_image" {
  value = var.analysis_image
}
output "source_sha256" {
  value = var.source_sha256
}

output "api_uri" { value = try(google_cloud_run_v2_service.api[0].uri, null) }
output "index_job" { value = try(google_cloud_run_v2_job.index[0].name, null) }
output "analytics_dataset" { value = google_bigquery_dataset.analytics.dataset_id }
output "powerbi_source" {
  description = "Non-sensitive connector settings; null when Power BI access is disabled."
  value = local.enable_powerbi ? {
    billing_project = var.project_id
    data_project    = var.project_id
    dataset         = google_bigquery_dataset.analytics.dataset_id
    location        = var.region
    tables          = sort(keys(google_bigquery_table.analytics))
    mode            = "Import"
    use_storage_api = true
  } : null
}
output "publisher_job" { value = try(google_cloud_run_v2_job.publisher[0].name, null) }
output "notifier_uri" { value = try(google_cloud_run_v2_service.notifier[0].uri, null) }
output "event_topic" { value = try(google_pubsub_topic.messaging["analysis-completed"].id, null) }
output "dead_letter_subscription" { value = try(google_pubsub_subscription.dead_letter[0].id, null) }
