# Mocked plans exercise opt-in and paused-runtime IAM without cloud access.
mock_provider "google" {}

variables {
  analysis_image           = "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/analysis@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  source_revision          = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  source_sha256            = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  index_analysis_id        = "powerbi-test"
  prepared_run_id          = "pipeline-powerbi-test"
  request_uri              = "gs://your-gcp-project-id-results/analysis-requests/powerbi-test.json"
  deploy_analysis          = false
  deploy_data_compute      = false
  deploy_api               = false
  deploy_messaging_queues  = false
  deploy_messaging_compute = false
  provision_email_secret   = false
  enable_email             = false
  powerbi_reader           = null
}

run "disabled_by_default" {
  command = plan

  assert {
    condition     = length(google_project_iam_custom_role.powerbi) == 0 && length(google_project_iam_member.powerbi) == 0 && length(google_bigquery_dataset_iam_member.powerbi) == 0 && output.powerbi_source == null
    error_message = "An unconfigured reader must not provision Power BI access."
  }
}

run "reader_with_compute_paused" {
  command = plan
  variables {
    powerbi_reader = format("%s:%s@%s", "user", "powerbi-test", "example.invalid")
  }

  assert {
    condition     = length(google_project_iam_custom_role.powerbi) == 1 && length(google_project_iam_member.powerbi) == 1 && length(google_bigquery_dataset_iam_member.powerbi) == 1
    error_message = "Reader IAM must exist independently of all runtime switches."
  }
  assert {
    condition     = google_bigquery_dataset_iam_member.powerbi[0].role == "roles/bigquery.dataViewer" && google_bigquery_dataset_iam_member.powerbi[0].dataset_id == "poc_gemma_analytics" && output.powerbi_source.mode == "Import" && output.powerbi_source.use_storage_api
    error_message = "Import access must be read-only on the analytics dataset."
  }
  assert {
    condition     = contains(google_project_iam_custom_role.powerbi[0].permissions, "bigquery.readsessions.create") && contains(google_project_iam_custom_role.powerbi[0].permissions, "bigquery.readsessions.getData") && contains(google_project_iam_custom_role.powerbi[0].permissions, "bigquery.readsessions.update")
    error_message = "The dedicated reader role must support BigQuery Storage API read sessions."
  }
  assert {
    condition     = length(google_service_account.data) == 0 && length(google_cloud_run_v2_job.analysis) == 0 && length(google_cloud_run_v2_job.index) == 0 && length(google_cloud_run_v2_service.api) == 0 && length(google_cloud_run_v2_job.publisher) == 0 && length(google_cloud_run_v2_service.notifier) == 0
    error_message = "Enabling Power BI must not restore pipeline runtime."
  }
}

run "reject_public_principal" {
  command = plan
  variables {
    powerbi_reader = "allUsers"
  }
  expect_failures = [var.powerbi_reader]
}
