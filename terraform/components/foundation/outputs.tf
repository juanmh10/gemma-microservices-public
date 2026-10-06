output "buckets" {
  value = { for key, bucket in google_storage_bucket.pipeline : key => bucket.name }
}

output "jobs" {
  value = var.deploy_jobs ? {
    preparer = try(google_cloud_run_v2_job.preparer[0].name, null)
    l4       = try(google_cloud_run_v2_job.worker["l4"].name, null)
    rtx6000  = try(google_cloud_run_v2_job.worker["rtx6000"].name, null)
  } : {}
}

output "service_accounts" {
  value = {
    preparer = google_service_account.preparer.email
    worker   = google_service_account.worker.email
  }
}
