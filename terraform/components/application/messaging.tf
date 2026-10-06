data "google_project" "current" { project_id = var.project_id }

locals {
  pubsub_agent = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  messaging_readers = {
    publisher = ["analysis/", "publication/"]
    notifier  = ["analysis/", "notifications/"]
  }
}

# Pub/Sub API is already enabled outside this state; do not take second ownership.
# Queue storage survives compute shutdown. Removing the push subscription loses
# its backlog and requires a separately reviewed shutdown plan.
resource "google_pubsub_topic" "messaging" {
  for_each = var.deploy_messaging_queues ? toset(["analysis-completed", "notification-dead-letter"]) : toset([])
  project  = var.project_id
  name     = "poc-gemma-${each.key}"
  labels   = merge(local.labels, { component = "messaging" })
  message_storage_policy {
    allowed_persistence_regions = [var.region]
    enforce_in_transit          = true
  }
}
resource "google_pubsub_subscription" "dead_letter" {
  count                      = var.deploy_messaging_queues ? 1 : 0
  project                    = var.project_id
  name                       = "poc-gemma-notification-dead-letter"
  topic                      = google_pubsub_topic.messaging["notification-dead-letter"].id
  message_retention_duration = "604800s"
  ack_deadline_seconds       = 30
  expiration_policy { ttl = "" }
  labels = merge(local.labels, { component = "dead-letter" })
}
resource "google_service_account" "messaging" {
  for_each     = var.deploy_messaging_compute ? toset(["publisher", "notifier", "push"]) : toset([])
  project      = var.project_id
  account_id   = "poc-gemma-${each.key}"
  display_name = "POC Gemma ${each.key}; simulated messaging only"
}
resource "google_project_iam_custom_role" "messaging_quota" {
  count       = var.deploy_messaging_compute ? 1 : 0
  project     = var.project_id
  role_id     = "pocGemmaMessagingQuota"
  title       = "POC Gemma messaging quota use"
  permissions = ["serviceusage.services.use"]
}
resource "google_project_iam_member" "messaging_quota" {
  for_each = var.deploy_messaging_compute ? local.messaging_readers : {}
  project  = var.project_id
  role     = google_project_iam_custom_role.messaging_quota[0].name
  member   = "serviceAccount:${google_service_account.messaging[each.key].email}"
}
resource "google_storage_bucket_iam_member" "messaging_read" {
  for_each = var.deploy_messaging_compute ? local.messaging_readers : {}
  bucket   = "${var.project_id}-results"
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${google_service_account.messaging[each.key].email}"
  condition {
    title      = "part4-${each.key}-read"
    expression = join(" || ", [for prefix in each.value : "resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/${prefix}')"])
  }
}
resource "google_storage_bucket_iam_member" "messaging_write" {
  for_each = var.deploy_messaging_compute ? { publisher = "publication/", notifier = "notifications/" } : {}
  bucket   = "${var.project_id}-results"
  role     = "roles/storage.objectCreator"
  member   = "serviceAccount:${google_service_account.messaging[each.key].email}"
  condition {
    title      = "part4-${each.key}-create"
    expression = "resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/${each.value}')"
  }
}
resource "google_storage_bucket_iam_member" "api_notifications" {
  count  = local.deploy_api ? 1 : 0
  bucket = "${var.project_id}-results"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.data["api"].email}"
  condition {
    title      = "part4-api-status-read"
    expression = "resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/publication/') || resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/notifications/')"
  }
}
resource "google_pubsub_topic_iam_member" "publisher" {
  count   = var.deploy_messaging_compute ? 1 : 0
  project = var.project_id
  topic   = google_pubsub_topic.messaging["analysis-completed"].name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.messaging["publisher"].email}"
}
resource "google_pubsub_topic_iam_member" "dead_letter_agent" {
  count   = var.deploy_messaging_compute ? 1 : 0
  project = var.project_id
  topic   = google_pubsub_topic.messaging["notification-dead-letter"].name
  role    = "roles/pubsub.publisher"
  member  = local.pubsub_agent
}
resource "google_service_account_iam_member" "push_token" {
  count              = var.deploy_messaging_compute ? 1 : 0
  service_account_id = google_service_account.messaging["push"].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = local.pubsub_agent
}
resource "google_cloud_run_v2_job" "publisher" {
  count               = var.deploy_messaging_compute ? 1 : 0
  project             = var.project_id
  location            = var.region
  name                = "poc-gemma-publisher"
  deletion_protection = false
  labels              = merge(local.labels, { component = "publisher" })
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.messaging["publisher"].email
      max_retries     = 0
      timeout         = "120s"
      containers {
        image = var.messaging_image
        args  = ["--remote", "--mode", "publish"]
        resources { limits = { cpu = "1", memory = "512Mi" } }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "PUBLISH_ANALYSIS_ID"
          value = var.index_analysis_id
        }
      }
    }
  }
  depends_on = [google_storage_bucket_iam_member.messaging_read, google_storage_bucket_iam_member.messaging_write, google_project_iam_member.messaging_quota, google_pubsub_topic_iam_member.publisher, google_pubsub_subscription.notify, google_pubsub_subscription_iam_member.dead_letter_agent]
}
resource "google_cloud_run_v2_service" "notifier" {
  count                = var.deploy_messaging_compute ? 1 : 0
  project              = var.project_id
  location             = var.region
  name                 = "poc-gemma-notifier"
  ingress              = "INGRESS_TRAFFIC_ALL"
  invoker_iam_disabled = false
  deletion_protection  = false
  labels               = merge(local.labels, { component = "notifier" })
  scaling {
    min_instance_count = 0
    max_instance_count = 1
  }
  template {
    service_account                  = google_service_account.messaging["notifier"].email
    timeout                          = "30s"
    max_instance_request_concurrency = 1
    containers {
      image = var.messaging_image
      args  = ["--remote", "--mode", "notify"]
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
      env {
        name  = "EMAIL_ENABLED"
        value = tostring(var.enable_email)
      }
      dynamic "env" {
        for_each = var.enable_email ? toset(["EMAIL_FROM", "EMAIL_TO", "EMAIL_SENDER_DOMAIN"]) : toset([])
        content {
          name  = env.value
          value = { EMAIL_FROM = var.email_from, EMAIL_TO = var.email_to, EMAIL_SENDER_DOMAIN = var.email_sender_domain }[env.value]
        }
      }
      dynamic "env" {
        for_each = var.enable_email ? [1] : []
        content {
          name = "RESEND_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.resend_api_key[0].secret_id
              version = var.resend_secret_version
            }
          }
        }
      }
    }
  }
  depends_on = [google_storage_bucket_iam_member.messaging_read, google_storage_bucket_iam_member.messaging_write, google_project_iam_member.messaging_quota, google_secret_manager_secret_iam_member.notifier]
}
resource "google_cloud_run_v2_service_iam_member" "push" {
  count    = var.deploy_messaging_compute ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.notifier[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.messaging["push"].email}"
}
resource "google_pubsub_subscription" "notify" {
  count                      = var.deploy_messaging_compute ? 1 : 0
  project                    = var.project_id
  name                       = "poc-gemma-notify"
  topic                      = google_pubsub_topic.messaging["analysis-completed"].id
  ack_deadline_seconds       = 30
  message_retention_duration = "86400s"
  expiration_policy { ttl = "" }
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.messaging["notification-dead-letter"].id
    max_delivery_attempts = 5
  }
  push_config {
    push_endpoint = "${google_cloud_run_v2_service.notifier[0].uri}/events"
    oidc_token {
      service_account_email = google_service_account.messaging["push"].email
      audience              = google_cloud_run_v2_service.notifier[0].uri
    }
  }
  labels     = merge(local.labels, { component = "notify-push" })
  depends_on = [google_pubsub_subscription.dead_letter, google_cloud_run_v2_service_iam_member.push, google_service_account_iam_member.push_token, google_pubsub_topic_iam_member.dead_letter_agent]
}
resource "google_pubsub_subscription_iam_member" "dead_letter_agent" {
  count        = var.deploy_messaging_compute ? 1 : 0
  project      = var.project_id
  subscription = google_pubsub_subscription.notify[0].name
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}
