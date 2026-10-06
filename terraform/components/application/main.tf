locals {
  labels = {
    environment = "poc"
    service     = "gemma-pipeline"
    component   = "analysis"
    managed-by  = "terraform"
  }
  # All referenced buckets and the image repository remain owned by Foundation.
  readers = {
    prepared = {
      bucket = "${var.project_id}-prepared"
      paths  = ["runs/${var.prepared_run_id}/"]
    }
    truth = {
      bucket = "${var.project_id}-ground-truth"
      paths  = ["runs/${var.prepared_run_id}/"]
    }
    results = {
      bucket = "${var.project_id}-results"
      paths  = concat(["canary/reject-diag-cached-300-20260930/", "analysis/", "analysis-requests/"], var.worker_a_canary_id == "" ? [] : ["canary/${var.worker_a_canary_id}/"])
    }
  }
}

# run/storage/iam/logging/artifactregistry APIs are managed by the Foundation state.
# Confirm no other state owns Vertex enablement before applying this new root.
resource "google_project_service" "vertex" {
  count              = var.deploy_analysis ? 1 : 0
  project            = var.project_id
  service            = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

resource "google_service_account" "analysis" {
  count        = var.deploy_analysis ? 1 : 0
  project      = var.project_id
  account_id   = "poc-gemma-analysis"
  display_name = "POC Gemma Part 2 CPU analysis"
}

resource "google_project_iam_custom_role" "vertex_invoker" {
  count       = var.deploy_analysis ? 1 : 0
  project     = var.project_id
  role_id     = "pocGemmaPart2VertexInvoker"
  title       = "POC Gemma Part 2 model invocation"
  description = "Bounded Worker B GenerateContent and project quota use only; no provisioning, IAM mutation, or GPU Job invocation."
  permissions = ["aiplatform.endpoints.predict", "serviceusage.services.use"]
}

resource "google_project_iam_member" "vertex_invoker" {
  count   = var.deploy_analysis ? 1 : 0
  project = var.project_id
  role    = google_project_iam_custom_role.vertex_invoker[0].name
  member  = "serviceAccount:${google_service_account.analysis[0].email}"
}

resource "google_storage_bucket_iam_member" "read" {
  for_each = var.deploy_analysis ? local.readers : {}
  bucket   = each.value.bucket
  role     = "roles/storage.objectViewer"
  member   = "serviceAccount:${google_service_account.analysis[0].email}"
  condition {
    title       = "part2-${each.key}-objects"
    description = "Read only retained canary inputs or Part 2 artifacts; bucket listing is not granted by this condition."
    expression  = join(" || ", [for prefix in each.value.paths : "resource.name.startsWith('projects/_/buckets/${each.value.bucket}/objects/${prefix}')"])
  }
}

resource "google_storage_bucket_iam_member" "write" {
  count  = var.deploy_analysis ? 1 : 0
  bucket = "${var.project_id}-results"
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.analysis[0].email}"
  condition {
    title       = "part2-analysis-create"
    description = "Create-only analysis artifacts; no writes to canary inputs, labels, models or request objects."
    expression  = "resource.name.startsWith('projects/_/buckets/${var.project_id}-results/objects/analysis/')"
  }
}

resource "google_cloud_run_v2_job" "analysis" {
  count               = var.deploy_analysis ? 1 : 0
  project             = var.project_id
  name                = "poc-gemma-analysis"
  location            = var.region
  deletion_protection = false
  labels              = local.labels
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.analysis[0].email
      max_retries     = 0
      timeout         = "600s"
      containers {
        image = var.analysis_image
        args  = ["--remote", "--model", "vertex"]
        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
        env {
          name  = "ANALYSIS_REQUEST_URI"
          value = var.request_uri
        }
        env {
          name  = "GOOGLE_GENAI_USE_VERTEXAI"
          value = "true"
        }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "GOOGLE_CLOUD_LOCATION"
          value = var.vertex_location
        }
        env {
          name  = "IMAGE_DIGEST"
          value = var.analysis_image
        }
        env {
          name  = "SOURCE_REVISION"
          value = var.source_revision
        }
        env {
          name  = "SOURCE_SHA256"
          value = var.source_sha256
        }
      }
    }
  }
  depends_on = [google_project_service.vertex, google_project_iam_member.vertex_invoker, google_storage_bucket_iam_member.read, google_storage_bucket_iam_member.write]
}
