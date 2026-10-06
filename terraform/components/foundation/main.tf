locals {
  worker_rtx_git_sha = var.worker_rtx_git_sha == "" ? var.git_sha : var.worker_rtx_git_sha
  labels = {
    environment = "poc"
    service     = "gemma-pipeline"
    managed-by  = "terraform"
  }
  bucket_names = {
    raw          = "${var.project_id}-raw"
    prepared     = "${var.project_id}-prepared"
    models       = "${var.project_id}-models"
    results      = "${var.project_id}-results"
    ground_truth = "${var.project_id}-ground-truth"
  }
  jobs = {
    l4 = {
      name           = "poc-gemma-worker-a-l4"
      image          = var.worker_l4_image
      accelerator    = "nvidia-l4"
      cpu            = "4"
      memory         = "16Gi"
      v2_runner      = "0"
      model_manifest = "gs://${local.bucket_names.models}/diffusiongemma/26b-a4b-it/l4-awq/model-manifest.json"
    }
    rtx6000 = {
      name           = "poc-gemma-worker-a-rtx6000"
      image          = var.worker_rtx_image
      accelerator    = "nvidia-rtx-pro-6000"
      cpu            = "20"
      memory         = "80Gi"
      v2_runner      = "1"
      model_manifest = "gs://${local.bucket_names.models}/diffusiongemma/26b-a4b-it/rtx6000/model-manifest.json"
    }
  }
}

resource "google_project_service" "required" {
  for_each = toset(concat([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "cloudquotas.googleapis.com",
  ], var.deploy_jobs || var.retain_worker_network ? ["compute.googleapis.com"] : []))
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_cloud_quotas_quota_preference" "l4_pilot" {
  provider      = google.quota
  parent        = "projects/${var.project_id}"
  name          = "poc-gemma-l4-us-central1"
  service       = "run.googleapis.com"
  quota_id      = "NvidiaL4GpuAllocNoZonalRedundancyPerProjectRegion"
  dimensions    = { region = var.region }
  contact_email = var.l4_quota_contact_email
  justification = "12-conversation POC with three parallel Cloud Run L4 GPU tasks in us-central1."
  quota_config {
    preferred_value = 3
  }
  depends_on = [google_project_service.required]
}

resource "google_storage_bucket" "pipeline" {
  for_each                    = local.bucket_names
  name                        = each.value
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  deletion_policy             = each.key == "models" ? "PREVENT" : "DELETE"
  labels                      = local.labels
  dynamic "autoclass" {
    for_each = each.key == "models" ? [true] : []
    content {
      enabled                = true
      terminal_storage_class = "NEARLINE"
    }
  }
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  depends_on = [google_project_service.required]
}

resource "google_artifact_registry_repository" "pipeline" {
  project       = var.project_id
  location      = var.region
  repository_id = "pipeline"
  format        = "DOCKER"
  labels        = local.labels
  depends_on    = [google_project_service.required]
}

resource "google_compute_network" "worker" {
  count                   = var.deploy_jobs || var.retain_worker_network ? 1 : 0
  project                 = var.project_id
  name                    = "poc-gemma-worker"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}

resource "google_compute_subnetwork" "worker" {
  count                    = var.deploy_jobs || var.retain_worker_network ? 1 : 0
  project                  = var.project_id
  name                     = "poc-gemma-worker-us-central1"
  region                   = var.region
  network                  = google_compute_network.worker[0].id
  ip_cidr_range            = "10.90.0.0/26"
  private_ip_google_access = true
}

resource "google_service_account" "preparer" {
  project      = var.project_id
  account_id   = "poc-gemma-preparer"
  display_name = "POC Gemma dataset preparer"
  depends_on   = [google_project_service.required]
}

resource "google_service_account" "worker" {
  project      = var.project_id
  account_id   = "poc-gemma-worker-a"
  display_name = "POC Gemma Worker A"
  depends_on   = [google_project_service.required]
}

resource "google_storage_bucket_iam_member" "preparer_raw_reader" {
  bucket = google_storage_bucket.pipeline["raw"].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.preparer.email}"
}

resource "google_storage_bucket_iam_member" "preparer_prepared_writer" {
  bucket = google_storage_bucket.pipeline["prepared"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.preparer.email}"
}

resource "google_storage_bucket_iam_member" "preparer_truth_writer" {
  bucket = google_storage_bucket.pipeline["ground_truth"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.preparer.email}"
}

resource "google_storage_bucket_iam_member" "worker_prepared_reader" {
  bucket = google_storage_bucket.pipeline["prepared"].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_storage_bucket_iam_member" "worker_model_reader" {
  bucket = google_storage_bucket.pipeline["models"].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_storage_bucket_iam_member" "worker_model_bucket_viewer" {
  # Run:ai's GCS adapter reads bucket metadata before listing model objects.
  bucket = google_storage_bucket.pipeline["models"].name
  role   = "roles/storage.bucketViewer"
  member = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_storage_bucket_iam_member" "worker_result_writer" {
  bucket = google_storage_bucket.pipeline["results"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_cloud_run_v2_job" "preparer" {
  lifecycle {
    precondition {
      condition     = var.prompt_version != "worker-a-v3" || startswith(var.domain_config_uri, "gs://${local.bucket_names.raw}/")
      error_message = "Prompt v3 requires a domain object in the existing raw bucket."
    }
    precondition {
      condition     = !var.canonical_source || (var.prompt_version == "worker-a-v3" && var.source_uri != "" && var.metadata_uri != "")
      error_message = "Canonical input requires prompt v3 and explicit source/reference objects."
    }
    precondition {
      condition     = (var.source_uri == "" || startswith(var.source_uri, "gs://${local.bucket_names.raw}/")) && (var.metadata_uri == "" || startswith(var.metadata_uri, "gs://${local.bucket_names.raw}/"))
      error_message = "Dataset and reference sources must stay in the existing raw bucket."
    }
  }

  count               = var.deploy_jobs && var.deploy_preparer ? 1 : 0
  project             = var.project_id
  name                = "poc-gemma-dataset-preparer"
  location            = var.region
  deletion_protection = false
  labels              = local.labels
  template {
    task_count  = 1
    parallelism = 1
    template {
      service_account = google_service_account.preparer.email
      max_retries     = 0
      timeout         = "1200s"
      containers {
        image = var.preparer_image
        args  = concat(["--limit", tostring(var.pilot_records), "--prompt-version", var.prompt_version], var.preparer_shards == 3 ? [] : ["--shards", tostring(var.preparer_shards)], var.prompt_version == "worker-a-v3" ? concat(["--domain", var.domain_config_uri, "--dataset-version", var.dataset_version], var.canonical_source ? ["--canonical"] : []) : [])
        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
        env {
          name  = "SOURCE_URI"
          value = var.source_uri != "" ? var.source_uri : "gs://${local.bucket_names.raw}/gym-sales-v1/gym_v3_dataset_clean.jsonl"
        }
        env {
          name  = "METADATA_URI"
          value = var.metadata_uri != "" ? var.metadata_uri : "gs://${local.bucket_names.raw}/gym-sales-v1/gym_v3_metadata.jsonl"
        }
        env {
          name  = "PREPARED_PREFIX"
          value = "gs://${local.bucket_names.prepared}/runs"
        }
        env {
          name  = "GROUND_TRUTH_PREFIX"
          value = "gs://${local.bucket_names.ground_truth}/runs"
        }
        env {
          name  = "RUN_ID"
          value = var.run_id
        }
        env {
          name  = "GIT_SHA"
          value = var.git_sha
        }
      }
    }
  }
  depends_on = [google_project_service.required]
}

resource "google_cloud_run_v2_job" "worker" {
  for_each            = var.deploy_jobs ? { for key, job in local.jobs : key => job if key != "l4" || var.deploy_l4 } : {}
  project             = var.project_id
  name                = each.value.name
  location            = var.region
  deletion_protection = false
  labels              = local.labels
  template {
    task_count  = each.key == "rtx6000" && var.rtx_canary_id != "" ? var.rtx_canary_tasks : 3
    parallelism = each.key == "rtx6000" && var.rtx_canary_id != "" ? var.rtx_canary_tasks : 3
    template {
      service_account               = google_service_account.worker.email
      max_retries                   = 0
      timeout                       = each.key == "rtx6000" && var.rtx_canary_id != "" ? "900s" : "1200s"
      gpu_zonal_redundancy_disabled = true
      dynamic "vpc_access" {
        for_each = each.key == "rtx6000" ? [true] : []
        content {
          egress = "ALL_TRAFFIC"
          network_interfaces {
            network    = google_compute_network.worker[0].id
            subnetwork = google_compute_subnetwork.worker[0].id
          }
        }
      }
      node_selector {
        accelerator = each.value.accelerator
      }
      containers {
        image = each.value.image
        volume_mounts {
          name       = "models"
          mount_path = "/mnt/models"
        }
        dynamic "volume_mounts" {
          for_each = each.key == "rtx6000" ? [true] : []
          content {
            name       = "model-cache"
            mount_path = "/mnt/model-cache"
          }
        }
        resources {
          limits = {
            cpu              = each.value.cpu
            memory           = each.value.memory
            "nvidia.com/gpu" = "1"
          }
        }
        env {
          name  = "MANIFEST_URI"
          value = "gs://${local.bucket_names.prepared}/runs/${var.run_id}/manifest.json"
        }
        env {
          name  = "MODEL_MANIFEST_URI"
          value = each.value.model_manifest
        }
        env {
          name  = "RESULTS_PREFIX"
          value = each.key == "rtx6000" && var.rtx_canary_id != "" ? "gs://${local.bucket_names.results}/canary/${var.rtx_canary_id}" : "gs://${local.bucket_names.results}"
        }
        env {
          name  = "GPU_PROFILE"
          value = each.key
        }
        env {
          name  = "SOFT_DEADLINE_SECONDS"
          value = each.key == "rtx6000" && var.rtx_canary_id != "" ? "600" : "1080"
        }
        env {
          name  = "TASK_TIMEOUT_SECONDS"
          value = each.key == "rtx6000" && var.rtx_canary_id != "" ? "900" : "1200"
        }
        env {
          name  = "EXPORT_COMPILE_CACHE"
          value = each.key == "rtx6000" && var.export_rtx_compile_cache ? "1" : "0"
        }
        env {
          name  = "REQUIRE_COMPILE_CACHE"
          value = each.key == "rtx6000" && var.require_rtx_compile_cache ? "1" : "0"
        }
        env {
          name  = "VLLM_USE_V2_MODEL_RUNNER"
          value = each.value.v2_runner
        }
        env {
          name  = "GIT_SHA"
          value = each.key == "rtx6000" ? local.worker_rtx_git_sha : var.git_sha
        }
        env {
          name  = "IMAGE_DIGEST"
          value = each.value.image
        }
      }
      volumes {
        name = "models"
        gcs {
          bucket    = google_storage_bucket.pipeline["models"].name
          read_only = true
        }
      }
      dynamic "volumes" {
        for_each = each.key == "rtx6000" ? [true] : []
        content {
          name = "model-cache"
          empty_dir {
            medium     = "MEMORY"
            size_limit = "24Gi"
          }
        }
      }
    }
  }
  depends_on = [google_project_service.required]
}
