variable "project_id" {
  type    = string
  default = "your-gcp-project-id"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Project ID must be a valid GCP project identifier (6-30 lowercase characters)."
  }
}

variable "region" {
  type    = string
  default = "us-central1"
  validation {
    condition     = var.region == "us-central1"
    error_message = "Foundation is scoped to us-central1."
  }
}

variable "run_id" {
  type = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,40}$", var.run_id))
    error_message = "Use a short lowercase run ID."
  }
}

variable "pilot_records" {
  type        = number
  description = "Explicit source population size; zero means all. The legacy variable name remains compatible with the frozen worker profile."
  validation {
    condition     = var.pilot_records == floor(var.pilot_records) && var.pilot_records >= 0 && (var.pilot_records == 0 || var.pilot_records >= 3)
    error_message = "Use zero for all records or at least three records for three shards."
  }
}

variable "prompt_version" {
  type        = string
  default     = "worker-a-v1"
  description = "Prompt contract declared by the preparer; must match the worker image."
  validation {
    condition     = contains(["worker-a-v1", "worker-a-v2", "worker-a-v3"], var.prompt_version)
    error_message = "Use a versioned Worker A prompt."
  }
}

variable "deploy_jobs" {
  type        = bool
  description = "Explicitly choose whether the preparer and RTX jobs are managed in this plan."
}

variable "retain_worker_network" {
  type        = bool
  default     = false
  description = "Retain the managed worker network during shutdown while a Cloud Run serverless address prevents subnet deletion."
}

variable "deploy_l4" {
  type        = bool
  default     = false
  description = "Excluded from MVP acceptance; optional L4 benchmark requires separate approval, quota and a pinned image."
}

variable "rtx_canary_id" {
  type        = string
  default     = ""
  description = "Nonempty ID selects one RTX task, parallelism one, a 15-minute timeout, and an isolated canary results prefix."
  validation {
    condition     = var.rtx_canary_id == "" || can(regex("^[a-z0-9][a-z0-9-]{2,40}$", var.rtx_canary_id))
    error_message = "Use a short lowercase canary ID or leave it empty for the three-task pilot."
  }
}

variable "export_rtx_compile_cache" {
  type        = bool
  default     = false
  description = "Export bounded AOT artifacts from one approved RTX bootstrap canary."
  validation {
    condition     = !var.export_rtx_compile_cache || (var.rtx_canary_id != "" && !var.require_rtx_compile_cache)
    error_message = "Cache export requires a one-task canary and cannot require a baked cache."
  }
}

variable "require_rtx_compile_cache" {
  type        = bool
  default     = false
  description = "Require a compatible baked AOT cache; fail instead of recompiling."
}

variable "git_sha" {
  type        = string
  default     = ""
  description = "Source commit built into the pinned container images."
  validation {
    condition     = !var.deploy_jobs || can(regex("^[0-9a-f]{40}$", var.git_sha))
    error_message = "Set a full Git SHA when deploying jobs."
  }
}

variable "worker_rtx_git_sha" {
  type        = string
  default     = ""
  description = "Source commit for the RTX worker image when it differs from the preparer image."
  validation {
    condition     = var.worker_rtx_git_sha == "" || can(regex("^[0-9a-f]{40}$", var.worker_rtx_git_sha))
    error_message = "Set a full Git SHA for the RTX worker image."
  }
}

variable "preparer_image" {
  type        = string
  default     = ""
  description = "Artifact Registry image URL pinned by @sha256 digest."
  validation {
    condition     = !var.deploy_jobs || can(regex("@sha256:[0-9a-f]{64}$", var.preparer_image))
    error_message = "Pin the preparer image by digest."
  }
}

variable "worker_l4_image" {
  type        = string
  default     = ""
  description = "Worker A image URL pinned by @sha256 digest for L4."
  validation {
    condition     = !var.deploy_jobs || !var.deploy_l4 || can(regex("@sha256:[0-9a-f]{64}$", var.worker_l4_image))
    error_message = "Pin the L4 image by digest when deploying L4."
  }
}

variable "worker_rtx_image" {
  type        = string
  default     = ""
  description = "Worker A image URL pinned by @sha256 digest for RTX PRO 6000."
  validation {
    condition     = !var.deploy_jobs || can(regex("@sha256:[0-9a-f]{64}$", var.worker_rtx_image))
    error_message = "Pin the RTX image by digest."
  }
}

variable "l4_quota_contact_email" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Contact email required by Cloud Quotas for the L4 quota increase request."
  validation {
    condition     = !var.deploy_jobs || can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", var.l4_quota_contact_email))
    error_message = "Set an L4 quota contact email in the ignored tfvars file."
  }
}

variable "domain_config_uri" {
  type        = string
  default     = ""
  description = "Versioned domain JSON read by the preparer for prompt v3."
}

variable "canonical_source" {
  type        = bool
  default     = false
  description = "Use canonical dialogue-only input with separate reference JSONL (prompt v3)."
}
variable "dataset_version" {
  type        = string
  default     = "gym-sales-v1"
  description = "Versioned input dataset identity recorded in v3 manifests."
}
variable "source_uri" {
  type        = string
  default     = ""
  description = "Optional raw-bucket source object; empty retains the pinned gym source."
}
variable "metadata_uri" {
  type        = string
  default     = ""
  description = "Optional raw-bucket isolated reference source; never passed to Worker A."
}

variable "deploy_preparer" {
  type        = bool
  default     = true
  description = "Create the preparer definition when jobs are deployed; disable for worker-only validation using existing shards."
}

variable "preparer_shards" {
  type        = number
  default     = 3
  description = "Preserve historical preparation; use one shard for a fresh bounded pipeline."
  validation {
    condition     = contains([1, 3], var.preparer_shards) && (var.pilot_records == 0 || var.pilot_records >= var.preparer_shards)
    error_message = "Preparation supports one or three nonempty shards."
  }
}

variable "rtx_canary_tasks" {
  type        = number
  default     = 1
  description = "One bounded canary by default; three parallel tasks require the full 900-record profile and explicit execution approval."
  validation {
    condition     = contains([1, 3], var.rtx_canary_tasks)
    error_message = "Use one or three bounded RTX tasks."
  }
}
