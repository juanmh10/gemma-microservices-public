variable "powerbi_reader" {
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
  description = "Google user or group used by Power BI; supply the IAM principal only in ignored private tfvars. Null revokes this module's grants."
  validation {
    condition     = var.powerbi_reader == null ? true : can(regex("^(user|group):[^\\s:@]+@[^\\s:@]+\\.[^\\s:@]+$", var.powerbi_reader))
    error_message = "Specify a Google user or group IAM principal; public identities and service-account keys are outside this scope."
  }
}

variable "provision_email_secret" {
  type        = bool
  default     = false
  description = "Create the protected empty Resend secret; does not deploy an email adapter or send messages."
}

variable "email_sender_domain" {
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
  description = "Private sender-domain allowlist; supply only in ignored local tfvars."
}

variable "enable_email" {
  type    = bool
  default = false
  validation {
    condition     = !var.enable_email || (var.provision_email_secret && var.deploy_messaging_compute && var.email_sender_domain != null && var.email_from != null && var.email_to != null && var.resend_secret_version != null)
    error_message = "Email requires messaging compute, the protected secret, a pinned version and private sender/recipient configuration."
  }
}
variable "email_from" {
  type      = string
  default   = null
  sensitive = true
}
variable "email_to" {
  type      = string
  default   = null
  sensitive = true
}
variable "resend_secret_version" {
  type    = string
  default = null
  validation {
    condition     = var.resend_secret_version == null ? true : can(regex("^[1-9][0-9]*$", var.resend_secret_version))
    error_message = "Pin an enabled numeric secret version; never use latest."
  }
}

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
    error_message = "Application infrastructure stays in us-central1."
  }
}
variable "vertex_location" {
  type    = string
  default = "us"
  validation {
    condition     = var.vertex_location == "us"
    error_message = "The approved Vertex endpoint is us."
  }
}
variable "analysis_image" {
  type        = string
  description = "Published immutable analysis image; local OCI digest must be verified in Artifact Registry before apply."
  validation {
    condition     = can(regex("^us-central1-docker[.]pkg[.]dev/[a-z][a-z0-9-]+/pipeline/analysis@sha256:[a-f0-9]{64}$", var.analysis_image))
    error_message = "Use the exact analysis image digest in the pipeline repository."
  }
}
variable "source_revision" {
  type = string
  validation {
    condition     = can(regex("^[a-f0-9]{40}$", var.source_revision))
    error_message = "A source Git revision is required; also record the workspace source hash when dirty."
  }
}
variable "source_sha256" {
  type = string
  validation {
    condition     = can(regex("^[a-f0-9]{64}$", var.source_sha256))
    error_message = "Record the source-content SHA-256 of the local image artifact."
  }
}
variable "request_uri" {
  type = string
  validation {
    condition     = can(regex("^gs://[a-z0-9._-]+/analysis-requests/[a-z][a-z0-9-]{0,62}[.]json$", var.request_uri))
    error_message = "Use a verified analysis request in the fixed request prefix."
  }
}

variable "deploy_analysis" {
  type        = bool
  default     = false
  description = "Keep the billable model analysis milestone paused unless explicitly enabled."
}
variable "deploy_data_compute" {
  type        = bool
  default     = false
  description = "Deploy index/API compute and its identities; analytics tables persist independently."
}
variable "deploy_api" {
  type        = bool
  default     = false
  description = "Restore only the private read-only API; does not deploy the index Job."
}
variable "data_image" {
  type     = string
  default  = null
  nullable = true
  validation {
    condition     = var.data_image == null ? !(var.deploy_data_compute || var.deploy_api) : can(regex("^us-central1-docker[.]pkg[.]dev/[a-z][a-z0-9-]+/pipeline/analytics@sha256:[a-f0-9]{64}$", var.data_image))
    error_message = "Use the immutable analytics image digest before enabling data compute."
  }
}
variable "api_invoker" {
  type        = string
  default     = null
  nullable    = true
  description = "Expected operator IAM principal; supplied only through ignored local tfvars."
  validation {
    condition     = var.api_invoker == null ? !(var.deploy_data_compute || var.deploy_api) : can(regex("^(user|serviceAccount):[^ ]+@[^ ]+$", var.api_invoker))
    error_message = "Specify the authorized private API invoker; public principals are forbidden."
  }
}
variable "index_analysis_id" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,62}$", var.index_analysis_id))
    error_message = "Use a completed allowlisted analysis ID."
  }
}

variable "deploy_messaging_queues" {
  type    = bool
  default = false
}
variable "deploy_messaging_compute" {
  type    = bool
  default = false
  validation {
    condition     = !var.deploy_messaging_compute || var.deploy_messaging_queues
    error_message = "Messaging compute requires retained queue infrastructure."
  }
}
variable "messaging_image" {
  type     = string
  default  = null
  nullable = true
  validation {
    condition     = var.messaging_image == null ? !var.deploy_messaging_compute : can(regex("^us-central1-docker[.]pkg[.]dev/[a-z][a-z0-9-]+/pipeline/messaging@sha256:[a-f0-9]{64}$", var.messaging_image))
    error_message = "Use a verified immutable messaging image digest."
  }
}
variable "worker_a_canary_id" {
  type        = string
  default     = ""
  description = "Explicit new Worker A acceptance prefix; empty retains the original canary-only reader."
  validation {
    condition     = var.worker_a_canary_id == "" || can(regex("^pipeline-[a-z0-9-]{3,30}$", var.worker_a_canary_id))
    error_message = "Use an explicitly reviewed pipeline canary identifier."
  }
}

variable "prepared_run_id" {
  type        = string
  description = "Exact prepared and ground-truth run readable by Worker B."
  validation {
    condition     = var.prepared_run_id == "pilot-004" || can(regex("^pipeline-[a-z0-9-]{3,30}$", var.prepared_run_id))
    error_message = "Use pilot-004 or an isolated pipeline-* run."
  }
}
