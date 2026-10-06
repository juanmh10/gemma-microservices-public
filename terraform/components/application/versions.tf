terraform {
  required_version = ">= 1.8.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

# Independent local state by default. Do not reuse the Foundation backend/state.
provider "google" {
  project = var.project_id
  region  = var.region
}
