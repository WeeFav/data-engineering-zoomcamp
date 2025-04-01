terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.27.0"
    }
  }
}

provider "google" {
    credentials = "./keys/terraform-gcp-455419-1179cbb98622.json"
  project = "terraform-gcp-455419"
  region  = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-gcp-455419-terra-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}