variable "credentials" {
  description = "credentials"
  default = "./.keys/terraform-gcp-455419-1179cbb98622.json"
}

variable "location" {
  description = "project location"
  default = "US"
}

variable "bq_dataset_name" {
  description = "bigquery dataset name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "storage bucket name"
  default = "terraform-gcp-455419-terra-bucket"
}

variable "gcs_storage_class" {
  description = "bigquery dataset name"
  default = "STANDARD"
}