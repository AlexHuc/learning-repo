terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# terraform init -> initializes the terraform configuration

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

#   lifecycle_rule {
#     condition {
#       age = 3
#     }
#     action {
#       type = "Delete"
#     }
#   }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# terraform fmt -> formats the terraform code

# terraform plan -> shows what changes will be made
# terraform apply -> applies the changes to reach the desired state
  # terraform.tfstate -> state file that keeps track of resources created by terraform and their current state
  # .terraform.tfstate.lock.info -> lock info file to prevent concurrent modifications to the state file
# terraform destroy -> destroys the resources created by terraform
  # terraform.tfstate.backup -> backup file for the state file
  # .terraform.lock.hcl -> lock file to prevent concurrent modifications to the state file

# If you want to push this to github you will need a terraform .gitignore file to ignore sensitive files

# terraform show -> shows the current state of the resources managed by terraform

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}