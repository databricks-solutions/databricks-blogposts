terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.13.0"
    }

  }
}

provider "databricks" {
  host     = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}
