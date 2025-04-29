# Configure Terraform to use the required providers
terraform {
  required_providers {
    # Specify the Databricks provider and its source
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Configure the Databricks provider for account-level operations
provider "databricks" {
  # Create an alias for this provider instance to differentiate it from others
  alias         = "accounts"
  
  # Specify the host URL for the Databricks workspace or account
  host          = var.databricks_host
  
  # Databricks account ID for authentication
  account_id    = var.databricks_account_id
  
  # Client ID for authentication
  client_id     = var.databricks_client_id
  
  # Client secret for authentication
  client_secret = var.databricks_client_secret
}
