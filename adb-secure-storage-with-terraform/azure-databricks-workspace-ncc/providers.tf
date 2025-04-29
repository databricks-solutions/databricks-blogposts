# We strongly recommend using the required_providers block to set the
# Azure Provider source and version being used
terraform {
  required_providers {
    # Specify the Azure Provider and its source
    azurerm = {
      source = "hashicorp/azurerm"
    }
    
    # Specify the Databricks Provider and its source
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  # Enable features for the Azure Provider
  features {}
  
  # Tenant ID for Azure authentication
  tenant_id       = var.azure_tenant_id
  
  # Subscription ID for Azure authentication
  subscription_id = var.azure_subscription_id
  
  # Client ID for Azure authentication
  client_id       = var.azure_client_id
  
  # Client secret for Azure authentication
  client_secret   = var.azure_client_secret
}

# Configure the Databricks Provider for account-level operations
provider "databricks" {
  # Create an alias for this provider instance to differentiate it from others
  alias         = "accounts"
  
  # Host URL for the Databricks workspace or account
  host          = var.databricks_host
  
  # Databricks account ID for authentication
  account_id    = var.databricks_account_id
  
  # Client ID for Databricks authentication
  client_id     = var.databricks_client_id
  
  # Client secret for Databricks authentication
  client_secret = var.databricks_client_secret
}
