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
    
    # Specify the REST API Provider and its source, along with a specific version
    restapi = {
      source = "pruiz/restapi"
      version = "1.16.2-p2"
    }
    
    # Specify the AzAPI Provider and its source, along with a specific version
    azapi = {
      source = "Azure/azapi"
      version = "2.0.1"
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

# Configure the Databricks Provider for workspace-level operations
provider "databricks" {
  # Create an alias for this provider instance to differentiate it from others
  alias         = "workspace"
  
  # Host URL for the Databricks workspace
  host          = var.databricks_workspace_host
  
  # Client ID for Databricks authentication
  client_id     = var.databricks_client_id
  
  # Client secret for Databricks authentication
  client_secret = var.databricks_client_secret
}

# Configure the AzAPI Provider for Azure resources
provider "azapi" {
  # More information on the authentication methods supported by
  # the AzApi Provider can be found here:
  # https://registry.terraform.io/providers/Azure/azapi/latest/docs
  
  # Subscription ID for Azure authentication
  subscription_id = var.azure_subscription_id
  
  # Client ID for Azure authentication
  client_id       = var.azure_client_id
  
  # Client secret for Azure authentication
  client_secret   = var.azure_client_secret
  
  # Tenant ID for Azure authentication
  tenant_id       = var.azure_tenant_id
}

# Configure the REST API Provider for interacting with Databricks APIs
provider "restapi" {
  # Base URI for the REST API
  uri = var.databricks_host
  
  # OAuth client credentials for authentication
  oauth_client_credentials {
    # Client ID for OAuth authentication
    oauth_client_id      = var.databricks_client_id
    
    # Client secret for OAuth authentication
    oauth_client_secret  = var.databricks_client_secret
    
    # Token endpoint for obtaining an access token
    oauth_token_endpoint = "${var.databricks_host}/oidc/accounts/${var.databricks_account_id}/v1/token"
    
    # Scopes for the OAuth token (e.g., all APIs)
    oauth_scopes = ["all-apis"]
  }
}
