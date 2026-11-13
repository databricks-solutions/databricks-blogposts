terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.8"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.136"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

# All providers authenticate with the Azure CLI session (`az login --tenant <tenant_id>`).
# No secrets are configured here.

provider "azurerm" {
  features {
    # Deleting APIM soft-deletes it and keeps its name for 48 hours. Purge on destroy, so a re-apply can reuse it.
    api_management {
      purge_soft_delete_on_destroy = true
      recover_soft_deleted         = false
    }
  }
  tenant_id       = var.tenant_id
  subscription_id = var.subscription_id
}

# Entra app registrations for app role access (entra.tf).
provider "azuread" {
  tenant_id = var.tenant_id
}

# Databricks account: service principals for the managed identities, workspace assignment, federation policies.
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
  auth_type  = "azure-cli"
  # Pin the Entra tenant: with several Entra tenants in the az CLI cache, the token can otherwise come from the wrong one
  # ("Expected iss claim to be https://sts.windows.net/<tenant_id>/").
  azure_tenant_id = var.tenant_id
}
