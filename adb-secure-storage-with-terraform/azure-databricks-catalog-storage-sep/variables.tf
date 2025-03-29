# Variable for the Azure region where resources will be deployed
variable "azure_region" {
  type    = string
  default = ""
}

# Variable for the Azure tenant ID used for authentication
variable "azure_tenant_id" {
  type    = string
  default = ""
}

# Variable for the Azure subscription ID used for authentication
variable "azure_subscription_id" {
  type    = string
  default = ""
}

# Variable for the Azure service principal client ID with necessary permissions
variable "azure_client_id" {
  description = "Azure service principal with needed permissions"
  type    = string
  default = ""
}

# Variable for the Azure service principal client secret
variable "azure_client_secret" {
  type    = string
  default = ""
}

# Variable for the Databricks account URL
variable "databricks_host" {
  description = "Databricks Account URL"
  type        = string
  default     = ""
}

# Variable for the Databricks workspace URL (if different from account host)
variable "databricks_workspace_host" {
  description = "Databricks Workspace URL"
  type        = string
  default     = ""
}

# Variable for the Databricks account ID
variable "databricks_account_id" {
  description = "Your Databricks Account ID"
  type        = string
  default = ""
}

# Variable for the Databricks account client ID (service principal with account admin role)
variable "databricks_client_id" {
  description = "Databricks Account Client Id (databricks service principal - account admin)"
  type        = string
  default     = ""
}

# Variable for the Databricks account client secret
variable "databricks_client_secret" {
  description = "Databricks Account Client Secret"
  type        = string
  default     = ""
}

# Variable for the resource group name of the ADLS storage account
variable "data_storage_account_rg" {
  description = "ADLS Storage account resource group"
  type        = string
  default     = ""
}

# Variable for the name of the ADLS storage account
variable "data_storage_account" {
  description = "ADLS Storage account Name"
  type        = string
  default     = ""
}

# Variable for the list of allowed IP addresses for the storage account
variable "storage_account_allowed_ips" {
  type = list(string)
  default = []
}

# Variable for the name of the virtual network for the Databricks workspace
variable "databricks_workspace_vnet" {
  description = "Name of Databricks workspace VNET"
  type = string
  default = ""
}

# Variable for the name of the resource group containing the Databricks workspace virtual network
variable "databricks_workspace_vnet_rg" {
  description = "Name of Databricks workspace VNET resource group"
  type = string
  default = ""
}

# Variable for the name of the network connectivity configuration for the workspace
variable "workspace_ncc_name" {
  description = "Name of Databricks workspace NCC"
  type = string
  default = ""
}

# Variable for additional subnets (not currently used)
variable "additional_subnets" {
  type = list(string)
  default = []
}

// Prefix for naming Databricks resources
variable "name_prefix" {
  type    = string
  default = ""
}

# Variable for the name of the Unity Catalog metastore
variable "databricks_metastore" {
  description = "Name of the UC metastore"
  type    = string
  default = ""
}

# Variable for the name of the catalog in the metastore
variable "databricks_calalog" {
  description = "Name of catalog in metastore"
  type        = string
  default     = ""
}

# Variable for the name of the principal to grant access to the catalog
variable "principal_name" {
  description = "Name of principal to grant access to catalog"
  type        = string
  default     = ""
}

# Variable for the list of privileges to grant to the principal on the catalog
variable "catalog_privileges" {
  description = "List of Privileges to catalog (grant to principal_name)"
  type        = list(string)
  default     = ["BROWSE"]
}

# Variable for tags to apply to resources for organization and billing
variable "tags" {
  default = {
    Owner = ""
  }
}
