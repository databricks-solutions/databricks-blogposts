# Variable for the Azure region where resources will be deployed
variable "azure_region" {
  type    = string
  default = ""
}

# Variable for the Azure tenant ID used for authentication
variable "azure_tenant_id" {
  type  = string
  default = ""
}

# Variable for the Azure subscription ID used for authentication
variable "azure_subscription_id" {
  type  = string
  default = ""
}

# Variable for the client ID used for Azure authentication
variable "azure_client_id" {
  type  = string
  default = ""
}

# Variable for the client secret used for Azure authentication
variable "azure_client_secret" {
  type  = string
  default = ""
}

# Variable for the Databricks account URL
variable "databricks_host" {
  description = "Databricks Account URL"
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

# Variable for the name of the Databricks workspace
variable "databricks_workspace" {
  description = "Name of Databricks workspace"
  type = string
  default = ""
}

# Variable for the name of the resource group containing the Databricks workspace
variable "databricks_workspace_rg" {
  description = "Name of Databricks workspace resource group"
  type = string
  default = ""
}

# Variable for tags to apply to resources for organization and billing
variable "tags" {
  default = ""
}
