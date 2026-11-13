variable "tenant_id" {
  description = "Entra tenant (directory) ID that hosts the subscription and the Databricks account."
  type        = string
}

variable "subscription_id" {
  description = "Azure subscription for the resource group, managed identities and APIM."
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID (accounts.azuredatabricks.net, top-right user menu)."
  type        = string
}

variable "workspace_id" {
  description = "Numeric ID of the existing Azure Databricks workspace that hosts the Lakebase project."
  type        = string
}

variable "workspace_url" {
  description = "URL of that workspace, for example https://adb-1234567890123456.7.azuredatabricks.net (no trailing slash)."
  type        = string
}

variable "data_api_url" {
  description = "URL of the Lakebase Data API for the database, as shown on the Lakebase project's Data API page (no trailing slash)."
  type        = string
}

variable "lakebase_endpoint" {
  description = "Resource name of the Lakebase endpoint the setup scripts connect to: projects/<project>/branches/<branch>/endpoints/<endpoint>."
  type        = string
}

variable "lakebase_database" {
  description = "Resource name of the Lakebase database behind the Data API: projects/<project>/branches/<branch>/databases/<database>."
  type        = string
}

variable "location" {
  description = "Azure region for the resource group, managed identities and APIM."
  type        = string
  default     = "eastus2"
}

variable "prefix" {
  description = "Short lowercase prefix for resource names (letters and digits only, max 10 chars)."
  type        = string
  default     = "lbapim"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,9}$", var.prefix))
    error_message = "prefix must be 2-10 lowercase letters/digits, starting with a letter."
  }
}

variable "apim_publisher_email" {
  description = "Publisher email for the APIM instance (required by Azure)."
  type        = string
}

variable "apim_sku" {
  description = "APIM SKU. Basic v2 deploys in minutes and includes the built-in cache the policies use."
  type        = string
  default     = "BasicV2_1"
}

variable "tags" {
  description = "Tags applied to Azure resources."
  type        = map(string)
  default = {
    purpose = "lakebase-data-api-apim"
  }
}

variable "enable_mi_api" {
  description = "Deploy the single-identity API at /lakebase: APIM exchanges its managed identity's Entra token for a Databricks token. Callers use a subscription key."
  type        = bool
  default     = true
}

variable "enable_app_role_rls" {
  description = "Deploy app role access at /lakebase-rls: Entra app registrations with app roles Orders.AppRoleA and Orders.AppRoleB, one managed identity per app role, and row-level security in Postgres."
  type        = bool
  default     = true
}

variable "enable_secret_alternative" {
  description = "Deploy the OAuth-secret alternative at /lakebase-s: APIM gets its Databricks token with client_credentials and a stored Databricks OAuth secret."
  type        = bool
  default     = true
}

variable "databricks_secret_lifetime_days" {
  description = "Lifetime of the Databricks OAuth secret used by the OAuth-secret alternative (max 730)."
  type        = number
  default     = 90
}

variable "entra_name_prefix" {
  description = "Display-name prefix for the Entra app registrations (<prefix>-gateway, <prefix>-client-a, ...)."
  type        = string
  default     = "lakebase"
}

variable "client_secret_lifetime_days" {
  description = "Lifetime in days of the sample client apps' secrets (entra.tf), used to get test tokens. Changing it replaces the secrets."
  type        = number
  default     = 90
}

locals {
  # The shared managed identity (identities.tf) serves the /lakebase and /lakebase-s APIs.
  shared_mi    = var.enable_mi_api || var.enable_secret_alternative
  app_role_rls = var.enable_app_role_rls
  secret_api   = var.enable_secret_alternative
  # App roles (entra.tf). Each needs a <when> branch and a named value in
  # ../policies/lakebase-data-api-app-role-rls.xml and its group role in ../sql/03-app-role-rls.sql, so the list is fixed here.
  app_roles = local.app_role_rls ? toset(["a", "b"]) : toset([])
}
