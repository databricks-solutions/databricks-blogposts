# Variable for the Azure region where resources will be deployed
variable "azure_region" {
  type    = string
  default = ""
}

# Variable for the name of the resource group to create
variable "rg_name" {
  type    = string
  default = ""
}

# Variable for the prefix used in naming resources
variable "name_prefix" {
  type    = string
  default = ""
}

# Variable for the name of the storage account for DBFS
variable "dbfs_storage_account" {
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

# Variable for the CIDR block range of the virtual network
variable "cidr_block" {
  description = "VPC CIDR block range"
  type        = string
  default     = "10.20.0.0/23"
}

// Commented out variable for whitelisted URLs (not currently used)
//variable "whitelisted_urls" {
//  default = [".pypi.org", ".pythonhosted.org", ".cran.r-project.org"]
//}

# Variable for the CIDR block of the private subnet for cluster containers
variable "private_subnets_cidr" {
  type = string
  default = "10.20.0.0/25"
}

# Variable for the CIDR block of the public subnet for cluster hosts
variable "public_subnets_cidr" {
  type = string
  default = "10.20.0.128/25"
}

# Variable for service endpoints to enable on subnets
variable "subnet_service_endpoints" {
  type = list(string)
  default = []
}

# Variable to control whether network security group rules are required
variable "network_security_group_rules_required" {
  type = string
  default = "AllRules"
  # Options:
  #   - "AllRules"
  #   - "NoAzureServiceRules"
  #   - "NoAzureDatabricksRules" (use with private link)
}

# Variable to control whether public access to the default storage account is disallowed
variable "default_storage_firewall_enabled" {
  description = "Disallow public access to default storage account"
  type = bool
  default = false
}

# Variable to control whether public access to the frontend workspace web UI is allowed
variable "public_network_access_enabled" {
  description = "Allow public access to frontend workspace web UI"
  type = bool
  default = true
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

# Variable for the name of the Databricks Unity Catalog metastore
variable "databricks_metastore" {
  description = "Databricks UC Metastore"
  type        = string
  default     = ""
}

# Variable for tags to apply to resources for organization and billing
variable "tags" {
  default = ""
}
