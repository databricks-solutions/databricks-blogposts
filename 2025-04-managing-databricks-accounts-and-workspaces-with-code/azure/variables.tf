variable "workspace_name" {
  type        = string
  default     = ""
  description = "(Required) Workspace Name for this module - if none are provided, the prefix will be used to name the workspace via coalesce()"
}

variable "private_subnet_name" {
  type        = string
  description = "(Required) Name of subnet for internal cluster communication"
}

variable "public_subnet_name" {
  type        = string
  description = "(Required Name of subnet for cluster host"
}

variable "private_subnet_id" {
  type        = string
  description = "(Required) Private subnet resource ID from Azure portal"
}

variable "public_subnet_id" {
  type        = string
  description = "(Required) Public subnet resource ID from Azure portal"
}

variable "nat_gateway_id" {
  type        = string
  description = "(Required) NAT Gateway resource ID from Azure portal"
}

variable "virtual_network_id" {
  type        = string
  description = "(Required) Virtual network resource ID from Azure portal"
}

variable "sku_type" {
  type        = string
  description = "(Required) Pricing tier from Azure Portal"
  validation {
    condition     = contains(["premium", "standard"], var.sku_type)
    error_message = "Invalid instance type. Allowed values are standard or premium."
  }
}

variable "location" {
  type        = string
  description = "(Required) Databricks workspace region"
}

variable "resource_group_name" {
  type        = string
  description = "(Required) Resource group name from Azure Portal"
}

variable "managed_resource_group_name" {
  type        = string
  description = "(Required) Databricks managed resource group name from Azure Portal"
}
