variable "workspace_name" {
  type        = string
  description = "(Required) Workspace name"
}

variable "workspace_id" {
  type        = string
  description = "(Required) Workspace ID from URL"
}

variable "subnet_region" {
  type        = string
  description = "(Required) GCP region where the assets will be deployed"
}

variable "vpc_id" {
  type        = string
  description = "(Required) GCP VPC ID"
}

variable "subnet_name" {
  type        = string
  description = "(Required) GCP VPC Subnet name from Databricks account console"
}

variable "databricks_account_id" {
  type        = string
  description = "(Required) Databricks Account ID"
}

variable "network_name" {
  type        = string
  description = "(Required) Network name from Databricks account console"
}

variable "network_id" {
  type        = string
  description = "(Required) Network ID from Databricks account console"
}

variable "workspace_google_project" {
  type        = string
  description = "(Required) The GCP Project ID from Databricks account console for workspace resources"
}

variable "network_google_project" {
  type        = string
  description = "(Required) The Network GCP Project ID from Databricks account console's network configuration page"
}