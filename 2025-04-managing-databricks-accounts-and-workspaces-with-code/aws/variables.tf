variable "workspace_name" {
  type        = string
  description = "(Required) Workspace name"
}

variable "workspace_id" {
  type        = string
  description = "(Required) Workspace ID"
}

variable "region" {
  type        = string
  description = "(Required) AWS region where the assets will be deployed"
}

variable "vpc_id" {
  type        = string
  description = "(Required) AWS VPC ID"
}

variable "security_group_ids" {
  type        = list(string)
  description = "(Required) List of VPC network security group IDs from Databricks Account Console"
}

variable "vpc_private_subnets" {
  type        = list(string)
  description = "(Required) AWS VPC Subnet IDs from Databricks account console"
}

variable "databricks_account_id" {
  type        = string
  description = "(Required) Databricks Account ID"
}

variable "cross_account_role_arn" {
  type        = string
  description = "(Required) AWS cross account role ARN from Databricks account console"
}

variable "root_storage_bucket" {
  type        = string
  description = "(Required) AWS root storage bucket"
}

variable "credentials_name" {
  type        = string
  description = "(Required) Credentials name from Databricks account console"
}

variable "credentials_id" {
  type        = string
  description = "(Required) Credentials ID from Databricks account console"
}

variable "network_name" {
  type        = string
  description = "(Required) Network name from Databricks account console"
}

variable "network_id" {
  type        = string
  description = "(Required) Network configuration ID from Databricks account console"
}

variable "storage_configuration_name" {
  type        = string
  description = "(Required) Storage name from Databricks account console"
}

variable "storage_configuration_id" {
  type        = string
  description = "(Required) Storage configuration name from Databricks account console"
}