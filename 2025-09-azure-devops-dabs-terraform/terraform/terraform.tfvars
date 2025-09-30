# Update with your actual values

# ==============================================================================
# AZURE DEVOPS ORGANIZATION CONFIGURATION
# ==============================================================================
# Your Azure DevOps organization name (from https://dev.azure.com/{org_name})
organization_name = "zachjacobson"

# Your Azure DevOps organization GUID 
# Get this from Azure DevOps organization settings -> Microsoft Entra -> Download
organization_id = "c491b849-9990-4a0f-b50c-ea73352ae55c"

# Azure DevOps Personal Access Token with required permissions:
# - Project and Team: Read & Write
# - Service Connections: Read & Write  
# - Build: Read & Execute
azdo_personal_access_token = ""

# ==============================================================================
# PROJECT CONFIGURATION
# ==============================================================================
project_name        = "dabs-cicd-project"
project_description = "Description of your Azure DevOps project"
project_visibility  = "private"  # or "public"

pipeline_name     = "DAB-CI-Pipeline"
pipeline_yml_path = "azure-pipelines.yml"

# ==============================================================================
# MANAGEMENT RESOURCE GROUP
# ==============================================================================
# Resource Group where managed identities will be created (can be in any subscription)
resource_group_name = "databricks-mfg-central-rg"

# ==============================================================================
# DEV ENVIRONMENT CONFIGURATION
# ==============================================================================
# Dev Azure subscription details
azure_subscription_id_dev   = "edd4cc45-85c7-4aec-8bf5-648062d519bf"
azure_subscription_name_dev = "azure-sandbox-field-eng"
service_connection_name_dev  = "dabs-cicd-project-Dev-Connection"
databricks_host_dev         = "https://adb-2454423661594633.13.azuredatabricks.net/"

# ==============================================================================
# TEST ENVIRONMENT CONFIGURATION
# ==============================================================================
# Test Azure subscription details
azure_subscription_id_test   = "edd4cc45-85c7-4aec-8bf5-648062d519bf"
azure_subscription_name_test = "azure-sandbox-field-eng"
service_connection_name_test  = "dabs-cicd-project-Test-Connection"
databricks_host_test         = "https://adb-2454423661594633.13.azuredatabricks.net/"

# ==============================================================================
# PROD ENVIRONMENT CONFIGURATION
# ==============================================================================
# Prod Azure subscription details
azure_subscription_id_prod   = "edd4cc45-85c7-4aec-8bf5-648062d519bf"
azure_subscription_name_prod = "azure-sandbox-field-eng" 
service_connection_name_prod  = "dabs-cicd-project-Prod-Connection"
databricks_host_prod         = "https://adb-2454423661594633.13.azuredatabricks.net/"

# ==============================================================================
# INSTRUCTIONS FOR CUSTOMERS
# ==============================================================================
# 1. Copy this file to terraform.tfvars
# 2. Replace all YOUR_* placeholders with actual values
# 3. Ensure the management resource group exists before running terraform
# 4. Generate Azure DevOps PAT with required permissions
# 5. Run: terraform init && terraform plan && terraform apply
#
# This single deployment will create:
# - Azure DevOps project and pipeline
# - 3 variable groups (Dev, Test, Prod)
# - 3 managed identities (one per environment)
# - 3 service connections (one per environment/subscription)
#
# Required Azure DevOps PAT Permissions:
# - Project and Team (Read & Write)
# - Service Connections (Read & Write)
# - Build (Read & Execute)
#
# Prerequisites:
# - Azure CLI installed and logged in (az login)
# - Terraform installed
# - Access to all target subscriptions as Owner or Contributor
# - Azure DevOps organization with appropriate permissions