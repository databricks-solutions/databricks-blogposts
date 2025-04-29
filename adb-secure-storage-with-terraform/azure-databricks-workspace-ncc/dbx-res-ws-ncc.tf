// Retrieve the ID of the specified Azure Databricks Workspace
data "azurerm_databricks_workspace" "this" {
  # Name of the Databricks workspace to retrieve
  name                = var.databricks_workspace
  
  # Name of the resource group where the workspace resides
  resource_group_name = var.databricks_workspace_rg
}

# Create a Databricks MWS Network Connectivity Configuration
resource "databricks_mws_network_connectivity_config" "ncc" {
  # Use the Databricks provider configured for account-level operations
  provider = databricks.accounts
  
  # Name of the network connectivity configuration
  name     = "ncc-${var.databricks_workspace}"
  
  # Region where the configuration will be created
  region   = var.azure_region
}

# Bind the network connectivity configuration to a Databricks workspace
resource "databricks_mws_ncc_binding" "ncc_binding" {
  # Use the Databricks provider configured for account-level operations
  provider                       = databricks.accounts
  
  # ID of the network connectivity configuration to bind
  network_connectivity_config_id = databricks_mws_network_connectivity_config.ncc.network_connectivity_config_id
  
  # ID of the workspace to bind the configuration to
  workspace_id                   = data.azurerm_databricks_workspace.this.workspace_id
  
  # Ensure the binding depends on the network connectivity configuration being created first
  depends_on = [databricks_mws_network_connectivity_config.ncc]
}
