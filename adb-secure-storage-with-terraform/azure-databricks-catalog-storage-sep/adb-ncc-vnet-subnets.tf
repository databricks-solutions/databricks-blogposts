// Retrieve the Azure Virtual Network (VNet) for the Databricks workspace
data "azurerm_virtual_network" "ws_vnet" {
  # Name of the VNet to retrieve
  name                = var.databricks_workspace_vnet
  
  # Name of the resource group where the VNet resides
  resource_group_name = var.databricks_workspace_vnet_rg
}

// Retrieve the subnets within the Databricks workspace VNet
data "azurerm_subnet" "ws_subnets" {
  # Iterate over each subnet in the VNet
  for_each             = toset(data.azurerm_virtual_network.ws_vnet.subnets)
  
  # Name of the subnet to retrieve
  name                 = each.value
  
  # Name of the VNet where the subnet resides
  virtual_network_name = var.databricks_workspace_vnet
  
  # Name of the resource group where the VNet resides
  resource_group_name  = var.databricks_workspace_vnet_rg
}

// Retrieve the network connectivity configuration for the workspace using the Databricks REST API
data "restapi_object" "subnets" {
  # Path to the API endpoint for network connectivity configurations
  path = "/api/2.0/accounts/${var.databricks_account_id}/network-connectivity-configs"
  
  # Key to search for in the API response
  search_key = "name"
  
  # Value to match in the search
  search_value = var.workspace_ncc_name
  
  # Attribute containing the ID of the network connectivity configuration
  id_attribute = "network_connectivity_config_id"
  
  # Key in the API response containing the list of results
  results_key = "items"
}
