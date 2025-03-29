// Retrieve the network connectivity configuration (NCC) ID using the Databricks REST API
data "restapi_object" "ncc" {
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

// Add a private endpoint rule for the NCC to access the storage account
resource "databricks_mws_ncc_private_endpoint_rule" "storage" {
  # Use the Databricks provider configured for account-level operations
  provider                       = databricks.accounts
  
  # ID of the network connectivity configuration to add the rule to
  network_connectivity_config_id = jsondecode(data.restapi_object.ncc.api_response).network_connectivity_config_id
  
  # ID of the storage account resource
  resource_id                    = azurerm_storage_account.this.id
  
  # Group ID for the private endpoint rule (e.g., dfs for Data Lake Storage)
  group_id                       = "dfs"
  
  # Ensure the rule depends on both the NCC data and storage account being created first
  depends_on = [data.restapi_object.ncc, azurerm_storage_account.this]
}

// Retrieve the list of private endpoint connections for the storage account
data "azapi_resource_list" "list_storage_private_endpoint_connection" {
  # Type of resource to retrieve (e.g., private endpoint connections for storage accounts)
  type                   = "Microsoft.Storage/storageAccounts/privateEndpointConnections@2022-09-01"
  
  # Parent ID of the storage account resource
  parent_id              = "/subscriptions/${var.azure_subscription_id}/resourceGroups/${var.data_storage_account_rg}/providers/Microsoft.Storage/storageAccounts/${var.data_storage_account}"
  
  # Export all response values
  response_export_values = ["*"]
  
  # Ensure the data depends on the private endpoint rule being created first
  depends_on = [databricks_mws_ncc_private_endpoint_rule.storage]
}

// Approve the private endpoint connection for the storage account
resource "azapi_update_resource" "approve_storage_private_endpoint_connection" {
  # Type of resource to update (e.g., private endpoint connections for storage accounts)
  type      = "Microsoft.Storage/storageAccounts/privateEndpointConnections@2022-09-01"
  
  # Name of the private endpoint connection to approve
  name      = [
    for i in data.azapi_resource_list.list_storage_private_endpoint_connection.output.value 
    : i.name if endswith(i.properties.privateEndpoint.id, databricks_mws_ncc_private_endpoint_rule.storage.endpoint_name)
  ][0]
  
  # Parent ID of the storage account resource
  parent_id = azurerm_storage_account.this.id
  
  # Body of the update request
  body = {
    properties = {
      # Update the private link service connection state to approved
      privateLinkServiceConnectionState = {
        description = "Auto Approved via Terraform"
        status      = "Approved"
      }
    }
  }
  
  # Ensure the update depends on the list of private endpoint connections being retrieved first
  depends_on = [data.azapi_resource_list.list_storage_private_endpoint_connection]
}
