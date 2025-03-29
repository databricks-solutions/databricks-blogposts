// Retrieve the Azure Virtual Network (VNet) for the Databricks workspace
data "azurerm_virtual_network" "ws_vnet" {
  # Name of the VNet to retrieve
  name                = var.databricks_workspace_vnet
  
  # Name of the resource group where the VNet resides
  resource_group_name = var.databricks_workspace_vnet_rg
}

// Create a private link subnet within the VNet
resource "azurerm_subnet" "plsubnet" {
  # Name of the subnet to create
  name                                      = "${var.name_prefix}-privatelink"
  
  # Name of the resource group where the subnet will reside
  resource_group_name                       = var.databricks_workspace_vnet_rg
  
  # Name of the VNet where the subnet will be created
  virtual_network_name                      = var.databricks_workspace_vnet
  
  # Address prefix for the subnet
  address_prefixes                          = [var.pl_subnets_cidr]
}

// Create a private DNS zone for Data Lake Storage (DBFS)
resource "azurerm_private_dns_zone" "dnsdbfs" {
  # Name of the private DNS zone to create
  name                = "privatelink.dfs.core.windows.net"
  
  # Name of the resource group where the DNS zone will reside
  resource_group_name = var.databricks_workspace_vnet_rg
  
  # Tags to apply to the DNS zone for organization and billing
  tags                = var.tags
}

// Link the private DNS zone to the VNet
resource "azurerm_private_dns_zone_virtual_network_link" "dbfsdnszonevnetlink" {
  # Name of the DNS zone link to create
  name                  = "dbfsvnetconnection"
  
  # Name of the resource group where the link will reside
  resource_group_name   = var.databricks_workspace_vnet_rg
  
  # Name of the private DNS zone to link
  private_dns_zone_name = azurerm_private_dns_zone.dnsdbfs.name
  
  # ID of the VNet to link to the DNS zone
  virtual_network_id    = data.azurerm_virtual_network.ws_vnet.id // Connect to the spoke VNet
  
  # Tags to apply to the link for organization and billing
  tags                  = var.tags
  
  # Ensure the link depends on both the DNS zone and VNet being created first
  depends_on = [azurerm_private_dns_zone.dnsdbfs, data.azurerm_virtual_network.ws_vnet]
}

// Create a private endpoint for the workspace to access the data storage (catalog external location)
resource "azurerm_private_endpoint" "data" {
  # Name of the private endpoint to create
  name                = "datapvtendpoint"
  
  # Location where the private endpoint will be created
  location            = var.azure_region
  
  # Name of the resource group where the private endpoint will reside
  resource_group_name = var.databricks_workspace_vnet_rg
  
  # ID of the subnet where the private endpoint will be created
  subnet_id           = azurerm_subnet.plsubnet.id
  
  # Tags to apply to the private endpoint for organization and billing
  tags                = var.tags
  
  # Configure the private service connection
  private_service_connection {
    # Name of the private connection
    name                           = "ple-${var.name_prefix}-data"
    
    # ID of the storage account resource to connect to
    private_connection_resource_id = azurerm_storage_account.this.id
    
    # Use automatic connection approval
    is_manual_connection           = false
    
    # Subresource names to connect to (e.g., dfs for Data Lake Storage)
    subresource_names              = ["dfs"]
  }
  
  # Configure the private DNS zone group
  private_dns_zone_group {
    # Name of the DNS zone group
    name                 = "private-dns-zone-dbfs"
    
    # IDs of the private DNS zones to include in the group
    private_dns_zone_ids = [azurerm_private_dns_zone.dnsdbfs.id]
  }
  
  # Ensure the private endpoint depends on the subnet, DNS zone, and storage account being created first
  depends_on = [azurerm_subnet.plsubnet, azurerm_private_dns_zone.dnsdbfs, azurerm_storage_account.this]
}
