resource "azurerm_databricks_workspace" "this" {
  name                        = var.workspace_name
  resource_group_name         = var.resource_group_name
  managed_resource_group_name = var.managed_resource_group_name
  location                    = var.location
  sku                         = var.sku_type

  custom_parameters {
    
    no_public_ip                                         = true
    virtual_network_id                                   = var.virtual_network_id
    private_subnet_name                                  = var.private_subnet_name
    public_subnet_name                                   = var.public_subnet_name
  }
}

resource "azurerm_subnet_nat_gateway_association" "private" {
  subnet_id      = var.private_subnet_id
  nat_gateway_id = var.nat_gateway_id
}

resource "azurerm_subnet_nat_gateway_association" "public" {
  subnet_id      = var.public_subnet_id
  nat_gateway_id = var.nat_gateway_id
}