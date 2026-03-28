import {
  to = azurerm_databricks_workspace.this
  id = var.workspace_id
}

import {
  to = azurerm_subnet_nat_gateway_association.private
  id = var.private_subnet_id
}

import {
  to = azurerm_subnet_nat_gateway_association.public
  id = var.public_subnet_id
}
