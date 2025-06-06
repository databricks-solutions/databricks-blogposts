#!/bin/bash

# Define variables
azure_workspace_resource_id=""
public_subnet_resource_id=""
private_subnet_resource_id= ""

terraform import azurerm_databricks_workspace.this "${azure_workspace_resource_id}"
terraform import azurerm_subnet_nat_gateway_association.private "${private_subnet_resource_id}"
terraform import azurerm_subnet_nat_gateway_association.public "${public_subnet_resource_id}"

echo "Terraform import commands completed."

chmod +x bash.sh