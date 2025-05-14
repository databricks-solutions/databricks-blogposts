#!/bin/bash

# Define variables
databricks_account_id=""
network_id=""
workspace_id=""


terraform import databricks_mws_networks.this "${databricks_account_id}/${network_id}"
terraform import databricks_mws_workspaces.this "${databricks_account_id}/${workspace_id}"

echo "Terraform import commands completed."

chmod +x bash.sh