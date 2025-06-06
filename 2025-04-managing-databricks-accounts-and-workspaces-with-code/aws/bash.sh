#!/bin/bash

# Define variables
databricks_account_id=""
storage_configuration_id=""
network_id=""
credentials_id=""
workspace_id=""

# Run multiple import commands at once

terraform import databricks_mws_credentials.this "${databricks_account_id}/${credentials_id}"
terraform import databricks_mws_networks.this "${databricks_account_id}/${network_id}"
terraform import databricks_mws_storage_configurations.this "${databricks_account_id}/${storage_configuration_id}"
terraform import databricks_mws_workspaces.this "${databricks_account_id}/${workspace_id}"


echo "Terraform import commands completed."

chmod +x bash.sh