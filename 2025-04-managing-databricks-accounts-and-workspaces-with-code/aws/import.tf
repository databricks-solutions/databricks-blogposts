import {
  to = databricks_mws_credentials.this
  id = "${var.databricks_account_id}/${var.credentials_id}"
}

import {
  to = databricks_mws_networks.this
  id = "${var.databricks_account_id}/${var.network_id}"
}

import {
  to = databricks_mws_storage_configurations.this
  id = "${var.databricks_account_id}/${var.storage_configuration_id}"
}

import {
  to = databricks_mws_workspaces.this
  id = "${var.databricks_account_id}/${var.workspace_id}"
}
