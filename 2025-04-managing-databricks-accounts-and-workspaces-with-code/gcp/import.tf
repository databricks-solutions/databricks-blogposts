import {
  to = databricks_mws_networks.this
  id = "${var.databricks_account_id}/${var.network_id}"
}

import {
  to = databricks_mws_workspaces.this
  id = "${var.databricks_account_id}/${var.workspace_id}"
}