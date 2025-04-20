// register VPC
resource "databricks_mws_networks" "this" {
  account_id   = var.databricks_account_id
  network_name = var.network_name
  gcp_network_info {
    network_project_id    = var.network_google_project
    vpc_id                = var.vpc_id
    subnet_id             = var.subnet_name
    subnet_region         = var.subnet_region
  }
}

// create workspace in given VPC
resource "databricks_mws_workspaces" "this" {
  account_id     = var.databricks_account_id
  workspace_name = var.workspace_name
  location       = var.subnet_region
  cloud_resource_container {
    gcp {
      project_id = var.workspace_google_project
    }
  }

  network_id = databricks_mws_networks.this.network_id

}