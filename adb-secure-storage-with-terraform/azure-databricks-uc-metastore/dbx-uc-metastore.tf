# Create a Databricks Unity Catalog Metastore resource
resource "databricks_metastore" "this" {
  # Use the Databricks provider configured for account-level operations
  provider      = databricks.accounts
  
  # Name of the metastore to be created
  name          = var.databricks_metastore
  
  # Force destroy the metastore when it is no longer needed
  force_destroy = true
  
  # Specify the Azure region where the metastore will be located
  region        = var.azure_region
}
