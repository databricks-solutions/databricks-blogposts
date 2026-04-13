# Overview
This code example is based on this [Databricks Terraform workspace module](https://github.com/databricks/terraform-databricks-examples/tree/main/modules/adb-lakehouse) and showcases the first step to migrating an existing workspace's infrastructure to Terraform.


If your workspace has features not included in this template, such as Private Link or Customer-Managed Keys, use the [Deployment section in the Databricks Terraform Provider's documentation](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/databricks_workspace)for the resource syntax to include.

# Prerequistes
* Permissions to view the details of the Databricks workspace in the Azure Portal.
* Workspace admin role.
* (Optional but recommended) A Databricks service principal with workspace admin role for authentication.
* [Terraform CLI installed on your machine](https://developer.hashicorp.com/terraform/install).


# Instructions
* Configure [Azure authentication](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs#authenticating-to-azure).
* Review the main.tf file and include any additional configuration.
* Fill out the variables in the terraform.tfvars file with values as specified below. Include new values and variables if added in the previous step.
* Run `terraform init` in your terminal.
* Run `terraform plan` in your terminal. You should receive a "Plan: (# of resources added to main.tf) to import, 0 to add, 0 to change, 0 to destroy." message.
* Run `terraform apply` in your terminal to import the current state of your workspace configuration. 



# Variables to inlcude in .tfvars file

### From the Azure portal
* workspace_name (string) - from workspace overview
* workspace_id (string) - resource ID from workspace overview JSON view in the upper right corner. 
* resource_group_name (string) -  from workspace overview
* location (string) -  from workspace overview
* managed_resource_group_name (string) -  from workspace overview
* sku_type (string) - pricing tier from workspace overview (premium or standard)
* virtual_network_id (string) - resource ID from virtual network JSON view in the upper right corner. Click on the virtual network link in workspace overview page to find the virtual network attached to the workspace.
* private_subnet_name (string) - from workspace overview page. Click on see more if not visible
* public_subnet_name (string) - from workspace overview page. Click on see more if not visible
* public_subnet_id (string) - subnet ID from virtual network subnet setting details page. Click on the subnet link in the workspace overview page and then click on the subnet name to access the subnet setting details page.
* private_subnet_id (string) - subnet ID from virtual network subnet setting details page. Click on the subnet link in the workspace overview page and then click on the subnet name to access the subnet setting details page.
* nat_gateway_id (string) - resource ID from NAT gateway JSON view in the upper right corner. Find the NAT gateway on the subnet settings page in the security section.

