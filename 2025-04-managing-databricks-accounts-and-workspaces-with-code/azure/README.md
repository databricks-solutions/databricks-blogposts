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
* Input variable values in bash.sh file and run `sh bash.sh` in your terminal.
* Run `terraform plan` in your terminal. You should receive a "No changes. Your infrastructure matches the configuration." message.
    * Edit main.tf to match discrepanies if you get an execution plan instead.



# Variables to inlcude in .tfvars file

### From the Azure portal
* workspace_name (string) - from workspace overview
* resource_group_name (string) -  from workspace overview
* location (string) -  from workspace overview
* managed_resource_group_name (string) -  from workspace overview
* sku_type (string) - from workspace overview (premium or standard)
* virtual_network_id (string) - resource ID from virtual network JSON view in the upper right corner
* private_subnet_name (string) - from virtual network subnet setting page
* public_subnet_name (string) - from virtual network subnet setting page
* public_subnet_id (string) - subnet ID from virtual network subnet setting details page
* private_subnet_id (string) - subnet ID from virtual network subnet setting details page
* nat_gateway_id (string) - resource ID from NAT gateway JSON view in the upper right corner

