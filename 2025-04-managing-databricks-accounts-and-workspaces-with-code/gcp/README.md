# Overview
This code example is based on this [Databricks Terraform workspace module](https://registry.terraform.io/providers/databricks/databricks/latest/docs/guides/gcp-workspace#creating-a-databricks-workspace) and showcases the first step to migrating an existing workspace's infrastructure to Terraform.


If your workspace has features not included in this template, such as Private Link or Customer-Managed keys, use the [Deployment section in the Databricks Terraform Provider's documentation](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_workspaces#argument-reference)for the resource syntax to include.

# Prerequistes
* Databricks Account Admin role and access to the account console.
* (Optional but recommended) A Databricks service principal with account admin role for authentication 
* [Terraform CLI installed on your machine](https://developer.hashicorp.com/terraform/install)


# Instructions
* Configure Databricks Authentication. If using a service principal, generate an OAuth Token and add to your local .databrickcfg file. See the [documentation for details and alternative authenication methods](https://docs.databricks.com/gcp/en/dev-tools/auth/oauth-m2m?language=Terraform). 
* Fill out the variables in the terraform.tfvars file with values as specified below.
* Run `terraform init` in your terminal.
* Input variable values in bash.sh file and run `sh bash.sh` in your terminal.
* Run `terraform plan` in your terminal. You should receive a "No changes. Your infrastructure matches the configuration." message.
    * Edit main.tf to match discrepanies if you get an execution plan instead.

# Variables to inlcude in .tfvars file

### From the Databricks account console
* databricks_account_id (string) - see upper right corner and click on the circular avatar
* workspace_name (string) - from workspace details
* subnet_region (string) - from network configuration
* network_name (string) - from network configuration
* vpc_id (string) - from network configuration
* subnet_name(string) - from network configuration
* workspace_google_project(string)  - from workspace details
* network_google_project(string)  - from network configuration