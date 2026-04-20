# Overview
This code example is based on this [Databricks Terraform workspace module](https://github.com/databricks/terraform-databricks-examples/blob/main/modules/aws-databricks-workspace/main.tf) and showcases the first step to migrating an existing workspace's infrastructure to Terraform.


If your workspace has features not included in this template, such as Private Link or Network Connnectivity Configurations, use the [Deployment section in the Databricks Terraform Provider's documentation](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/mws_workspaces#argument-reference)for the resource syntax to include.

# Prerequistes
* Databricks Account Admin role and access to the account console.
* (Optional but recommended) A Databricks service principal with account admin role for authentication 
* [Terraform CLI installed on your machine](https://developer.hashicorp.com/terraform/install)


# Instructions
* Configure Databricks Authentication. If using a service principal, generate an OAuth Token and add to your local .databrickcfg file. See the [documentation for details and alternative authenication methods](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m?language=Terraform). 
* Fill out the variables in the terraform.tfvars file with values as specified below.
* Run `terraform init` in your terminal.
* Run `terraform plan` in your terminal. You should receive a "Plan: (# of resources added to main.tf) to import, 0 to add, 0 to change, 0 to destroy." message.

* Run `terraform apply` in your terminal to import the current state of your workspace configuration. 

# Variables to inlcude in .tfvars file

### From the Databricks account console
* databricks_account_id (string) - see upper right corner and click on the circular avatar.
* workspace_name (string) - from workspace details page.
* region (string) - from the region section in the workspace details page.
* network_name (string) - from network configuration page. Click on the link in the network section of the workspace details page.
* network_id (string) - from network configuration page URL. Click on the link in the network section of the workspace details page. This is the uuid after network-configurations/
* vpc_id (string) - from network configuration page. Click on the link in the network section of the workspace details page.
* vpc_private_subnets list(string) - from network configuration page. Click on the link in the network section of the workspace details page.
* security_group_ids list(string) - from network configuration page. Click on the link in the network section of the workspace details page.
* cross_account_role_arn (string) - from credential configuration page. Click on the link in the credentials section of the workspace details page.
* credentials_name (string) - from credential configuration page. Click on the link in the credentials section of the workspace details page.
* credentials_id (string) - from credential configuration page URL. Click on the link in the credentials section of the workspace details page. This is the uuid after credential-configurations/
* root_storage_bucket (string) - bucket name from storage configuration page. Click on the link in the storage section of the workspace details page. 
* storage_configuration_name (string) - name from storage configuration page. Click on the link in the storage section of the workspace details page. 
* storage_configuration_id (string) - from storage configuration page URL. Click on the link in the storage section of the workspace details page. This is the uuid after storage-configurations/

### From the workspace
* workspace_id (string) - 16 digit number from workspace URL after o=
