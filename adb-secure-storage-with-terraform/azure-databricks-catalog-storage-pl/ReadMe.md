# Secure Storage Connectivity and Serverless compute using private link
- An NCC (Network Connectivity Configuration) private link to an Azure Storage account establishes a secure and private connection between your Databricks serverless compute resources and your Azure Storage account. This private link utilizes Azure Private Endpoints, which are network interfaces that allow you to privately and securely access your Azure Storage account from within your virtual network.

- By using an NCC private link, you can ensure that data transferred between your Databricks serverless compute and your Azure Storage account remains within the Microsoft Azure backbone network, thereby avoiding the public internet. This enhances the security of your data and helps you comply with data privacy regulations. Additionally, private links can improve the performance of your data transfers by reducing network latency.

# Initial Steps
- Run the scripts under azure-databricks-uc-metastore for Metastore creation.
- Run the scripts under azure-databricks-workspace-vnet for creating workspace with NET injection.
- Run the scripts under azure-databricks-workspace-ncc for enabling NCC in the existing workspace.

# to block public access to the following azure services i.e. Storage account
- block public access to catalog storage account and allow only access from the specific virtual network subnets and NCC.

# To run
- Create tfvars file from the template under tfvars/variables.tfvars.template and assign the variables as per need
- Run Terraform init , plan and apply as a standard deploy flow.

### Network Architecture
![alt text](./drawio/architecture.drawio.svg)