# Secure Storage Connectivity and Serverless compute using service endpoint
- To ensure secure connections between your Databricks serverless compute resources and your Azure Storage account, implement Network Connectivity Center (NCC) subnets. This configuration enhances security by providing a private and isolated network environment for data transfer and communication. By leveraging NCC subnets, you can effectively restrict access to your Azure Storage account, mitigating the risk of unauthorized data exposure and potential security breaches.

# Initial Steps
- Run the scripts under azure-databricks-uc-metastore for Metastore creation.
- Run the scripts under azure-databricks-workspace-vnet for creating workspace with NET injection.
- Run the scripts under azure-databricks-workspace-ncc for enabling NCC in the existing workspace.

# to block public access to the following azure services i.e. Storage account
- block public access to catalog storage account and allow only access from the specific virtual network subnets.

# To run
- Create tfvars file from the template under tfvars/variables.tfvars.template and assign the variables as per need
- Run Terraform init , plan and apply as a standard deploy flow.

### Network Architecture
![alt text](./drawio/architecture.drawio.svg)