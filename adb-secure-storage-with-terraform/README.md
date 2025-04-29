
# Secure Storage Connectivity to both Classic and Serverless compute using terraform in Azure

Data security is the top most priority and critical requirement for any organization in today's digital world. Data storage security refers to the measures and protocols implemented to protect data stored in various storage systems from unauthorized access, breaches, and other security threats. In the context of Databricks, storage security involves several key components and practices. 

Here, we explore one of those which is how to secure the data storage connectivity to both Classic and Serverless Compute using Terraform in Azure. This network perimeter control is a coarse-grained security measure that adds an additional layer of protection.

## Why is it important to consider both Classic and Serverless?

For two reasons, it is important to consider both Classic and Serverless:

1. Use cases where the users would like to take advantage of Serverless for their new workloads.  
2. Seamless migration of the existing workloads from the Classic compute to Serverless with confidence and control.

## What are the options to secure connectivity between Storage and Compute in Azure?

The following two options are explained in this blog with the implementation code snippets using Terraform.

1. Service endpoint provides secure and direct connectivity to Azure service such as Azure Storage over an optimized route over the Azure backbone network. This is a secured approach with no additional costs.  
2. Private endpoint provides a network interface that connects privately and securely to a service such as Azure Storage that's powered by Azure Private Link. This is a more secured approach with the additional costs.

## Option 1 \- Steps to secure connectivity using Service endpoints:

![alt text](./azure-databricks-catalog-storage-sep/drawio/architecture.drawio.svg)

1. Deploy a Databricks workspace with VNET injection with service endpoint to the data storage account in the host subnet as follows. Run the scripts under [azure-databricks-workspace-vnet](./azure-databricks-workspace-vnet) for creating workspace with NET injection.
2. Create [Serverless Network Connectivity Configuration](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/)(NCC) and attach to Workspace. Run the scripts under [azure-databricks-workspace-ncc](./azure-databricks-workspace-ncc) for enabling NCC in the existing workspace.
3. Get the list of subnets from Classic VNET and Serverless NCC configuration and Create a Data Storage Account with network rules to allow connection from the list of subnets from Classic VNET and Serverless NCC configuration only (as retrieved from the previous step). Run the scripts under [azure-databricks-catalog-storage-sep](./azure-databricks-catalog-storage-sep)  
   

## Option 2 \- Steps to secure connectivity using Private endpoints:

![alt text](./azure-databricks-catalog-storage-pl/drawio/architecture.drawio.svg)

1. Create Data Storage Account with network rule (default action : deny and allow access from terraform environment IP)     
2. Deploy a Databricks workspace with [VNET injection](https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/vnet-inject) with a private link subnet for private endpoints.  Run the scripts under [azure-databricks-workspace-vnet](./azure-databricks-workspace-vnet) for creating workspace with NET injection.
3.  Add Storage Private endpoint to allow connection from workspace VNET.  
4.  Create [Serverless Network Connectivity Configuration](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/)(NCC) and attach to Workspace.   Run the scripts under [azure-databricks-workspace-ncc](./azure-databricks-workspace-ncc) for enabling NCC in the existing workspace.
5. Add Storage account Private endpoint rule in NCC to allow connection from Serverless and Finally, approve the newly added NCC private endpoint connection via Azure API.  Run the scripts under [azure-databricks-catalog-storage-pl](./azure-databricks-catalog-storage-pl)  
   
Now, connectivity has been secured for both Classic VNET and Serverless compute to the Storage Account using private endpoints. This can be tested by checking the access to data from the associated workspace and non-associated workspaces. 

## Conclusion

Here, we explored how to secure the data storage connectivity to both Classic and Serverless Compute using Terraform in Azure by reasoning their use cases. Also we explored the available connectivity options using service endpoints and private endpoints with the ready to go implementation terraform code snippets.   


