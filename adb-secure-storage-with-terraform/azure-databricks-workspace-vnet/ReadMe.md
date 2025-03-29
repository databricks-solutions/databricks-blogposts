# Network Access

## Deployment Option - VNet Injection + SCC (Secure Cluster Connectivity)

### User to Control Plane
- the workspace URL is publicly accessible

### VNet to Control Plane
- deploy Databricks in the customer's VNet
- the VNet and subnets created in advance
- cluster VMs receive only Private IPs
- traffic is now outbound (the cluster VMs connect to the Control Plane)

#### IP address ranges of Azure Databricks
##### Necessary IP ranges (subnets) for VNet injection
- VNet					            /16 - /24	(e.g. /20)
    -   Host (”public-subnet”)		up to /26	(e.g. /21)
    -   Container (”private-subnet”)	up to /26	(e.g. /21)
    -   Public IP (for non-NPIP)		    Azure IP range
- Notes: 
    -   Private CIDR range size = Public CIDR range size 
    -   Azure reserves 5 addresses for internal purposes, e.g. in a /20 VNet with /21 subnets the largest possible cluster would have one driver and maximal 2043 workers
    -   Even for “No Public IP” workspaces the host network is called “public-subnet” in the Azure Portal, however, it is a private IP range
    -   For network infrastructure nodes e.g. for Private Link separate subnets are needed. Consider to use smaller subnets than half of the VPC side (e.g. cidr - 2) 
    -   container subnet” is also referred to as “private subnet” and  “host subnet” as “public subnet”. Since the terms “private” and “public” are misleading, “container” and “host” are preferred
#### Recommended VNet setup
- Being prepared for network security setups like Private Link
    -   It is possible to configure a network setup for Databricks with subnet size of Vnet CIDR - 1, e.g. VNet = /16, subnets = /17. This maximizes the possible cluster size, however there is no space in the VNet left for configuring more secure setups
    -   It is recommended to use VNet CIDR - 2 for the subnets, e.g. VNet = /16, subnets = /18
    -   Given this configuration, other subnets can be created in the VNet, e.g. to hold private endpoints for a private link configuration

### VNet to Storage
- Option 1: Default  :  Public Endpoints --> storage accounts are accessible from the Internet
- Option 2: Service endpoint provides secure and direct connectivity to Azure services (Storage)

### Communication in an SCC workspace 
- Setup : VNet injection with SCC
- Secure Cluster Connectivity reverses the call direction via the SCC Relay and removes Public IPs from the Compute Plane. 
- Calls from Compute Plane to the Control Plane still use Public IPs
- Control Plane to DBFS is going over Azure Backbone since both are first party Azure services
- Users access Databricks over the Public Internet
- Do NOT use DBFS as a storage layer (e.g. no access control) but rather use storage accounts with Unity Catalog access control for all customer tables, volumes, …
- Note : network_security_group_rules_required =  "AllRules" (for no backend privatelink) or "NoAzureDatabricksRules" (for backend privatelink) 

### Network Architecture
#### Option 1 : Public Endpoints
![alt text](./drawio/architecture-option1.drawio.svg)

#### Option 2 : Service Endpoints
- Following variable to updated witth values as mentioned below:
    - `private_subnet_endpoints = ["Microsoft.Sql", "Microsoft.Storage", "Microsoft.EventHub"]`
- Also, consider disabling the public access to storage account if not already and allow only access from workspace vnet (the module - azure-databricks-workspace-vnet-adls-nw-rules is to do that same)

### To run
- Create tfvars file from the template under tfvars/variables.tfvars.template and assign the variables as per need
- Run Terraform init , plan and apply as a standard deploy flow.
![alt text](./drawio/architecture-option2.drawio.svg)
