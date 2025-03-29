# Serverless Compute Plane - Networking Options
## Network Connectivity Configuration (NCC) 
- Databricks introduces a Network Connectivity Configuration (NCC) construct that assists in centrally managing network connectivity across one or multiple workspaces’ Serverless Compute Planes.
  - With NCC, shared NAT gateways provide stable CIDR ranges for allowlisting on resource firewalls. Note: This feature is in Private Preview for SQL, GC, MS 
  - With NCC, shared Service Endpoints provide stable subnets for allowlisting on resource firewalls for e.g. Azure storage accounts, sql server and any resource that supports service endpoints
  - For connectivity instances where Private Link is available, you can create a dedicated Private Endpoint in the NCC. Note: GA for DB and Private Preview for GC. Currently supports Storage account and Azure SQL
- Note: Availability might also depend on the cloud region
- It is recommended to
  - leverage direct connectivity to Azure services over an optimized route with Service Endpoints or Stable IP (where service endpoint is not supported), and only use Private Link for storage when absolutely required (to avoid additional cost)
  - share an NCC among workspaces within the same business unit, environment and those sharing the same region connectivity properties. 

## Serverless Egress Control
-  With Serverless Egress Control you can restrict access to the internet while allowing access via Unity Catalog Connections or Private Link. Further, this feature blocks direct access to Cloud Storage to ensure that all data access occurs via Unity Catalog controlled paths. 
  - Outbound access from the serverless network is denied by default. However, 
    - access to Databricks-specific systems, e.g. the workspace root container and system tables is allowed.
    - FQDNs can be configured as exceptions to allow access to defined services
  - Access via private link is governed by a network policy.
  - Direct access to storage accounts from user code in UDFs and Notebooks (REPL) is denied. Access is only allowed to Unity Catalog securables

  - Note: This feature is in Private Preview, see the preview docs for more details

### To run
- Create tfvars file from the template under tfvars/variables.tfvars.template and assign the variables as per need
- Run Terraform init , plan and apply as a standard deploy flow.

### Network Architecture
![alt text](./drawio/architecture.drawio.svg)
