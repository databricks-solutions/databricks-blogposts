# The identities APIM uses toward Lakebase. Each user-assigned managed identity (MI) is registered in the Databricks
# account as an Entra-managed service principal (application_id = MI client ID), assigned to the workspace, and
# given a federation policy that lets it exchange its Entra tokens for Databricks tokens at /oidc/v1/token.
# In Postgres, the MI's client ID is its role name (sql/).

resource "azurerm_resource_group" "this" {
  name     = "rg-${var.prefix}"
  location = var.location
  tags     = var.tags
}

locals {
  # Microsoft's fixed ID for api://AzureADTokenExchange, the same in every Entra directory. Managed identity tokens
  # requested for api://AzureADTokenExchange carry it as their aud claim, so the policies must accept it.
  token_exchange_app_id = "fb60f99c-7a34-4190-8149-302f77469936"

  # Every MI and what it is for: "apim" serves /lakebase and /lakebase-s, "app-role-<x>" serves app role Orders.AppRole<X> on /lakebase-rls.
  mis = merge(
    local.shared_mi ? { apim = "id-${var.prefix}-apim" } : {},
    { for r in local.app_roles : "app-role-${r}" => "id-${var.prefix}-app-role-${r}" },
  )
}

resource "azurerm_user_assigned_identity" "this" {
  for_each            = local.mis
  name                = each.value
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags
}

resource "databricks_service_principal" "this" {
  for_each       = local.mis
  provider       = databricks.account
  application_id = azurerm_user_assigned_identity.this[each.key].client_id
  display_name   = azurerm_user_assigned_identity.this[each.key].name
}

# Workspace access. No entitlements are needed: the Data API governs access with Postgres roles.
resource "databricks_mws_permission_assignment" "this" {
  for_each     = local.mis
  provider     = databricks.account
  workspace_id = var.workspace_id
  principal_id = databricks_service_principal.this[each.key].id
  permissions  = ["USER"]
}

resource "databricks_service_principal_federation_policy" "this" {
  for_each             = local.mis
  provider             = databricks.account
  service_principal_id = tonumber(databricks_service_principal.this[each.key].id)
  policy_id            = "entra-apim-mi"
  description          = "Entra managed identity tokens for ${azurerm_user_assigned_identity.this[each.key].name}"

  oidc_policy = {
    issuer    = "https://login.microsoftonline.com/${var.tenant_id}/v2.0"
    audiences = [local.token_exchange_app_id, "api://AzureADTokenExchange"]
    subject   = azurerm_user_assigned_identity.this[each.key].principal_id
  }
}

# The OAuth-secret alternative: a Databricks OAuth secret on the shared MI's service principal, used with
# client_credentials at /oidc/v1/token. It reaches Postgres as the same role as the /lakebase API.
resource "databricks_service_principal_secret" "apim" {
  count                = local.secret_api ? 1 : 0
  provider             = databricks.account
  service_principal_id = databricks_service_principal.this["apim"].id
  lifetime             = "${var.databricks_secret_lifetime_days * 86400}s"
}
