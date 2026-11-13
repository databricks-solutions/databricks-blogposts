# Azure API Management in front of the Lakebase Data API, with up to three APIs:
#   /lakebase      the shared MI's token exchange (../policies/lakebase-data-api-mi-exchange.xml), subscription key
#   /lakebase-rls  app role access (../policies/lakebase-data-api-app-role-rls.xml), Entra token for the gateway app
#   /lakebase-s    the OAuth-secret alternative (../policies/lakebase-data-api.xml), subscription key
# Basic v2 deploys in minutes and has the built-in cache the policies use for Databricks tokens.

resource "azurerm_api_management" "this" {
  name                = "apim-${var.prefix}-${substr(md5(azurerm_resource_group.this.id), 0, 6)}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  publisher_name      = "Lakebase Data API"
  publisher_email     = var.apim_publisher_email
  sku_name            = var.apim_sku

  identity {
    type         = "UserAssigned"
    identity_ids = [for k, mi in azurerm_user_assigned_identity.this : mi.id]
  }

  tags = var.tags
}

locals {
  apim_named_values = merge(
    {
      lakebase-workspace-url = trimsuffix(var.workspace_url, "/")
      lakebase-data-api-url  = trimsuffix(var.data_api_url, "/")
    },
    local.shared_mi ? { lakebase-mi-client-id = azurerm_user_assigned_identity.this["apim"].client_id } : {},
    local.app_role_rls ? merge(
      {
        entra-tenant-id         = var.tenant_id
        lakebase-gateway-app-id = azuread_application.gateway[0].client_id
      },
      { for r in local.app_roles : "lakebase-mi-app-role-${r}-client-id" => azurerm_user_assigned_identity.this["app-role-${r}"].client_id },
    ) : {},
  )
  apim_methods = toset(["GET", "POST", "PATCH", "DELETE"])
  # Subscription keys protect /lakebase and /lakebase-s.
  keyed_apis = var.enable_mi_api || local.secret_api
}

resource "azurerm_api_management_named_value" "this" {
  for_each            = local.apim_named_values
  name                = each.key
  display_name        = each.key
  value               = each.value
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

resource "azurerm_api_management_product" "this" {
  count                 = local.keyed_apis ? 1 : 0
  product_id            = "lakebase"
  display_name          = "Lakebase"
  published             = true
  subscription_required = true
  approval_required     = false
  api_management_name   = azurerm_api_management.this.name
  resource_group_name   = azurerm_resource_group.this.name
}

resource "azurerm_api_management_subscription" "this" {
  count        = local.keyed_apis ? 1 : 0
  display_name = "sample-client"
  product_id   = azurerm_api_management_product.this[0].id
  state        = "active"
  # Policy traces can include header values (tokens), so callers can't request them.
  allow_tracing       = false
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

# /lakebase: the shared managed identity's token exchange.
resource "azurerm_api_management_api" "lakebase" {
  count                 = var.enable_mi_api ? 1 : 0
  name                  = "lakebase-data-api"
  display_name          = "Lakebase Data API"
  path                  = "lakebase"
  protocols             = ["https"]
  revision              = "1"
  service_url           = local.apim_named_values["lakebase-data-api-url"]
  subscription_required = true
  api_management_name   = azurerm_api_management.this.name
  resource_group_name   = azurerm_resource_group.this.name
}

# Wildcard operations: everything after the API path is passed to the Data API (/<schema>/<table>, /<schema>/rpc/<fn>).
resource "azurerm_api_management_api_operation" "lakebase" {
  for_each            = var.enable_mi_api ? local.apim_methods : toset([])
  operation_id        = "${lower(each.key)}-all"
  display_name        = "${each.key} /*"
  method              = each.key
  url_template        = "/*"
  api_name            = azurerm_api_management_api.lakebase[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

resource "azurerm_api_management_api_policy" "lakebase" {
  count               = var.enable_mi_api ? 1 : 0
  api_name            = azurerm_api_management_api.lakebase[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
  xml_content         = file("${path.module}/../policies/lakebase-data-api-mi-exchange.xml")

  depends_on = [azurerm_api_management_named_value.this]
}

resource "azurerm_api_management_product_api" "lakebase" {
  count               = var.enable_mi_api ? 1 : 0
  product_id          = azurerm_api_management_product.this[0].product_id
  api_name            = azurerm_api_management_api.lakebase[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

# /lakebase-rls: app role access. Callers send an Entra token for the gateway app instead of a subscription key,
# and the policy maps its Orders.AppRole<X> app role to that app role's MI.
resource "azurerm_api_management_api" "app_roles" {
  count                 = local.app_role_rls ? 1 : 0
  name                  = "lakebase-app-roles"
  display_name          = "Lakebase app roles"
  path                  = "lakebase-rls"
  protocols             = ["https"]
  revision              = "1"
  service_url           = local.apim_named_values["lakebase-data-api-url"]
  subscription_required = false
  api_management_name   = azurerm_api_management.this.name
  resource_group_name   = azurerm_resource_group.this.name
}

resource "azurerm_api_management_api_operation" "app_roles" {
  for_each            = local.app_role_rls ? local.apim_methods : toset([])
  operation_id        = "${lower(each.key)}-all"
  display_name        = "${each.key} /*"
  method              = each.key
  url_template        = "/*"
  api_name            = azurerm_api_management_api.app_roles[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

resource "azurerm_api_management_api_policy" "app_roles" {
  count               = local.app_role_rls ? 1 : 0
  api_name            = azurerm_api_management_api.app_roles[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
  xml_content         = file("${path.module}/../policies/lakebase-data-api-app-role-rls.xml")

  depends_on = [azurerm_api_management_named_value.this]
}

# /lakebase-s: the OAuth-secret alternative. APIM gets the Databricks token with client_credentials and a stored
# secret instead of the token exchange. In production, keep the secret in Key Vault and reference it from the
# named value.
resource "azurerm_api_management_named_value" "sp_client_id" {
  count               = local.secret_api ? 1 : 0
  name                = "lakebase-sp-client-id"
  display_name        = "lakebase-sp-client-id"
  value               = databricks_service_principal.this["apim"].application_id
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

resource "azurerm_api_management_named_value" "sp_client_secret" {
  count               = local.secret_api ? 1 : 0
  name                = "lakebase-sp-client-secret"
  display_name        = "lakebase-sp-client-secret"
  value               = databricks_service_principal_secret.apim[0].secret
  secret              = true
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

resource "azurerm_api_management_api" "secret" {
  count                 = local.secret_api ? 1 : 0
  name                  = "lakebase-data-api-secret"
  display_name          = "Lakebase Data API (OAuth secret)"
  path                  = "lakebase-s"
  protocols             = ["https"]
  revision              = "1"
  service_url           = local.apim_named_values["lakebase-data-api-url"]
  subscription_required = true
  api_management_name   = azurerm_api_management.this.name
  resource_group_name   = azurerm_resource_group.this.name
}

resource "azurerm_api_management_api_operation" "secret" {
  for_each            = local.secret_api ? local.apim_methods : toset([])
  operation_id        = "${lower(each.key)}-all"
  display_name        = "${each.key} /*"
  method              = each.key
  url_template        = "/*"
  api_name            = azurerm_api_management_api.secret[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}

resource "azurerm_api_management_api_policy" "secret" {
  count               = local.secret_api ? 1 : 0
  api_name            = azurerm_api_management_api.secret[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
  xml_content         = file("${path.module}/../policies/lakebase-data-api.xml")

  depends_on = [
    azurerm_api_management_named_value.this,
    azurerm_api_management_named_value.sp_client_id,
    azurerm_api_management_named_value.sp_client_secret,
  ]
}

resource "azurerm_api_management_product_api" "secret" {
  count               = local.secret_api ? 1 : 0
  product_id          = azurerm_api_management_product.this[0].product_id
  api_name            = azurerm_api_management_api.secret[0].name
  api_management_name = azurerm_api_management.this.name
  resource_group_name = azurerm_resource_group.this.name
}
