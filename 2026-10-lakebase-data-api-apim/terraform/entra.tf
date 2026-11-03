# Entra app registrations for app role access: the gateway app (the API, with app roles Orders.AppRoleA and
# Orders.AppRoleB) and one sample client app per app role.

data "azuread_client_config" "current" {}

locals {
  # Make the person running Terraform an owner. Owning the gateway app is what allows assigning its app roles
  # (the "admin consent" step) without an Entra admin role, and guests aren't made owners automatically.
  entra_owners = [data.azuread_client_config.current.object_id]
}

# Stable app role IDs. Changing the ID of a role that has assignments would break them.
resource "random_uuid" "app_role" {
  for_each = local.app_roles
}

resource "azuread_application" "gateway" {
  count            = local.app_role_rls ? 1 : 0
  display_name     = "${var.entra_name_prefix}-gateway"
  sign_in_audience = "AzureADMyOrg"
  owners           = local.entra_owners

  dynamic "app_role" {
    for_each = local.app_roles
    content {
      id                   = random_uuid.app_role[app_role.value].result
      allowed_member_types = ["Application"]
      display_name         = "Orders app role ${upper(app_role.value)}"
      description          = "Read and write the orders of Postgres group role orders_app_role_${app_role.value}"
      value                = "Orders.AppRole${upper(app_role.value)}"
      enabled              = true
    }
  }

  # Set by azuread_application_identifier_uri below (it needs the app's client ID).
  lifecycle {
    ignore_changes = [identifier_uris]
  }
}

# Application ID URI api://<client-id>: the audience of client tokens, checked by validate-azure-ad-token in APIM.
resource "azuread_application_identifier_uri" "gateway" {
  count          = local.app_role_rls ? 1 : 0
  application_id = azuread_application.gateway[0].id
  identifier_uri = "api://${azuread_application.gateway[0].client_id}"
}

# Assignment required: Entra won't issue gateway tokens to apps that hold no app role.
resource "azuread_service_principal" "gateway" {
  count                        = local.app_role_rls ? 1 : 0
  client_id                    = azuread_application.gateway[0].client_id
  app_role_assignment_required = true
  owners                       = local.entra_owners
}

# Sample callers. In production each client brings its own app registration or managed identity
# and only gets an app role assigned.
resource "azuread_application" "client" {
  for_each         = local.app_roles
  display_name     = "${var.entra_name_prefix}-client-${each.key}"
  sign_in_audience = "AzureADMyOrg"
  owners           = local.entra_owners

  # Shows the role under API permissions; the assignment below is what grants it.
  required_resource_access {
    resource_app_id = azuread_application.gateway[0].client_id

    resource_access {
      id   = random_uuid.app_role[each.key].result
      type = "Role"
    }
  }
}

resource "azuread_service_principal" "client" {
  for_each  = local.app_roles
  client_id = azuread_application.client[each.key].client_id
  owners    = local.entra_owners
}

# The "grant admin consent" step: assigns Orders.AppRole<X> to client <x>.
resource "azuread_app_role_assignment" "client" {
  for_each            = local.app_roles
  app_role_id         = random_uuid.app_role[each.key].result
  principal_object_id = azuread_service_principal.client[each.key].object_id
  resource_object_id  = azuread_service_principal.gateway[0].object_id
}

# For testing only: lets curl get a client token (client_credentials). The secret is kept in Terraform state, so
# keep the state private. The expiry is fixed when the secret is created.
resource "time_offset" "client_secret" {
  offset_days = var.client_secret_lifetime_days
}

resource "azuread_application_password" "client" {
  for_each       = local.app_roles
  application_id = azuread_application.client[each.key].id
  display_name   = "sample client"
  end_date       = time_offset.client_secret.rfc3339
}
