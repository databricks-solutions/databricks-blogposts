-- Row-level security keyed on app roles, for the /lakebase-rls API.
--
-- Scenario: a supplier publishes one product catalog (public.products, from 01-catalog.sql), and the clients
-- of each Entra app role on the gateway (Orders.AppRoleA, Orders.AppRoleB) place and see only their own orders.
-- APIM calls Lakebase as one managed identity per app role. Each app role has a Postgres group role
-- (orders_app_role_a, orders_app_role_b) whose only member is that managed identity's role, and every row records
-- the group role it belongs to in owner_role.
--
-- Schemas:
--   public   shared catalog. Clients may read it; grants control it, not RLS.
--   app      rows under RLS: orders, order_lines, order_totals (view). Exposed by the Data API.
--   private  owner_role(), the caller's group role. Not exposed.
--
-- Run as the project owner, on the branch and in the database the Data API serves, after 01-catalog.sql
-- (order_lines references public.products). Replace <APP_ROLE_A_MI_CLIENT_ID> and <APP_ROLE_B_MI_CLIENT_ID> with
-- the managed identities' client IDs (scripts/10-postgres-setup.sh does this).
-- Re-running drops and recreates app and private (roles, memberships and grants to authenticator are kept).

CREATE EXTENSION IF NOT EXISTS databricks_auth;

-- Managed identity roles must be created with databricks_create_role (not the UI) to be grantable to authenticator.
SELECT databricks_create_role('<APP_ROLE_A_MI_CLIENT_ID>', 'SERVICE_PRINCIPAL')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '<APP_ROLE_A_MI_CLIENT_ID>');
SELECT databricks_create_role('<APP_ROLE_B_MI_CLIENT_ID>', 'SERVICE_PRINCIPAL')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '<APP_ROLE_B_MI_CLIENT_ID>');
GRANT "<APP_ROLE_A_MI_CLIENT_ID>" TO authenticator;
GRANT "<APP_ROLE_B_MI_CLIENT_ID>" TO authenticator;

-- One group role per app role. They never log in and aren't granted to authenticator: they only own rows and
-- hold the grants, which their member inherits.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orders_app_role_a') THEN
        CREATE ROLE orders_app_role_a NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'orders_app_role_b') THEN
        CREATE ROLE orders_app_role_b NOLOGIN;
    END IF;
END $$;
GRANT orders_app_role_a TO "<APP_ROLE_A_MI_CLIENT_ID>";
GRANT orders_app_role_b TO "<APP_ROLE_B_MI_CLIENT_ID>";

DROP SCHEMA IF EXISTS app CASCADE;
DROP SCHEMA IF EXISTS private CASCADE;
CREATE SCHEMA app;
CREATE SCHEMA private;

-- The caller's group role: the one orders_app_role_* role current_user belongs to. NULL if it belongs to several,
-- which matches no rows and fails every write. (A role in none holds no grants, so it never gets this far.)
-- pg_auth_members is readable by everyone, so the function needs no SECURITY DEFINER.
CREATE FUNCTION private.owner_role() RETURNS text
    LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp
    AS $$
        SELECT CASE WHEN count(*) = 1 THEN min(g.rolname) END
          FROM pg_auth_members m
          JOIN pg_roles g ON g.oid = m.roleid
          JOIN pg_roles u ON u.oid = m.member
         WHERE u.rolname = current_user
           AND g.rolname LIKE 'orders\_app\_role\_%'
    $$;
REVOKE ALL ON FUNCTION private.owner_role() FROM PUBLIC;

-- Every table under RLS carries owner_role, defaulted to the caller's group role so clients don't send it.
-- Keys lead with owner_role: uniqueness is per group role, and the primary key doubles as its index.
CREATE TABLE app.orders (
    owner_role text NOT NULL DEFAULT private.owner_role(),
    id         bigint GENERATED ALWAYS AS IDENTITY,
    status     text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'submitted', 'shipped')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_role, id)
);

-- Foreign-key checks ignore RLS. The composite key (owner_role, order_id), with owner_role fixed to the caller's
-- group role by the default and the policy, means a line can only point at the caller's own order.
CREATE TABLE app.order_lines (
    owner_role text   NOT NULL DEFAULT private.owner_role(),
    id         bigint GENERATED ALWAYS AS IDENTITY,
    order_id   bigint NOT NULL,
    sku        text   NOT NULL REFERENCES public.products (sku),
    qty        int    NOT NULL CHECK (qty > 0),
    PRIMARY KEY (owner_role, id),
    FOREIGN KEY (owner_role, order_id) REFERENCES app.orders (owner_role, id) ON DELETE CASCADE
);
CREATE INDEX ON app.order_lines (owner_role, order_id);

-- security_invoker: the view runs with the caller's rights, so the RLS policies below apply to it. Without it the
-- view runs as its owner, and the Lakebase project owner has BYPASSRLS: every client would see every row.
CREATE VIEW app.order_totals WITH (security_invoker = true) AS
SELECT o.owner_role,
       o.id                  AS order_id,
       o.status,
       count(l.id)           AS lines,
       coalesce(sum(l.qty * p.price), 0) AS total
  FROM app.orders o
  LEFT JOIN app.order_lines l ON l.owner_role = o.owner_role AND l.order_id = o.id
  LEFT JOIN public.products p ON p.sku = l.sku
 GROUP BY o.owner_role, o.id, o.status;

-- Seed data. The project owner has BYPASSRLS, so RLS never applies to it (here or in psql later).
-- One row per INSERT so the fresh identity hands out order IDs 1-4 in this order.
INSERT INTO app.orders (owner_role, status) VALUES ('orders_app_role_a', 'submitted');  -- 1
INSERT INTO app.orders (owner_role, status) VALUES ('orders_app_role_a', 'open');       -- 2
INSERT INTO app.orders (owner_role, status) VALUES ('orders_app_role_b', 'shipped');    -- 3
INSERT INTO app.orders (owner_role, status) VALUES ('orders_app_role_b', 'open');       -- 4
INSERT INTO app.order_lines (owner_role, order_id, sku, qty) VALUES
    ('orders_app_role_a', 1, 'BEV-002', 12),
    ('orders_app_role_a', 1, 'SNK-002', 6),
    ('orders_app_role_a', 2, 'BEV-001', 4),
    ('orders_app_role_b', 3, 'DRY-001', 20),
    ('orders_app_role_b', 3, 'SNK-001', 30),
    ('orders_app_role_b', 4, 'BEV-001', 10);

-- USING filters what a caller can read, update and delete; WITH CHECK rejects writes for another group role.
-- (SELECT ...) makes Postgres look the group role up once per query instead of once per row.
-- FORCE applies the policies to a table owner without BYPASSRLS, for example if ownership moves to another role.
ALTER TABLE app.orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.orders FORCE ROW LEVEL SECURITY;
CREATE POLICY owner_role_isolation ON app.orders
    USING (owner_role = (SELECT private.owner_role()))
    WITH CHECK (owner_role = (SELECT private.owner_role()));

ALTER TABLE app.order_lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.order_lines FORCE ROW LEVEL SECURITY;
CREATE POLICY owner_role_isolation ON app.order_lines
    USING (owner_role = (SELECT private.owner_role()))
    WITH CHECK (owner_role = (SELECT private.owner_role()));

-- Grants go to the group roles, and their members inherit them. RLS decides which rows each one sees.
-- The catalog is read-only for clients.
GRANT USAGE ON SCHEMA app, private, public TO orders_app_role_a, orders_app_role_b;
GRANT EXECUTE ON FUNCTION private.owner_role() TO orders_app_role_a, orders_app_role_b;
GRANT SELECT, INSERT, UPDATE, DELETE ON app.orders, app.order_lines TO orders_app_role_a, orders_app_role_b;
GRANT SELECT ON app.order_totals, public.products TO orders_app_role_a, orders_app_role_b;

NOTIFY pgrst, 'reload schema';

-- Verify: one row per managed identity, all true.
SELECT r.group_role,
       r.mi_role,
       pg_has_role(r.mi_role, r.group_role, 'MEMBER')                       AS group_member,
       pg_has_role('authenticator', r.mi_role, 'MEMBER')                    AS authenticator_member,
       has_table_privilege(r.mi_role, 'app.order_lines', 'INSERT')         AS can_write_lines,
       has_table_privilege(r.mi_role, 'public.products', 'SELECT')         AS can_read_catalog,
       NOT has_table_privilege(r.mi_role, 'public.products', 'UPDATE')     AS catalog_read_only
  FROM (VALUES ('orders_app_role_a', '<APP_ROLE_A_MI_CLIENT_ID>'),
               ('orders_app_role_b', '<APP_ROLE_B_MI_CLIENT_ID>')) AS r (group_role, mi_role);
