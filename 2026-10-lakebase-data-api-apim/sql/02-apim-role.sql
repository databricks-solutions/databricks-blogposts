-- Postgres role for the shared managed identity (the /lakebase and /lakebase-s APIs), with full access to the
-- catalog. Run by scripts/10-postgres-setup.sh as the Lakebase project owner, after 01-catalog.sql.
-- psql variable: apim (the managed identity's client ID, which is its Databricks application ID).
-- Idempotent: safe to re-run.

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS databricks_auth;

-- Roles must be created with databricks_create_role to be grantable to authenticator, the role the Data API
-- connects as before it switches to the caller's role.
SELECT databricks_create_role(:'apim', 'SERVICE_PRINCIPAL')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'apim');

GRANT :"apim" TO authenticator;

GRANT USAGE ON SCHEMA public TO :"apim";
GRANT SELECT, INSERT, UPDATE, DELETE ON public.products TO :"apim";
GRANT USAGE ON SEQUENCE public.products_id_seq TO :"apim";
GRANT EXECUTE ON FUNCTION public.adjust_stock(text, int), public.inventory_summary() TO :"apim";

-- Verify.
SELECT rolname,
       pg_has_role('authenticator', rolname, 'MEMBER')                                     AS authenticator_member,
       has_table_privilege(rolname, 'public.products', 'SELECT,INSERT,UPDATE,DELETE')     AS crud_ok,
       has_function_privilege(rolname, 'public.adjust_stock(text, int)', 'EXECUTE')       AS rpc_ok
FROM pg_roles
WHERE rolname = :'apim';
