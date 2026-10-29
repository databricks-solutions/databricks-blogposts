-- The shared product catalog (public.products) and two functions callable over the Data API.
-- Run by scripts/10-postgres-setup.sh as the Lakebase project owner, in the database the Data API serves.
-- Grants are in 02-apim-role.sql and 03-app-role-rls.sql. Idempotent: re-running resets the seed rows.

\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS public.products (
    id         serial PRIMARY KEY,
    sku        text UNIQUE NOT NULL,
    name       text NOT NULL,
    category   text NOT NULL,
    price      numeric(10, 2) NOT NULL CHECK (price >= 0),
    stock      int NOT NULL DEFAULT 0 CHECK (stock >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Reset to the seed rows. Upsert rather than TRUNCATE: the app role demo's order lines reference these SKUs
-- (03-app-role-rls.sql), and Postgres won't truncate a referenced table.
DELETE FROM public.products WHERE sku NOT IN ('BEV-001', 'BEV-002', 'SNK-001', 'SNK-002', 'DRY-001');
INSERT INTO public.products (sku, name, category, price, stock) VALUES
    ('BEV-001', 'Sparkling Water 12pk', 'beverages', 6.99, 120),
    ('BEV-002', 'Cold Brew Coffee',     'beverages', 4.49,  35),
    ('SNK-001', 'Sea Salt Chips',       'snacks',    3.29,  80),
    ('SNK-002', 'Trail Mix',            'snacks',    5.99,   8),
    ('DRY-001', 'Basmati Rice 2lb',     'pantry',    4.99,  60)
ON CONFLICT (sku) DO UPDATE
    SET name = EXCLUDED.name, category = EXCLUDED.category, price = EXCLUDED.price,
        stock = EXCLUDED.stock, updated_at = now();

-- Internal to the supplier: no API role is granted on it, so the Data API answers 403 (Postgres 42501).
CREATE TABLE IF NOT EXISTS public.supplier_costs (
    sku       text PRIMARY KEY REFERENCES public.products (sku),
    unit_cost numeric(10, 2) NOT NULL
);

-- RPC 1 (write): change stock by a delta and return the updated row. Callable as POST /rpc/adjust_stock.
-- SECURITY INVOKER (the default) so the caller's own grants on products apply.
CREATE OR REPLACE FUNCTION public.adjust_stock(p_sku text, p_delta int)
RETURNS public.products
LANGUAGE plpgsql AS $$
DECLARE
    result public.products;
BEGIN
    UPDATE public.products
       SET stock = stock + p_delta, updated_at = now()
     WHERE sku = p_sku
    RETURNING * INTO result;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unknown sku %', p_sku USING ERRCODE = 'P0001';  -- P0001 maps to HTTP 400 in the Data API
    END IF;
    RETURN result;
END;
$$;

-- RPC 2 (read): stock value per category. STABLE so the Data API also allows GET /rpc/inventory_summary.
CREATE OR REPLACE FUNCTION public.inventory_summary()
RETURNS TABLE (category text, products bigint, units bigint, stock_value numeric)
LANGUAGE sql STABLE AS $$
    SELECT category, count(*), sum(stock), sum(price * stock)
      FROM public.products
     GROUP BY category
     ORDER BY category;
$$;

-- Functions are executable by PUBLIC by default; 02-apim-role.sql grants them to the shared role only.
REVOKE EXECUTE ON FUNCTION public.adjust_stock(text, int), public.inventory_summary() FROM PUBLIC;
