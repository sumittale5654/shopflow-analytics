-- ============================================================
-- ShopFlow Analytics — Source Database Schema
-- Simulates an e-commerce OLTP (Online Transaction Processing) DB
-- ============================================================

-- Raw / staging schema (ingested data lands here first)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- ── Customers ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    phone           VARCHAR(30),
    city            VARCHAR(100),
    country         VARCHAR(100),
    signup_date     DATE NOT NULL,
    customer_tier   VARCHAR(20) DEFAULT 'standard', -- standard, silver, gold
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ── Products ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    unit_price      NUMERIC(10,2) NOT NULL,
    cost_price      NUMERIC(10,2) NOT NULL,
    stock_quantity  INTEGER DEFAULT 0,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ── Orders ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES public.customers(customer_id),
    order_date      TIMESTAMP NOT NULL,
    status          VARCHAR(30) NOT NULL, -- pending, processing, shipped, delivered, cancelled
    payment_method  VARCHAR(50),          -- credit_card, debit_card, paypal, upi
    shipping_city   VARCHAR(100),
    shipping_country VARCHAR(100),
    discount_pct    NUMERIC(5,2) DEFAULT 0,
    total_amount    NUMERIC(12,2),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ── Order items ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.order_items (
    item_id         SERIAL PRIMARY KEY,
    order_id        INTEGER REFERENCES public.orders(order_id),
    product_id      INTEGER REFERENCES public.products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    discount_pct    NUMERIC(5,2) DEFAULT 0,
    line_total      NUMERIC(12,2),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Clickstream events ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.clickstream (
    event_id        SERIAL PRIMARY KEY,
    session_id      VARCHAR(64) NOT NULL,
    customer_id     INTEGER,  -- NULL if anonymous
    product_id      INTEGER,
    event_type      VARCHAR(50) NOT NULL, -- page_view, add_to_cart, remove_from_cart, purchase
    page_url        VARCHAR(500),
    referrer        VARCHAR(500),
    device_type     VARCHAR(30),  -- desktop, mobile, tablet
    browser         VARCHAR(50),
    event_timestamp TIMESTAMP NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── Pipeline watermark (tracks last extracted timestamp) ─────
CREATE TABLE IF NOT EXISTS public.pipeline_watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_extracted  TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00'
);

INSERT INTO public.pipeline_watermark (table_name) VALUES
    ('customers'), ('products'), ('orders'), ('order_items'), ('clickstream')
ON CONFLICT DO NOTHING;

-- ── Indexes for faster incremental extraction ─────────────────
CREATE INDEX IF NOT EXISTS idx_orders_updated ON public.orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_customers_updated ON public.customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_products_updated ON public.products(updated_at);
CREATE INDEX IF NOT EXISTS idx_clickstream_ts ON public.clickstream(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON public.order_items(order_id);
