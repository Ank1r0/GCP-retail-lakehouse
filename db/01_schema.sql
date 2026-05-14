-- ============================================================
-- retail-lakehouse :: source OLTP schema
-- Simulates an on-prem operational database.
-- Designed to feed: Postgres -> GCS -> BigQuery -> dbt -> Metabase
-- ============================================================

CREATE SCHEMA IF NOT EXISTS retail;
SET search_path TO retail, public;

-- ------------------------------------------------------------
-- MASTER DATA / DIMENSIONS
-- ------------------------------------------------------------

DROP TABLE IF EXISTS retail.payments        CASCADE;
DROP TABLE IF EXISTS retail.order_items     CASCADE;
DROP TABLE IF EXISTS retail.orders          CASCADE;
DROP TABLE IF EXISTS retail.products        CASCADE;
DROP TABLE IF EXISTS retail.suppliers       CASCADE;
DROP TABLE IF EXISTS retail.categories      CASCADE;
DROP TABLE IF EXISTS retail.customers       CASCADE;

CREATE TABLE retail.customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(80)  NOT NULL,
    last_name       VARCHAR(80)  NOT NULL,
    email           VARCHAR(160) NOT NULL UNIQUE,
    phone           VARCHAR(40),
    country         VARCHAR(60),
    city            VARCHAR(80),
    postal_code     VARCHAR(20),
    signup_date     DATE         NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive', 'churned')),
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE retail.categories (
    category_id          SERIAL PRIMARY KEY,
    category_name        VARCHAR(120) NOT NULL,
    parent_category_id   INTEGER REFERENCES retail.categories(category_id),
    created_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE retail.suppliers (
    supplier_id     SERIAL PRIMARY KEY,
    supplier_name   VARCHAR(160) NOT NULL,
    country         VARCHAR(60),
    contact_email   VARCHAR(160),
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE retail.products (
    product_id      SERIAL PRIMARY KEY,
    sku             VARCHAR(40)  NOT NULL UNIQUE,
    product_name    VARCHAR(200) NOT NULL,
    category_id     INTEGER      NOT NULL REFERENCES retail.categories(category_id),
    supplier_id     INTEGER      NOT NULL REFERENCES retail.suppliers(supplier_id),
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    cost_price      NUMERIC(10,2) NOT NULL CHECK (cost_price >= 0),
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- TRANSACTIONS / FACTS
-- ------------------------------------------------------------

CREATE TABLE retail.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER      NOT NULL REFERENCES retail.customers(customer_id),
    order_date      TIMESTAMP    NOT NULL,
    status          VARCHAR(20)  NOT NULL
                    CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled','returned')),
    currency        CHAR(3)      NOT NULL DEFAULT 'USD',
    total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE retail.order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER      NOT NULL REFERENCES retail.orders(order_id) ON DELETE CASCADE,
    product_id      INTEGER      NOT NULL REFERENCES retail.products(product_id),
    quantity        INTEGER      NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    discount_pct    NUMERIC(5,2) NOT NULL DEFAULT 0 CHECK (discount_pct BETWEEN 0 AND 100),
    line_amount     NUMERIC(12,2) NOT NULL
);

CREATE TABLE retail.payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INTEGER      NOT NULL REFERENCES retail.orders(order_id) ON DELETE CASCADE,
    payment_date    TIMESTAMP    NOT NULL,
    amount          NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    method          VARCHAR(20)  NOT NULL
                    CHECK (method IN ('card','bank_transfer','paypal','wallet')),
    status          VARCHAR(20)  NOT NULL
                    CHECK (status IN ('authorized','captured','refunded','failed')),
    reference_id    VARCHAR(64)  NOT NULL UNIQUE
);

-- ------------------------------------------------------------
-- INDEXES (the kind a real OLTP would have)
-- ------------------------------------------------------------

CREATE INDEX idx_customers_status        ON retail.customers(status);
CREATE INDEX idx_customers_signup_date   ON retail.customers(signup_date);

CREATE INDEX idx_products_category       ON retail.products(category_id);
CREATE INDEX idx_products_supplier       ON retail.products(supplier_id);
CREATE INDEX idx_products_active         ON retail.products(active);

CREATE INDEX idx_orders_customer         ON retail.orders(customer_id);
CREATE INDEX idx_orders_order_date       ON retail.orders(order_date);
CREATE INDEX idx_orders_status           ON retail.orders(status);

CREATE INDEX idx_order_items_order       ON retail.order_items(order_id);
CREATE INDEX idx_order_items_product     ON retail.order_items(product_id);

CREATE INDEX idx_payments_order          ON retail.payments(order_id);
CREATE INDEX idx_payments_status         ON retail.payments(status);
CREATE INDEX idx_payments_payment_date   ON retail.payments(payment_date);
