-- =============================================================
-- Retail CDC Pipeline — Postgres OLTP Schema
-- =============================================================

-- -------------------------------------------------------------
-- Step 1: Enable logical replication
-- Allows Debezium to read the WAL and capture CDC events
-- -------------------------------------------------------------
ALTER SYSTEM SET wal_level = logical;

-- -------------------------------------------------------------
-- Step 2: Create customers table
-- Loaded once at startup — 10 real + 490 synthetic
-- Simulator reads from this to get valid customer_ids
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id     SERIAL PRIMARY KEY,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    city            VARCHAR(100),
    country         VARCHAR(100),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- Step 3: Create products table
-- Loaded once at startup — 20 real + 80 synthetic
-- Simulator reads from this to get valid product_ids
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id      SERIAL PRIMARY KEY,
    name            VARCHAR(200),
    category        VARCHAR(100),
    price           NUMERIC(10, 2),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- Step 4: Create orders table
-- CDC target — Debezium captures every INSERT and UPDATE
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- Step 5: Create order_items table
-- CDC target — Debezium captures every INSERT
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id),
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- Step 6: Auto-update updated_at on orders
-- Every status change stamps updated_at automatically
-- dbt incremental models filter on this column
-- -------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- -------------------------------------------------------------
-- Step 7: Create publication for Debezium
-- Only watch orders + order_items — not customers/products
-- Customers and products are static, no CDC needed
-- -------------------------------------------------------------
CREATE PUBLICATION retail_publication FOR TABLE orders, order_items;