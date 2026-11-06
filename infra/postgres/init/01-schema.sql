-- Source database for the Deltaflow demo: a small shop.
-- Runs once, when the Postgres data volume is first created
-- (mounted at /docker-entrypoint-initdb.d). To re-run: docker compose down -v

CREATE TABLE customers (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT        NOT NULL UNIQUE,
    full_name   TEXT        NOT NULL,
    city        TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    id           BIGSERIAL PRIMARY KEY,
    sku          TEXT        NOT NULL UNIQUE,
    name         TEXT        NOT NULL,
    category     TEXT        NOT NULL,
    price_cents  INTEGER     NOT NULL CHECK (price_cents >= 0),
    stock        INTEGER     NOT NULL CHECK (stock >= 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT      NOT NULL REFERENCES customers (id),
    product_id   BIGINT      NOT NULL REFERENCES products (id),
    quantity     INTEGER     NOT NULL CHECK (quantity > 0),
    status       TEXT        NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    total_cents  INTEGER     NOT NULL CHECK (total_cents >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX orders_customer_idx ON orders (customer_id);
CREATE INDEX orders_status_idx ON orders (status);

-- Keep updated_at honest on every UPDATE.
CREATE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customers_touch BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER products_touch BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER orders_touch BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- With the default replica identity, an UPDATE/DELETE event only carries the primary key
-- in "before". FULL makes Postgres log the whole old row, so every event has a complete
-- before-image (needed for history, audits and correct deletes downstream). See ADR-0007.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

-- Explicit publication: the connector may only see these tables (least privilege).
CREATE PUBLICATION deltaflow_pub FOR TABLE customers, products, orders;
