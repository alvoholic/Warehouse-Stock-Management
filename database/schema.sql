-- README.md
-- Warehouse Stock Management - Database Design
-- Deliverables included in this single file for convenience:
-- 1) README (this header)
-- 2) ERD placeholder: erd_diagram.png (create using draw.io or dbdiagram.io and add to /database/)
-- 3) schema.sql (below)
--
-- Design choices (short):
-- - Postgres dialect used (BIGSERIAL primary keys, FK constraints, COMMENTs). Replace types if using other RDBMS.
-- - Normalized schema: master tables (products, categories, suppliers, warehouses), transactional tables (purchase_orders, sales_orders, stock_movements), and a current stock materialized table (stock).
-- - stock holds current level per product per warehouse and includes reorder_point / safety_stock.
-- - All changes produce audit entries in audit_log. An example trigger/function is provided to capture inserts/updates/deletes for selected tables.
-- - stock_movements tracks every movement with explicit movement_type (IN/OUT/TRANSFER/ADJUSTMENT). For transfers both from_warehouse_id and to_warehouse_id are used.
-- - Unique and check constraints ensure data quality. Indexes added for common lookup columns.
--
-- File structure suggested when committing to repo:
-- /database/
--   - erd_diagram.png      <-- create from your ERD tool and add
--   - schema.sql           <-- this file
--   - README.md            <-- you may extract the README header into README.md
--

-- =========================
-- SCHEMA (PostgreSQL)
-- =========================

-- Enable uuid-ossp if you prefer UUIDs (optional)
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1) Master: warehouses
CREATE TABLE warehouses (
    warehouse_id BIGSERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE warehouses IS 'Master table for warehouse locations';
COMMENT ON COLUMN warehouses.code IS 'Unique warehouse code (e.g. WH-JKT-01)';

-- 2) Master: categories
CREATE TABLE categories (
    category_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    parent_category_id BIGINT REFERENCES categories(category_id) ON DELETE SET NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE categories IS 'Product categories (hierarchical)';

-- 3) Master: suppliers
CREATE TABLE suppliers (
    supplier_id BIGSERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE,
    name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(255),
    contact_phone VARCHAR(50),
    contact_email VARCHAR(255),
    address TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE suppliers IS 'Suppliers / vendors';

-- 4) Master: products
CREATE TABLE products (
    product_id BIGSERIAL PRIMARY KEY,
    sku VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(500) NOT NULL,
    category_id BIGINT REFERENCES categories(category_id) ON DELETE SET NULL,
    supplier_id BIGINT REFERENCES suppliers(supplier_id) ON DELETE SET NULL,
    purchase_price NUMERIC(14,4) CHECK (purchase_price >= 0),
    retail_price NUMERIC(14,4) CHECK (retail_price >= 0),
    unit VARCHAR(50),
    weight_kg NUMERIC(10,3),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE products IS 'Product master. sku must be unique.';

-- 5) Current stock per warehouse (materialized single-row entry per product+warehouse)
CREATE TABLE stock (
    stock_id BIGSERIAL PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
    warehouse_id BIGINT NOT NULL REFERENCES warehouses(warehouse_id) ON DELETE CASCADE,
    quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    reserved_quantity NUMERIC(18,4) NOT NULL DEFAULT 0,
    reorder_point NUMERIC(18,4) DEFAULT 0,
    safety_stock NUMERIC(18,4) DEFAULT 0,
    last_movement_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE (product_id, warehouse_id)
);
COMMENT ON TABLE stock IS 'Current stock level per product per warehouse. quantity excludes reserved_quantity.';

-- Indexes to support lookups
CREATE INDEX idx_stock_product ON stock(product_id);
CREATE INDEX idx_stock_warehouse ON stock(warehouse_id);

-- 6) Stock movements (history)
CREATE TABLE stock_movements (
    movement_id BIGSERIAL PRIMARY KEY,
    movement_reference VARCHAR(100), -- optional reference (PO number, SO number, manual adj id)
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    from_warehouse_id BIGINT REFERENCES warehouses(warehouse_id) ON DELETE SET NULL,
    to_warehouse_id BIGINT REFERENCES warehouses(warehouse_id) ON DELETE SET NULL,
    quantity NUMERIC(18,4) NOT NULL CHECK (quantity > 0),
    movement_type VARCHAR(20) NOT NULL CHECK (movement_type IN ('IN','OUT','TRANSFER','ADJUSTMENT')),
    reason TEXT, -- optional note
    related_table VARCHAR(50), -- e.g. 'purchase_orders', 'sales_orders'
    related_id BIGINT, -- id in related table if applicable
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE stock_movements IS 'History of stock movements. Use movement_type to classify.';

-- Indexes for common queries
CREATE INDEX idx_sm_product ON stock_movements(product_id);
CREATE INDEX idx_sm_from_wh ON stock_movements(from_warehouse_id);
CREATE INDEX idx_sm_to_wh ON stock_movements(to_warehouse_id);
CREATE INDEX idx_sm_created_at ON stock_movements(created_at);

-- 7) Purchase orders (PO)
CREATE TABLE purchase_orders (
    purchase_order_id BIGSERIAL PRIMARY KEY,
    po_number VARCHAR(100) NOT NULL UNIQUE,
    supplier_id BIGINT REFERENCES suppliers(supplier_id) ON DELETE SET NULL,
    warehouse_id BIGINT REFERENCES warehouses(warehouse_id) ON DELETE SET NULL, -- receiving warehouse
    status VARCHAR(30) NOT NULL CHECK (status IN ('DRAFT','PENDING','RECEIVED','CANCELLED')) DEFAULT 'PENDING',
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expected_date DATE,
    total_amount NUMERIC(18,4) DEFAULT 0,
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE purchase_orders IS 'Purchase orders to suppliers';

CREATE INDEX idx_po_supplier ON purchase_orders(supplier_id);
CREATE INDEX idx_po_status ON purchase_orders(status);

-- Purchase order details
CREATE TABLE purchase_order_details (
    pod_id BIGSERIAL PRIMARY KEY,
    purchase_order_id BIGINT NOT NULL REFERENCES purchase_orders(purchase_order_id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity NUMERIC(18,4) NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(18,4) CHECK (unit_price >= 0),
    received_quantity NUMERIC(18,4) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE purchase_order_details IS 'Line items for PO';

CREATE INDEX idx_pod_po ON purchase_order_details(purchase_order_id);

-- 8) Sales orders (SO)
CREATE TABLE sales_orders (
    sales_order_id BIGSERIAL PRIMARY KEY,
    so_number VARCHAR(100) NOT NULL UNIQUE,
    customer_name VARCHAR(255),
    warehouse_id BIGINT REFERENCES warehouses(warehouse_id) ON DELETE SET NULL, -- shipping warehouse
    status VARCHAR(30) NOT NULL CHECK (status IN ('DRAFT','CONFIRMED','SHIPPED','CANCELLED')) DEFAULT 'CONFIRMED',
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    total_amount NUMERIC(18,4) DEFAULT 0,
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE sales_orders IS 'Customer sales orders';

CREATE INDEX idx_so_status ON sales_orders(status);
CREATE INDEX idx_so_warehouse ON sales_orders(warehouse_id);

-- Sales order details
CREATE TABLE sales_order_details (
    sod_id BIGSERIAL PRIMARY KEY,
    sales_order_id BIGINT NOT NULL REFERENCES sales_orders(sales_order_id) ON DELETE CASCADE,
    product_id BIGINT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    quantity NUMERIC(18,4) NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(18,4) CHECK (unit_price >= 0),
    shipped_quantity NUMERIC(18,4) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
COMMENT ON TABLE sales_order_details IS 'Line items for SO';

CREATE INDEX idx_sod_so ON sales_order_details(sales_order_id);

-- 9) Audit log (generic)
CREATE TABLE audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE')),
    record_id BIGINT,
    changed_by VARCHAR(255),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    diff JSONB -- optional: store before/after or changed fields
);
COMMENT ON TABLE audit_log IS 'Generic audit trail for important tables';

CREATE INDEX idx_audit_table ON audit_log(table_name);
CREATE INDEX idx_audit_changed_at ON audit_log(changed_at);

-- =========================
-- Example trigger to maintain stock on stock_movements insert
-- NOTE: In production carefully handle concurrency and failed transactions; consider using stored procedures or application-level logic.
-- This example updates the `stock` table when a stock_movement is inserted.
-- For TRANSFER: decrease from_warehouse, increase to_warehouse in a single transaction.
-- =========================

-- Function to adjust stock based on movement
CREATE OR REPLACE FUNCTION fn_handle_stock_movement()
RETURNS TRIGGER AS $$
DECLARE
    v_stock_from_id BIGINT;
    v_stock_to_id BIGINT;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        IF (NEW.movement_type = 'IN') THEN
            -- increase stock in to_warehouse_id
            IF NEW.to_warehouse_id IS NULL THEN
                RAISE EXCEPTION 'to_warehouse_id required for IN movement';
            END IF;
            INSERT INTO stock(product_id, warehouse_id, quantity, last_movement_at)
            VALUES (NEW.product_id, NEW.to_warehouse_id, NEW.quantity, now())
            ON CONFLICT (product_id, warehouse_id) DO UPDATE
            SET quantity = stock.quantity + EXCLUDED.quantity,
                last_movement_at = now(),
                updated_at = now();

        ELSIF (NEW.movement_type = 'OUT') THEN
            IF NEW.from_warehouse_id IS NULL THEN
                RAISE EXCEPTION 'from_warehouse_id required for OUT movement';
            END IF;
            -- reduce stock
            UPDATE stock
            SET quantity = quantity - NEW.quantity,
                last_movement_at = now(),
                updated_at = now()
            WHERE product_id = NEW.product_id AND warehouse_id = NEW.from_warehouse_id;
            -- Optionally enforce non-negative stock
            -- You may want to check rows affected and raise error if insufficient stock.

        ELSIF (NEW.movement_type = 'TRANSFER') THEN
            IF NEW.from_warehouse_id IS NULL OR NEW.to_warehouse_id IS NULL THEN
                RAISE EXCEPTION 'from_warehouse_id and to_warehouse_id required for TRANSFER';
            END IF;
            -- decrease from
            UPDATE stock
            SET quantity = quantity - NEW.quantity,
                last_movement_at = now(),
                updated_at = now()
            WHERE product_id = NEW.product_id AND warehouse_id = NEW.from_warehouse_id;
            -- increase to
            INSERT INTO stock(product_id, warehouse_id, quantity, last_movement_at)
            VALUES (NEW.product_id, NEW.to_warehouse_id, NEW.quantity, now())
            ON CONFLICT (product_id, warehouse_id) DO UPDATE
            SET quantity = stock.quantity + EXCLUDED.quantity,
                last_movement_at = now(),
                updated_at = now();

        ELSIF (NEW.movement_type = 'ADJUSTMENT') THEN
            -- adjustments can be to either warehouse (use to_warehouse_id if positive adjustment)
            IF NEW.to_warehouse_id IS NOT NULL THEN
                INSERT INTO stock(product_id, warehouse_id, quantity, last_movement_at)
                VALUES (NEW.product_id, NEW.to_warehouse_id, NEW.quantity, now())
                ON CONFLICT (product_id, warehouse_id) DO UPDATE
                SET quantity = stock.quantity + EXCLUDED.quantity,
                    last_movement_at = now(),
                    updated_at = now();
            ELSIF NEW.from_warehouse_id IS NOT NULL THEN
                UPDATE stock
                SET quantity = quantity - NEW.quantity,
                    last_movement_at = now(),
                    updated_at = now()
                WHERE product_id = NEW.product_id AND warehouse_id = NEW.from_warehouse_id;
            ELSE
                RAISE EXCEPTION 'Either to_warehouse_id or from_warehouse_id required for ADJUSTMENT';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on insert
CREATE TRIGGER trg_after_insert_stock_movements
AFTER INSERT ON stock_movements
FOR EACH ROW EXECUTE FUNCTION fn_handle_stock_movement();

-- =========================
-- Example audit trigger (generic) capturing INSERT/UPDATE/DELETE for selected tables
-- Attach triggers to tables you want audited (e.g., stock, stock_movements, purchase_orders)
-- =========================

CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
DECLARE
    v_old JSONB;
    v_new JSONB;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_new := to_jsonb(NEW);
        INSERT INTO audit_log(table_name, operation, record_id, changed_by, diff)
        VALUES (TG_TABLE_NAME, 'INSERT', COALESCE(NEW.id, NEW.*::TEXT::BIGINT), current_user, jsonb_build_object('new', v_new));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        v_old := to_jsonb(OLD);
        v_new := to_jsonb(NEW);
        INSERT INTO audit_log(table_name, operation, record_id, changed_by, diff)
        VALUES (TG_TABLE_NAME, 'UPDATE', COALESCE(NEW.id, NEW.*::TEXT::BIGINT), current_user, jsonb_build_object('old', v_old, 'new', v_new));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        v_old := to_jsonb(OLD);
        INSERT INTO audit_log(table_name, operation, record_id, changed_by, diff)
        VALUES (TG_TABLE_NAME, 'DELETE', COALESCE(OLD.id, OLD.*::TEXT::BIGINT), current_user, jsonb_build_object('old', v_old));
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Note: The above fn_audit_trigger uses a coarse approach to record entire row as JSONB.
-- In production you may instead store only changes, the user performing the change, and avoid storing large binary data.

-- Example of attaching audit trigger to tables
-- For demonstration, attach to stock_movements and purchase_orders
CREATE TRIGGER trg_audit_stock_movements
AFTER INSERT OR UPDATE OR DELETE ON stock_movements
FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

CREATE TRIGGER trg_audit_purchase_orders
AFTER INSERT OR UPDATE OR DELETE ON purchase_orders
FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

-- =========================
-- Additional safety / best-practice suggestions (document in README.md):
-- 1) Consider moving business logic for stock adjustments into stored procedures so that stock movements and stock table updates are atomic.
-- 2) For high throughput, consider using optimistic concurrency (version column) or explicit row-level locks when updating stock.
-- 3) Consider materialized views for aggregated analytics (e.g., stock aging, fast-moving SKUs) refreshed on schedule.
-- 4) Add monitoring/alerting when stock quantity <= reorder_point.
-- 5) Consider partitioning stock_movements by date if volume is large.
-- =========================

-- End of schema.sql
