
-- 1) DROP TABLES (SAFE RERUN)
DROP TABLE IF EXISTS audit_stock_changes CASCADE;
DROP TABLE IF EXISTS stock_in_batches CASCADE;
DROP TABLE IF EXISTS stock_movements CASCADE;
DROP TABLE IF EXISTS stock_current CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS warehouses CASCADE;

-- 2) CREATE MASTER TABLES
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku TEXT,
    name TEXT,
    reorder_point INT DEFAULT 0,
    safety_stock INT DEFAULT 0
);

CREATE TABLE warehouses (
    id SERIAL PRIMARY KEY,
    name TEXT
);

-- 3) CREATE STOCK TABLES
CREATE TABLE stock_current (
    product_id INT REFERENCES products(id),
    warehouse_id INT REFERENCES warehouses(id),
    quantity INT DEFAULT 0,
    PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE stock_movements (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id),
    warehouse_id INT REFERENCES warehouses(id),
    movement_type VARCHAR(20),
    quantity INT,
    unit_cost NUMERIC NULL,
    reference_type VARCHAR(50),
    reference_id INT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE stock_in_batches (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id),
    warehouse_id INT REFERENCES warehouses(id),
    movement_id INT REFERENCES stock_movements(id),
    received_qty INT,
    remaining_qty INT,
    unit_cost NUMERIC,
    received_at TIMESTAMP DEFAULT now(),
    consumed_at TIMESTAMP NULL
);

-- 4) CREATE AUDIT TABLE
CREATE TABLE audit_stock_changes (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT,
    operation TEXT,
    changed_at TIMESTAMP DEFAULT now(),
    changed_by TEXT DEFAULT NULL,
    row_data JSONB
);

-- 5) RUN FUNCTIONS & TRIGGERS (assumes functions.sql and triggers.sql were executed first)


-- 6) INSERT MASTER DATA
INSERT INTO products (sku, name, reorder_point, safety_stock) VALUES
('SKU-001', 'Product A', 10, 5);

INSERT INTO warehouses (name) VALUES ('WH-1'), ('WH-2');


-- 7) TEST CASE EXECUTION
-- IN #1 : 100 pcs @5.00
SELECT record_stock_movement(1, 1, 'IN', 100, 5.00, 'PO', 1001, 'Initial supply for Product A');

-- IN #2 : 50 pcs @6.00
SELECT record_stock_movement(1, 1, 'IN', 50, 6.00, 'PO', 1002, 'Second batch');

-- OUT #1 : 60 pcs
SELECT record_stock_movement(1, 1, 'OUT', 60, NULL, 'SO', 2001, 'Sales Order #2001');

-- TRANSFER : 20 pcs WH-1 → WH-2
SELECT transfer_stock(1, 1, 2, 20, 'Replenishment to WH-2');


-- 8) TEST REORDER
SELECT * FROM check_reorder_points();


-- 9) TEST VALUATION
SELECT * FROM calculate_stock_value('FIFO');
SELECT * FROM calculate_stock_value('LIFO');
SELECT * FROM calculate_stock_value('AVG');


-- 10) TEST AUDIT LOGS
SELECT * FROM audit_stock_changes ORDER BY changed_at DESC;


-- 11) FINAL VERIFICATION SELECTS
SELECT '=== FINAL stock_current ===' AS label;
SELECT * FROM stock_current ORDER BY warehouse_id;

SELECT '=== FINAL stock_in_batches ===' AS label;
SELECT * FROM stock_in_batches ORDER BY id;

SELECT '=== FINAL stock_movements ===' AS label;
SELECT * FROM stock_movements ORDER BY id;
