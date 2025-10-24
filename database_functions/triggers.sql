-- triggers.sql
-- Audit trigger function and trigger creation for stock_movements and stock_current


CREATE TABLE IF NOT EXISTS audit_stock_changes (
id BIGSERIAL PRIMARY KEY,
table_name TEXT,
operation TEXT,
changed_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
changed_by TEXT DEFAULT NULL,
row_data JSONB
);


CREATE OR REPLACE FUNCTION audit_stock_changes() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
IF (TG_OP = 'DELETE') THEN
INSERT INTO audit_stock_changes(table_name, operation, row_data) VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD)::jsonb);
RETURN OLD;
ELSIF (TG_OP = 'UPDATE') THEN
INSERT INTO audit_stock_changes(table_name, operation, row_data) VALUES (TG_TABLE_NAME, TG_OP, json_build_object('old', row_to_json(OLD), 'new', row_to_json(NEW))::jsonb);
RETURN NEW;
ELSIF (TG_OP = 'INSERT') THEN
INSERT INTO audit_stock_changes(table_name, operation, row_data) VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW)::jsonb);
RETURN NEW;
END IF;
RETURN NULL;
END;
$$;


-- Attach triggers to tables (these tables are created in test_cases.sql)


DROP TRIGGER IF EXISTS trg_audit_stock_movements ON stock_movements;
CREATE TRIGGER trg_audit_stock_movements
AFTER INSERT OR UPDATE OR DELETE ON stock_movements
FOR EACH ROW EXECUTE FUNCTION audit_stock_changes();


DROP TRIGGER IF EXISTS trg_audit_stock_current ON stock_current;
CREATE TRIGGER trg_audit_stock_current
AFTER INSERT OR UPDATE OR DELETE ON stock_current
FOR EACH ROW EXECUTE FUNCTION audit_stock_changes();


DROP TRIGGER IF EXISTS trg_audit_stock_in_batches ON stock_in_batches;
CREATE TRIGGER trg_audit_stock_in_batches
AFTER INSERT OR UPDATE OR DELETE ON stock_in_batches
FOR EACH ROW EXECUTE FUNCTION audit_stock_changes();
