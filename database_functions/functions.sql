CREATE OR REPLACE FUNCTION record_stock_movement(
END;
$$;




CREATE OR REPLACE FUNCTION check_reorder_points(
p_warehouse_id INT DEFAULT NULL
) RETURNS TABLE(
product_id INT,
product_name TEXT,
warehouse_id INT,
current_qty INT,
reorder_point INT,
safety_stock INT
) LANGUAGE plpgsql AS $$
BEGIN
RETURN QUERY
SELECT p.id, p.name, sc.warehouse_id, COALESCE(sc.quantity,0) as current_qty, p.reorder_point, p.safety_stock
FROM products p
LEFT JOIN stock_current sc ON sc.product_id = p.id
WHERE (p_warehouse_id IS NULL OR sc.warehouse_id = p_warehouse_id)
AND COALESCE(sc.quantity,0) <= COALESCE(p.reorder_point,0) + COALESCE(p.safety_stock,0);
END;
$$;




CREATE OR REPLACE FUNCTION calculate_stock_value(p_method VARCHAR(10))
RETURNS TABLE(product_id INT, warehouse_id INT, total_qty INT, total_value NUMERIC) LANGUAGE plpgsql AS $$
DECLARE
r RECORD;
v_value NUMERIC;
BEGIN
-- We'll compute total qty per product+warehouse from stock_current
FOR r IN SELECT product_id, warehouse_id, quantity FROM stock_current
LOOP
IF p_method = 'AVG' THEN
-- average cost = weighted average of batches
SELECT COALESCE(SUM((received_qty - remaining_qty) * unit_cost) + SUM(remaining_qty * unit_cost),0) INTO v_value
FROM stock_in_batches sib
WHERE sib.product_id = r.product_id AND sib.warehouse_id = r.warehouse_id;
-- If no batch cost data, fallback to 0
RETURN NEXT (r.product_id, r.warehouse_id, r.quantity, COALESCE(v_value,0));
ELSIF p_method IN ('FIFO','LIFO') THEN
-- compute by consuming batches in order
v_value := 0;
IF r.quantity = 0 THEN
RETURN NEXT (r.product_id, r.warehouse_id, r.quantity, 0);
END IF;


IF p_method = 'FIFO' THEN
FOR b IN SELECT remaining_qty, unit_cost FROM stock_in_batches WHERE product_id = r.product_id AND warehouse_id = r.warehouse_id ORDER BY received_at ASC
LOOP
IF r.quantity <= 0 THEN EXIT; END IF;
IF b.remaining_qty <= r.quantity THEN
v_value := v_value + (b.remaining_qty * COALESCE(b.unit_cost,0));
r.quantity := r.quantity - b.remaining_qty;
ELSE
v_value := v_value + (r.quantity * COALESCE(b.unit_cost,0));
r.quantity := 0;
END IF;
END LOOP;
ELSE -- LIFO
FOR b IN SELECT remaining_qty, unit_cost FROM stock_in_batches WHERE product_id = r.product_id AND warehouse_id = r.warehouse_id ORDER BY received_at DESC
LOOP
IF r.quantity <= 0 THEN EXIT; END IF;
IF b.remaining_qty <= r.quantity THEN
v_value := v_value + (b.remaining_qty * COALESCE(b.unit_cost,0));
r.quantity := r.quantity - b.remaining_qty;
ELSE
v_value := v_value + (r.quantity * COALESCE(b.unit_cost,0));
r.quantity := 0;
END IF;
END LOOP;
END IF;


RETURN NEXT (r.product_id, r.warehouse_id, (SELECT quantity FROM stock_current sc WHERE sc.product_id = r.product_id AND sc.warehouse_id = r.warehouse_id), COALESCE(v_value,0));
ELSE
RAISE EXCEPTION 'Unknown method %', p_method;
END IF;
END LOOP;
END;
$$;
