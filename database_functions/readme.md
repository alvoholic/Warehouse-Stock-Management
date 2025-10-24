functions.sql
PostgreSQL functions for stock movement, stock transfer, reorder check, and stock valuation
Assumptions:
- Uses tables created in test_cases.sql (stock_current, stock_movements, stock_in_batches, products, warehouses)
- movement_type values: 'IN', 'OUT'
- B = Tidak (we allow stock to go negative)
- transfer_stock performs OUT then IN within a transaction

triggers.sql
Audit trigger function and trigger creation for stock_movements and stock_current
