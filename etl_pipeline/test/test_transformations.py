import pandas as pd
from transform.inventory_metrics import InventoryMetrics

def test_turnover_basic():
    cfg = {"sources":{"csv": {"incremental_column":"modified_date"}}}
    inv = pd.DataFrame([{"product_id":1, "quantity":100, "unit_cost":10},
                        {"product_id":2, "quantity":0, "unit_cost":5}])
    moves = pd.DataFrame([{"product_id":1, "quantity":50, "movement_type":"out", "modified_date":"2025-01-01"}])
    im = InventoryMetrics(cfg)
    out = im.run(inv, moves)
    row = out[out.product_id==1].iloc[0]
    # turnover ratio should be 50/100 = 0.5
    assert round(float(row.turnover_ratio), 3) == 0.5
