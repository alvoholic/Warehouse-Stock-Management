
import os
import random
import math
import datetime
import uuid
import yaml
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm
from faker import Faker

# Load config.yaml (if absent, use defaults)
base_dir = Path(__file__).resolve().parent
cfg_path = base_dir / "config.yaml"
if cfg_path.exists():
    with open(cfg_path, "r") as f:
        config = yaml.safe_load(f)
else:
    # defaults (same as used earlier)
    config = {
        "start_date": (datetime.date.today() - datetime.timedelta(days=365*2)).isoformat(),
        "end_date": datetime.date.today().isoformat(),
        "warehouses": 10,
        "products": 5000,
        "categories": 50,
        "suppliers": 200,
        "stock_current_records": 100000,
        "stock_movements": 500000,
        "purchase_orders": 100000,
        "sales_orders": 200000,
        "output_mode": "csv",
        "price_min": 50000,
        "price_max": 5000000,
        "max_stock_per_record": 500,
        "data_quality_pct": 0.05,
        "random_seed": 42
    }

random.seed(config.get("random_seed", 42))
np.random.seed(config.get("random_seed", 42))
fake = Faker()
out_dir = base_dir / "output"
out_dir.mkdir(parents=True, exist_ok=True)

def rand_date(start, end):
    start_u = int(start.strftime("%s"))
    end_u = int(end.strftime("%s"))
    return datetime.datetime.fromtimestamp(random.randint(start_u, end_u)).date()

start_date = datetime.datetime.fromisoformat(config["start_date"]).date()
end_date = datetime.datetime.fromisoformat(config["end_date"]).date()

# 1) Warehouses
warehouses = [{"warehouse_id": i, "warehouse_code": f"WH{i:02d}", "name": f"Warehouse {i}"} for i in range(1, config["warehouses"]+1)]
pd.DataFrame(warehouses).to_csv(out_dir / "warehouses.csv", index=False)

# 2) Categories
categories = [{"category_id": i, "category_name": f"Category {i}", "season_peak_month": random.randint(1,12)} for i in range(1, config["categories"]+1)]
categories_df = pd.DataFrame(categories)
categories_df.to_csv(out_dir / "categories.csv", index=False)

# 3) Suppliers
suppliers = [{"supplier_id": i, "supplier_name": fake.company(), "contact": fake.phone_number()} for i in range(1, config["suppliers"]+1)]
pd.DataFrame(suppliers).to_csv(out_dir / "suppliers.csv", index=False)

# 4) Products with movement_weight for 80/20
P = config["products"]
top20 = int(P * 0.2)
movement_weight = np.ones(P)
movement_weight[:top20] = 5.0
np.random.shuffle(movement_weight)

products = []
for pid in range(1, P+1):
    cat = random.randint(1, config["categories"])
    supplier = random.randint(1, config["suppliers"])
    # price lognormal for long tail then clamp
    price = int(np.round(np.random.lognormal(mean=10, sigma=0.8)))
    price = max(config["price_min"], min(price, config["price_max"]))
    products.append({
        "product_id": pid,
        "sku": f"SKU-{pid:06d}",
        "product_name": f"Product {pid}",
        "category_id": cat,
        "supplier_id": supplier,
        "price": price,
        "movement_weight": float(movement_weight[pid-1])
    })
products_df = pd.DataFrame(products)
products_df.to_csv(out_dir / "products.csv", index=False)

# 5) Stock current
stock_records = []
for i in tqdm(range(config["stock_current_records"]), desc="stock_current"):
    wid = random.randint(1, config["warehouses"])
    pid = random.randint(1, P)
    qty = random.randint(0, config["max_stock_per_record"])
    reorder_point = random.choice([0,5,10,20,50])
    stock_records.append({"stock_id": i+1, "warehouse_id": wid, "product_id": pid, "quantity": qty, "reorder_point": reorder_point, "last_updated": rand_date(start_date,end_date).isoformat()})
pd.DataFrame(stock_records).to_csv(out_dir / "stock_current.csv", index=False)

# 6) Stock movements
movement_types = ['IN','OUT','TRANSFER','ADJUSTMENT','RETURN']
movements = []
movement_count = config["stock_movements"]
weights = products_df["movement_weight"].values.astype(float)
weights = weights / weights.sum()

for i in tqdm(range(movement_count), desc="stock_movements"):
    pid = int(np.random.choice(products_df["product_id"], p=weights))
    prod_row = products_df.iloc[pid-1]
    wid = random.randint(1, config["warehouses"])
    cat_peak = categories_df.loc[categories_df['category_id']==prod_row['category_id'],'season_peak_month'].iloc[0]
    month = cat_peak if random.random() < 0.25 else random.randint(1,12)
    year = random.randint(start_date.year, end_date.year)
    day = random.randint(1,28)
    mov_date = datetime.date(year, month, day)
    mtype = random.choices(movement_types, weights=[0.4,0.45,0.05,0.05,0.05])[0]
    qty = int(abs(int(np.random.poisson(lam=5) * (1 if mtype!='IN' else 1.5) + 1)))
    if mtype == 'OUT' and random.random() < 0.02:
        qty *= random.randint(5,20)
    ref = str(uuid.uuid4())[:8]
    movements.append({"movement_id": i+1, "movement_date": mov_date.isoformat(), "warehouse_id": wid, "product_id": pid, "movement_type": mtype, "quantity": qty, "reference": ref})

mov_df = pd.DataFrame(movements)
# Insert data quality issues (~data_quality_pct)
dq_n = int(len(mov_df) * config["data_quality_pct"])
if dq_n > 0:
    idxs = np.random.choice(mov_df.index, size=dq_n, replace=False)
    for j, idx in enumerate(idxs):
        choice = j % 4
        if choice == 0:
            mov_df.at[idx, "quantity"] = None
        elif choice == 1:
            mov_df.at[idx, "quantity"] = -abs(int(mov_df.at[idx, "quantity"] or 1))
        elif choice == 2:
            mov_df.at[idx, "reference"] = mov_df.at[idx, "reference"][:4]
        else:
            mov_df.at[idx, "reference"] = mov_df.at[max(0, idx-1), "reference"]
mov_df.to_csv(out_dir / "stock_movements.csv", index=False)

# 7) Purchase Orders & details
po_count = config["purchase_orders"]
po_headers = []
po_details = []
detail_id = 0
for i in tqdm(range(po_count), desc="purchase_orders"):
    po_id = i+1
    date = rand_date(start_date, end_date)
    supplier_id = random.randint(1, config["suppliers"])
    warehouse_id = random.randint(1, config["warehouses"])
    lines = random.randint(1,5)
    po_headers.append({"po_id": po_id, "po_date": date.isoformat(), "supplier_id": supplier_id, "warehouse_id": warehouse_id, "total_lines": lines})
    for _ in range(lines):
        detail_id += 1
        prod = int(np.random.choice(products_df["product_id"], p=weights))
        qty = random.randint(1,100)
        unit_price = int(products_df.at[prod-1, "price"] * (0.9 + random.random()*0.3))
        po_details.append({"po_detail_id": detail_id, "po_id": po_id, "product_id": prod, "quantity": qty, "unit_price": unit_price})
pd.DataFrame(po_headers).to_csv(out_dir / "purchase_orders.csv", index=False)
pd.DataFrame(po_details).to_csv(out_dir / "purchase_order_details.csv", index=False)

# 8) Sales Orders & details
so_count = config["sales_orders"]
so_headers = []
so_details = []
detail_id = 0
for i in tqdm(range(so_count), desc="sales_orders"):
    so_id = i+1
    date = rand_date(start_date, end_date)
    warehouse_id = random.randint(1, config["warehouses"])
    customer = fake.name()
    lines = random.randint(1,5)
    so_headers.append({"so_id": so_id, "so_date": date.isoformat(), "warehouse_id": warehouse_id, "customer": customer, "total_lines": lines})
    for _ in range(lines):
        detail_id += 1
        prod = int(np.random.choice(products_df["product_id"], p=weights))
        qty = random.randint(1,20)
        unit_price = int(products_df.at[prod-1, "price"] * (0.9 + random.random()*0.3))
        so_details.append({"so_detail_id": detail_id, "so_id": so_id, "product_id": prod, "quantity": qty, "unit_price": unit_price})
pd.DataFrame(so_headers).to_csv(out_dir / "sales_orders.csv", index=False)
pd.DataFrame(so_details).to_csv(out_dir / "sales_order_details.csv", index=False)

# 9) Validation summary
summary = {
    "warehouses": len(warehouses),
    "categories": len(categories),
    "suppliers": len(suppliers),
    "products": len(products),
    "stock_current": len(stock_records),
    "stock_movements": len(movements),
    "purchase_orders": len(po_headers),
    "purchase_order_details": len(po_details),
    "sales_orders": len(so_headers),
    "sales_order_details": len(so_details),
    "data_quality_issues_pct": config.get("data_quality_pct", 0.05)
}
with open(out_dir / "data_validation_summary.yaml", "w") as f:
    yaml.dump(summary, f)

with open(out_dir / "README.txt", "w") as f:
    f.write("Files generated. See data_validation_summary.yaml for counts.\\n")

print("Done. Files in:", out_dir)
