import os
import random
import yaml
import uuid
import csv
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# =========================
# Load Config
# =========================
def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# =========================
# Utility Functions
# =========================
def random_date(start, end):
    """Return random datetime between two dates"""
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

def weighted_choice(items, weights):
    """Pick item using weighted distribution"""
    return random.choices(items, weights=weights, k=1)[0]

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Seasonal multiplier (misal: high demand akhir tahun)
def seasonal_multiplier(date):
    month = date.month
    if month in [11, 12]:  # high peak e-commerce season
        return 1.8
    elif month in [6, 7]:  # holiday season
        return 1.4
    else:
        return 1.0

# ------------------------
# Generation Functions (masters, stock, movements, PO, SO)
# ------------------------

def gen_warehouses(n):
    rows = []
    for i in range(1, n+1):
        rows.append({
            "warehouse_id": i,
            "code": f"WH-{i:02d}",
            "name": f"Warehouse {i}",
            "address": fake.address().replace("\n", ", "),
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
    return pd.DataFrame(rows)


def gen_categories(n):
    rows = []
    for i in range(1, n+1):
        parent = i-1 if (i % 10 != 1) else None
        rows.append({
            "category_id": i,
            "name": f"Category {i}",
            "parent_category_id": parent,
            "description": fake.sentence(nb_words=6),
            "created_at": datetime.now().isoformat()
        })
    return pd.DataFrame(rows)


def gen_suppliers(n):
    rows = []
    for i in range(1, n+1):
        rows.append({
            "supplier_id": i,
            "code": f"SUP-{i:04d}",
            "name": fake.company(),
            "contact_name": fake.name(),
            "contact_phone": fake.phone_number(),
            "contact_email": fake.company_email(),
            "address": fake.address().replace("\n", ", "),
            "is_active": True,
            "created_at": datetime.now().isoformat()
        })
    return pd.DataFrame(rows)


def gen_products(n_products, n_categories, n_suppliers, sku_prefix="SKU"):
    rows = []
    for i in range(1, n_products+1):
        cat = random.randint(1, n_categories)
        sup = random.randint(1, n_suppliers)
        price = round(random.uniform(5.0, 500.0), 2)
        rows.append({
            "product_id": i,
            "sku": f"{sku_prefix}-{i:05d}",
            "name": f"Product {i}",
            "category_id": cat,
            "supplier_id": sup,
            "purchase_price": round(price * 0.7, 2),
            "retail_price": price,
            "unit": "pcs",
            "weight_kg": round(random.uniform(0.01, 20.0), 3),
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
    return pd.DataFrame(rows)


def gen_initial_stock(products_df, warehouses_df, required_stock_records, config):
    np.random.seed(config.get("random_seed", 42))
    total_products = len(products_df)
    total_warehouses = len(warehouses_df)
    combos = set()
    prod_ids = products_df["product_id"].tolist()
    top_k = max(1, int(total_products * 0.2))
    top_products = prod_ids[:top_k]
    other_products = prod_ids[top_k:]
    rows = []
    pbar = tqdm(total=required_stock_records, desc="Generating stock records")
    while len(rows) < required_stock_records:
        if random.random() < 0.8:
            p = random.choice(top_products)
        else:
            p = random.choice(other_products) if other_products else random.choice(top_products)
        w = random.randint(1, total_warehouses)
        key = (p, w)
        if key in combos:
            continue
        combos.add(key)
        qty = max(0, int(abs(np.random.normal(loc=200, scale=150))))
        reorder = int(max(0, qty * random.uniform(0.05, 0.25)))
        safety = int(max(0, qty * random.uniform(0.02, 0.1)))
        rows.append({
            "stock_id": len(rows) + 1,
            "product_id": p,
            "warehouse_id": w,
            "quantity": qty,
            "reserved_quantity": 0,
            "reorder_point": reorder,
            "safety_stock": safety,
            "last_movement_at": None,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        pbar.update(1)
    pbar.close()
    return pd.DataFrame(rows)


def month_weight_for_category(cat_idx, date):
    # seasonal multiplier per category index (deterministic)
    month = date.month
    if (cat_idx % 5) == 0:
        peak = (cat_idx % 12) + 1
        diff = min(abs(month - peak), 12 - abs(month - peak))
        mult = 1.1 + 0.4 * math.cos(diff * math.pi / 6.0)
        return max(0.3, mult)
    return 1.0


def gen_stock_movements(products_df, warehouses_df, start_date, end_date, n_movements, config):
    np.random.seed(config.get("random_seed", 42) + 1)
    prod_ids = products_df["product_id"].tolist()
    total_products = len(prod_ids)
    top_k = max(1, int(total_products * 0.2))
    top_products = prod_ids[:top_k]
    other_products = prod_ids[top_k:]
    movements = []
    days = max(1, (end_date - start_date).days)
    movement_types = config["movement_types"]
    weights = config.get("movement_type_weights", [0.35, 0.4, 0.15, 0.05, 0.05])
    pbar = tqdm(total=n_movements, desc="Generating stock_movements")
    for i in range(1, n_movements + 1):
        # skew dates: more movements in recent period
        day_offset = int(abs(np.random.normal(loc=days * 0.7, scale=days * 0.5))) % days
        moved_at = start_date + timedelta(days=day_offset, seconds=random.randint(0, 86400))
        if random.random() < 0.8:
            pid = random.choice(top_products)
        else:
            pid = random.choice(other_products) if other_products else random.choice(top_products)
        cat_idx = int(products_df.loc[products_df["product_id"] == pid, "category_id"].values[0])
        mult = month_weight_for_category(cat_idx, moved_at)
        base_qty = max(1, int(abs(np.random.poisson(lam=5) * mult)))
        mtype = random.choices(movement_types, weights=weights, k=1)[0]
        from_wh = to_wh = None
        if mtype == "IN":
            to_wh = random.randint(1, len(warehouses_df))
        elif mtype == "OUT":
            from_wh = random.randint(1, len(warehouses_df))
        elif mtype == "TRANSFER":
            from_wh = random.randint(1, len(warehouses_df))
            to_wh = random.randint(1, len(warehouses_df))
            if to_wh == from_wh:
                to_wh = (to_wh % len(warehouses_df)) + 1
        elif mtype == "ADJUSTMENT":
            if random.random() < 0.5:
                to_wh = random.randint(1, len(warehouses_df))
            else:
                from_wh = random.randint(1, len(warehouses_df))
        elif mtype == "RETURN":
            to_wh = random.randint(1, len(warehouses_df))
        movements.append({
            "movement_id": i,
            "movement_reference": None,
            "product_id": int(pid),
            "from_warehouse_id": from_wh,
            "to_warehouse_id": to_wh,
            "quantity": int(base_qty),
            "movement_type": mtype,
            "reason": None,
            "related_table": None,
            "related_id": None,
            "created_by": fake.user_name(),
            "created_at": moved_at.isoformat()
        })
        pbar.update(1)
    pbar.close()

    # inject data quality issues (5%)
    n_issues = max(1, int(len(movements) * config.get("data_issue_rate", 0.05)))
    for _ in range(n_issues):
        idx = random.randrange(len(movements))
        issue_type = random.choice(["null_field", "neg_qty", "bad_type"])
        if issue_type == "null_field":
            movements[idx]["movement_type"] = None
        elif issue_type == "neg_qty":
            movements[idx]["quantity"] = -abs(movements[idx]["quantity"])
        elif issue_type == "bad_type":
            movements[idx]["movement_type"] = "UNKNOWN"

    return pd.DataFrame(movements)


def gen_purchase_and_details(products_df, suppliers_df, warehouses_df, n_po, avg_lines, start_date, end_date, config):
    pos = []
    pods = []
    po_id = 1
    pod_id = 1
    prod_ids = products_df["product_id"].tolist()
    pbar = tqdm(total=n_po, desc="Generating purchase_orders")
    for i in range(n_po):
        order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        supplier_id = random.randint(1, len(suppliers_df))
        warehouse_id = random.randint(1, len(warehouses_df))
        status = random.choices(["PENDING", "RECEIVED", "CANCELLED"], weights=[0.6, 0.35, 0.05])[0]
        total_amount = 0.0
        lines = max(1, int(np.random.poisson(lam=avg_lines)))
        for l in range(lines):
            pid = random.choice(prod_ids)
            qty = max(1, int(abs(np.random.poisson(lam=10))))
            price = round(float(products_df.loc[products_df["product_id"] == pid, "purchase_price"].values[0]), 2)
            total_amount += qty * price
            pods.append({
                "pod_id": pod_id,
                "purchase_order_id": po_id,
                "product_id": int(pid),
                "quantity": qty,
                "unit_price": price,
                "received_quantity": 0,
                "created_at": order_date.isoformat()
            })
            pod_id += 1
        pos.append({
            "purchase_order_id": po_id,
            "po_number": f"PO-{po_id:08d}",
            "supplier_id": supplier_id,
            "warehouse_id": warehouse_id,
            "status": status,
            "order_date": order_date.isoformat(),
            "expected_date": (order_date + timedelta(days=random.randint(3, 30))).isoformat(),
            "total_amount": round(total_amount, 2),
            "created_by": fake.user_name(),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        po_id += 1
        pbar.update(1)
    pbar.close()
    return pd.DataFrame(pos), pd.DataFrame(pods)


def gen_sales_and_details(products_df, warehouses_df, n_so, avg_lines, start_date, end_date, config):
    sos = []
    sods = []
    so_id = 1
    sod_id = 1
    prod_ids = products_df["product_id"].tolist()
    pbar = tqdm(total=n_so, desc="Generating sales_orders")
    for i in range(n_so):
        order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        warehouse_id = random.randint(1, len(warehouses_df))
        status = random.choices(["CONFIRMED", "SHIPPED", "CANCELLED"], weights=[0.6, 0.35, 0.05])[0]
        total_amount = 0.0
        lines = max(1, int(np.random.poisson(lam=avg_lines)))
        for l in range(lines):
            pid = random.choice(prod_ids)
            qty = max(1, int(abs(np.random.poisson(lam=3))))
            price = round(float(products_df.loc[products_df["product_id"] == pid, "retail_price"].values[0]), 2)
            total_amount += qty * price
            sods.append({
                "sod_id": sod_id,
                "sales_order_id": so_id,
                "product_id": int(pid),
                "quantity": qty,
                "unit_price": price,
                "shipped_quantity": 0,
                "created_at": order_date.isoformat()
            })
            sod_id += 1
        sos.append({
            "sales_order_id": so_id,
            "so_number": f"SO-{so_id:09d}",
            "customer_name": fake.name(),
            "warehouse_id": warehouse_id,
            "status": status,
            "order_date": order_date.isoformat(),
            "total_amount": round(total_amount, 2),
            "created_by": fake.user_name(),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        so_id += 1
        pbar.update(1)
    pbar.close()
    return pd.DataFrame(sos), pd.DataFrame(sods)

# ------------------------
# Apply movements to stock (simulate trigger) & export + validation
# ------------------------

def apply_movements_to_stock(stock_df, movements_df, config):
    stock = stock_df.set_index(["product_id", "warehouse_id"])["quantity"].to_dict()
    movements_sorted = movements_df.sort_values("created_at")
    pbar = tqdm(total=len(movements_sorted), desc="Applying movements to stock")
    for _, mv in movements_sorted.iterrows():
        try:
            pid = int(mv["product_id"])
            qty = int(mv["quantity"]) if not pd.isnull(mv["quantity"]) else 0
            mtype = mv["movement_type"]
            fwh = int(mv["from_warehouse_id"]) if not pd.isnull(mv.get("from_warehouse_id")) else None
            twh = int(mv["to_warehouse_id"]) if not pd.isnull(mv.get("to_warehouse_id")) else None
            if mtype == "IN":
                key = (pid, twh)
                stock[key] = stock.get(key, 0) + qty
            elif mtype == "OUT":
                key = (pid, fwh)
                stock[key] = stock.get(key, 0) - qty
            elif mtype == "TRANSFER":
                kf = (pid, fwh); kt = (pid, twh)
                stock[kf] = stock.get(kf, 0) - qty
                stock[kt] = stock.get(kt, 0) + qty
            elif mtype == "ADJUSTMENT":
                if twh is not None:
                    key = (pid, twh); stock[key] = stock.get(key, 0) + qty
                elif fwh is not None:
                    key = (pid, fwh); stock[key] = stock.get(key, 0) - qty
            elif mtype == "RETURN":
                key = (pid, twh); stock[key] = stock.get(key, 0) + qty
        except Exception:
            continue
        pbar.update(1)
    pbar.close()

    rows = []
    idx = 1
    for (pid, wid), qty in stock.items():
        rows.append({
            "stock_id": idx,
            "product_id": pid,
            "warehouse_id": wid,
            "quantity": int(qty),
            "reserved_quantity": 0,
            "reorder_point": 0,
            "safety_stock": 0,
            "last_movement_at": None,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })
        idx += 1
    return pd.DataFrame(rows)


def write_csv(df, path, index=False):
    ensure_dir(os.path.dirname(path))
    df.to_csv(path, index=index)


def data_validation_summary(output_folder):
    summary = {}
    files = [f for f in os.listdir(output_folder) if f.endswith(".csv")]
    for f in files:
        p = os.path.join(output_folder, f)
        try:
            df = pd.read_csv(p, nrows=1000)
            summary[f] = {
                "rows_sampled": len(df),
                "columns": list(df.columns),
                "null_counts_sample": df.isnull().sum().to_dict()
            }
        except Exception as e:
            summary[f] = {"error": str(e)}
    # dump summary
    with open(os.path.join(output_folder, "validation_summary.yaml"), "w") as fh:
        yaml.safe_dump(summary, fh)
    print("Validation summary written to", os.path.join(output_folder, "validation_summary.yaml"))


# ------------------------
# Main runner
# ------------------------
def main_cli(config_path, sample=False):
    cfg = load_config(config_path)
    outdir = cfg.get("output_dir", "output")
    ensure_dir(outdir)

    # volumes (sample mode reduces counts)
    n_warehouses = min(cfg["volumes"]["warehouses"], 3) if sample else cfg["volumes"]["warehouses"]
    n_categories = min(cfg["volumes"]["categories"], 5) if sample else cfg["volumes"]["categories"]
    n_suppliers = min(cfg["volumes"]["suppliers"], 10) if sample else cfg["volumes"]["suppliers"]
    n_products = min(cfg["volumes"]["products"], 200) if sample else cfg["volumes"]["products"]
    n_stock = min(cfg["volumes"]["stock_records"], 500) if sample else cfg["volumes"]["stock_records"]
    n_movements = min(cfg["volumes"]["stock_movements"], 2000) if sample else cfg["volumes"]["stock_movements"]
    n_po = min(cfg["volumes"]["purchase_orders"], 50) if sample else cfg["volumes"]["purchase_orders"]
    n_so = min(cfg["volumes"]["sales_orders"], 100) if sample else cfg["volumes"]["sales_orders"]
    avg_po_lines = cfg.get("avg_po_lines", 3)
    avg_so_lines = cfg.get("avg_so_lines", 3)

    start_date = datetime.fromisoformat(cfg["date_range"]["start"])
    end_date = datetime.fromisoformat(cfg["date_range"]["end"])

    print("Generating masters...")
    warehouses_df = gen_warehouses(n_warehouses)
    categories_df = gen_categories(n_categories)
    suppliers_df = gen_suppliers(n_suppliers)
    products_df = gen_products(n_products, n_categories, n_suppliers)

    print("Writing masters to CSV...")
    write_csv(warehouses_df, os.path.join(outdir, "warehouses.csv"))
    write_csv(categories_df, os.path.join(outdir, "categories.csv"))
    write_csv(suppliers_df, os.path.join(outdir, "suppliers.csv"))
    write_csv(products_df, os.path.join(outdir, "products.csv"))

    print("Generating initial stock...")
    stock_df = gen_initial_stock(products_df, warehouses_df, n_stock, cfg)
    write_csv(stock_df, os.path.join(outdir, "stock.csv"))

    print("Generating stock movements...")
    movements_df = gen_stock_movements(products_df, warehouses_df, start_date, end_date, n_movements, cfg)
    write_csv(movements_df, os.path.join(outdir, "stock_movements.csv"))

    print("Generating purchase orders and details...")
    pos_df, pods_df = gen_purchase_and_details(products_df, suppliers_df, warehouses_df, n_po, avg_po_lines, start_date, end_date, cfg)
    write_csv(pos_df, os.path.join(outdir, "purchase_orders.csv"))
    write_csv(pods_df, os.path.join(outdir, "purchase_order_details.csv"))

    print("Generating sales orders and details...")
    sos_df, sods_df = gen_sales_and_details(products_df, warehouses_df, n_so, avg_so_lines, start_date, end_date, cfg)
    write_csv(sos_df, os.path.join(outdir, "sales_orders.csv"))
    write_csv(sods_df, os.path.join(outdir, "sales_order_details.csv"))

    print("Applying movements to stock (simulation)...")
    simulated_stock_df = apply_movements_to_stock(stock_df, movements_df, cfg)
    write_csv(simulated_stock_df, os.path.join(outdir, "simulated_stock_after_movements.csv"))

    print("Running data validation summary...")
    data_validation_summary(outdir)

    print("All done. Files written to:", outdir)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate fake warehouse stock data")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    parser.add_argument("--sample", action="store_true", help="Run in sample mode (smaller volumes)")
    args = parser.parse_args()
    main_cli(args.config, sample=args.sample)
