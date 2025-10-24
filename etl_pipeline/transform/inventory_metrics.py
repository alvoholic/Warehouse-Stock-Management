
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("etl.transform.inventory")

class InventoryMetrics:
    def __init__(self, cfg):
        self.cfg = cfg

    def run(self, inventory_df: pd.DataFrame, movements_df: pd.DataFrame) -> pd.DataFrame:
        if inventory_df is None or inventory_df.empty:
            logger.warning("No inventory data to compute metrics")
            return pd.DataFrame()

        df = inventory_df.copy()
        # normalize names
        df.columns = [c.lower() for c in df.columns]

        # group by product
        group_cols = ["product_id"]
        if "site_id" in df.columns:
            group_cols = ["product_id"]  # per-product metrics; adjust if you want per-site

        df = df.groupby(group_cols).agg({
            "quantity": "sum",
            "unit_cost": "mean"
        }).reset_index()

        df["inventory_value"] = df["quantity"] * df["unit_cost"].fillna(0)

        # estimate COGS (units) from movements (outgoing)
        df["cogs_quantity"] = 0
        if movements_df is not None and not movements_df.empty and "movement_type" in movements_df.columns:
            outs = movements_df[movements_df["movement_type"].astype(str).str.lower().isin(["out", "sale", "dispatch", "issued"])]
            if not outs.empty:
                cogs_df = outs.groupby("product_id").quantity.sum().reset_index().rename(columns={"quantity": "cogs_quantity"})
                df = df.merge(cogs_df, how="left", on="product_id")
                df["cogs_quantity"] = df["cogs_quantity"].fillna(0)
        else:
            df["cogs_quantity"] = 0

        # turnover ratio = COGS / avg inventory (approx avg = current quantity)
        df["turnover_ratio"] = np.where(df["quantity"] > 0, df["cogs_quantity"] / df["quantity"], np.nan)

        # Days of inventory on hand (DOH) = 365 / turnover_ratio
        df["doh"] = np.where(df["turnover_ratio"] > 0, 365.0 / df["turnover_ratio"], np.nan)

        # Dead stock: product with no movement in last N days (default 180)
        dead_days = self.cfg.get("dead_stock_days", 180)
        df["dead_stock"] = False
        if movements_df is not None and not movements_df.empty:
            tcol = self.cfg["sources"]["csv"].get("incremental_column", "modified_date")
            if tcol in movements_df.columns:
                recent = movements_df.copy()
                recent[tcol] = pd.to_datetime(recent[tcol], errors="coerce")
                last_movement = recent.groupby("product_id")[tcol].max().reset_index().rename(columns={tcol: "last_movement"})
                df = df.merge(last_movement, how="left", on="product_id")
                df["days_since_last_movement"] = (pd.Timestamp.utcnow() - pd.to_datetime(df["last_movement"])) / pd.Timedelta(days=1)
                df["dead_stock"] = df["days_since_last_movement"] > dead_days

        out_cols = ["product_id", "quantity", "inventory_value", "turnover_ratio", "doh", "dead_stock"]
        out = df[[c for c in out_cols if c in df.columns]]
        return out
