
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("etl.transform.financial")

class FinancialMetrics:
    def __init__(self, cfg):
        self.cfg = cfg

    def run(self, inventory_df: pd.DataFrame) -> pd.DataFrame:
        if inventory_df is None or inventory_df.empty:
            logger.warning("No inventory for financial metrics")
            return pd.DataFrame()

        df = inventory_df.copy()
        df.columns = [c.lower() for c in df.columns]
        if "quantity" not in df.columns:
            df["quantity"] = 0
        if "unit_cost" not in df.columns:
            df["unit_cost"] = 0.0

        df["inventory_value"] = df["quantity"] * df["unit_cost"]

        # holding cost: assume fixed annual rate (configurable)
        rate = self.cfg.get("holding_cost_rate", 0.20)
        df["annual_holding_cost"] = df["inventory_value"] * rate

        # ABC analysis by inventory value
        abc = df.groupby("product_id").inventory_value.sum().reset_index().sort_values("inventory_value", ascending=False)
        abc["cum_value"] = abc["inventory_value"].cumsum()
        total = abc["inventory_value"].sum()
        abc["pct_cum"] = abc["cum_value"] / (total if total > 0 else 1)

        def label(x):
            if x <= 0.8:
                return "A"
            if x <= 0.95:
                return "B"
            return "C"

        abc["class"] = abc["pct_cum"].apply(label)
        out = df.merge(abc[["product_id", "class"]], how="left", on="product_id")
        return out
