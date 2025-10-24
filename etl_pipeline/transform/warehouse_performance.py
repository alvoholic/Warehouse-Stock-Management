
import pandas as pd
import logging

logger = logging.getLogger("etl.transform.wh")

class WarehousePerformance:
    def __init__(self, cfg):
        self.cfg = cfg

    def run(self, inventory_df: pd.DataFrame, movements_df: pd.DataFrame) -> pd.DataFrame:
        if inventory_df is None or inventory_df.empty:
            logger.warning("No inventory data for warehouse performance")
            return pd.DataFrame()

        inv = inventory_df.copy()
        inv.columns = [c.lower() for c in inv.columns]

        # ensure site_id present
        if "site_id" not in inv.columns:
            # if no site info, create a single aggregate row
            total = inv["quantity"].sum() if "quantity" in inv.columns else 0
            out = pd.DataFrame([{"site_id": "ALL", "quantity": total, "capacity": self.cfg.get("default_capacity", 100000)}])
            out["utilization"] = out["quantity"] / out["capacity"]
            return out

        inv_agg = inv.groupby("site_id").quantity.sum().reset_index()
        # capacity may not exist in data; use config or default
        inv_agg["capacity"] = inv_agg.get("capacity", self.cfg.get("default_capacity", 100000))
        inv_agg["utilization"] = inv_agg["quantity"] / inv_agg["capacity"]

        transfers = pd.DataFrame()
        if movements_df is not None and not movements_df.empty and "from_site" in movements_df.columns and "to_site" in movements_df.columns:
            moves = movements_df.dropna(subset=["from_site", "to_site", "quantity"])
            transfers = moves.groupby(["from_site", "to_site"]).quantity.sum().reset_index().rename(columns={"from_site": "src", "to_site": "dst"})
        # merge to provide a view; note: this will widen rows if transfers exist
        if not transfers.empty:
            out = inv_agg.merge(transfers, how="left", left_on="site_id", right_on="src")
        else:
            out = inv_agg

        return out
