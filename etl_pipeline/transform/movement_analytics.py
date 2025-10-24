
import pandas as pd
import logging

logger = logging.getLogger("etl.transform.movement")

class MovementAnalytics:
    def __init__(self, cfg):
        self.cfg = cfg

    def run(self, movements_df: pd.DataFrame) -> pd.DataFrame:
        if movements_df is None or movements_df.empty:
            logger.warning("No movement data")
            return pd.DataFrame()

        df = movements_df.copy()
        tcol = self.cfg["sources"]["csv"].get("incremental_column", "modified_date")
        if tcol in df.columns:
            df[tcol] = pd.to_datetime(df[tcol], errors="coerce")
        else:
            # fallback: try common timestamp columns
            for cand in ["timestamp", "date", "movement_date"]:
                if cand in df.columns:
                    df[cand] = pd.to_datetime(df[cand], errors="coerce")
                    tcol = cand
                    break
        df = df.dropna(subset=[tcol, "product_id"])

        df["date"] = df[tcol].dt.date
        daily = df.groupby(["product_id", "date"]).quantity.sum().reset_index()

        avg_daily = daily.groupby("product_id").quantity.mean().reset_index().rename(columns={"quantity": "avg_daily"})
        peak = daily.groupby("product_id").quantity.max().reset_index().rename(columns={"quantity": "peak_daily"})

        out = avg_daily.merge(peak, on="product_id", how="left")

        # trend: compare last 30 days vs previous 30 days
        df = df.sort_values(tcol)
        latest = df[tcol].max()
        window1 = df[df[tcol] >= (latest - pd.Timedelta(days=30))]
        window2 = df[(df[tcol] >= (latest - pd.Timedelta(days=60))) & (df[tcol] < (latest - pd.Timedelta(days=30)))]

        w1 = window1.groupby("product_id").quantity.sum().reset_index().rename(columns={"quantity": "w1_qty"})
        w2 = window2.groupby("product_id").quantity.sum().reset_index().rename(columns={"quantity": "w2_qty"})

        out = out.merge(w1, how="left", on="product_id").merge(w2, how="left", on="product_id")
        out["w1_qty"] = out["w1_qty"].fillna(0)
        out["w2_qty"] = out["w2_qty"].fillna(0)

        # avoid division by zero
        out["trend_pct"] = None
        mask = out["w2_qty"] > 0
        out.loc[mask, "trend_pct"] = (out.loc[mask, "w1_qty"] - out.loc[mask, "w2_qty"]) / out.loc[mask, "w2_qty"]

        return out
