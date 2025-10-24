
import os
import json
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

logger = logging.getLogger("etl.extract")

class DataExtractor:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.state_file = Path(cfg.get("incremental", {}).get("state_file", ".etl_state.json"))

    def _load_state(self):
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text())
            except Exception:
                return {}
        return {}

    def _save_state(self, state: dict):
        self.state_file.write_text(json.dumps(state, default=str))

    def extract(self):
        state = self._load_state()
        postgres_cfg = self.cfg["sources"]["postgres"]
        csv_cfg = self.cfg["sources"]["csv"]

        # Postgres extract
        inventory_df = pd.DataFrame()
        if postgres_cfg.get("enabled"):
            pw = os.getenv(postgres_cfg.get("password_env", ""), "")
            conn_str = f"postgresql+psycopg2://{postgres_cfg['user']}:{pw}@{postgres_cfg['host']}:{postgres_cfg['port']}/{postgres_cfg['database']}"
            engine = create_engine(conn_str, pool_pre_ping=True)
            inc_col = postgres_cfg.get("incremental_column", "last_updated")
            last = state.get("postgres_last", None)
            if last is None:
                lookback = timedelta(days=self.cfg.get("incremental", {}).get("default_lookback_days", 7))
                last = (datetime.utcnow() - lookback).isoformat()
            query = text(f"SELECT * FROM {postgres_cfg['incremental_table']} WHERE {inc_col} >= :last")
            try:
                inventory_df = pd.read_sql(query, engine, params={"last": last})
                logger.info(f"Extracted {len(inventory_df)} rows from Postgres since {last}")
            except Exception as e:
                logger.error(f"Failed to extract from Postgres: {e}")
                inventory_df = pd.DataFrame()
            state["postgres_last"] = datetime.utcnow().isoformat()

        # CSV extract
        movements_df = pd.DataFrame()
        if csv_cfg.get("enabled"):
            path = Path(csv_cfg["path"])
            if not path.exists():
                logger.warning(f"CSV path not found: {path}")
            else:
                try:
                    movements_df = pd.read_csv(path, parse_dates=[csv_cfg.get("incremental_column")])
                    last_csv = state.get("csv_last", None)
                    if last_csv:
                        movements_df = movements_df[pd.to_datetime(movements_df[csv_cfg.get("incremental_column")]) >= pd.to_datetime(last_csv)]
                    logger.info(f"Loaded {len(movements_df)} rows from CSV")
                except Exception as e:
                    logger.error(f"Failed to read CSV: {e}")
                    movements_df = pd.DataFrame()
                state["csv_last"] = datetime.utcnow().isoformat()

        # Basic data quality
        inventory_df = self._clean_inventory(inventory_df)
        movements_df = self._clean_movements(movements_df)

        self._save_state(state)
        return inventory_df, movements_df

    def _clean_inventory(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df.columns = [c.strip().lower() for c in df.columns]
        required = ["product_id", "site_id", "quantity", "last_updated", "unit_cost"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning(f"Inventory missing columns: {missing}")
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(float)
        if "unit_cost" in df.columns:
            df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce").fillna(0.0)
        if "last_updated" in df.columns:
            df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
        return df

    def _clean_movements(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df.columns = [c.strip().lower() for c in df.columns]
        tcol = self.cfg["sources"]["csv"].get("incremental_column", "modified_date")
        if tcol in df.columns:
            df[tcol] = pd.to_datetime(df[tcol], errors="coerce")
        if "product_id" in df.columns:
            df = df.dropna(subset=["product_id"])
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(float)
        return df
