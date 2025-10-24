import os
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger("etl.load")

class DataLoader:
    def __init__(self, cfg):
        self.cfg = cfg
        self.out_dir = Path(cfg["output"]["out_dir"]) if cfg.get("output") else Path("./output")
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def save_parquet(self, df, name: str):
        if df is None or getattr(df, 'empty', True):
            logger.info(f"No data to save for {name}")
            return
        path = self.out_dir / f"{name}.parquet"
        try:
            df.to_parquet(path, index=False)
            logger.info(f"Saved {name} to {path}")
        except Exception as e:
            logger.error(f"Failed to save parquet {name}: {e}")

    def save_csv(self, df, name: str):
        if df is None or getattr(df, 'empty', True):
            logger.info(f"No data to save for {name} (csv)")
            return
        path = self.out_dir / f"{name}.csv"
        try:
            df.to_csv(path, index=False)
            logger.info(f"Saved {name} to {path}")
        except Exception as e:
            logger.error(f"Failed to save csv {name}: {e}")
