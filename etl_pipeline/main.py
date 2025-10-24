
import os
import logging
from pathlib import Path
import yaml

from extract.data_extractor import DataExtractor
from transform.inventory_metrics import InventoryMetrics
from transform.movement_analytics import MovementAnalytics
from transform.warehouse_performance import WarehousePerformance
from transform.financial_metrics import FinancialMetrics
from load.data_loader import DataLoader
from load.report_generator import ReportGenerator

ROOT = Path(__file__).parent
config_path = ROOT / "config" / "config.yaml"

with open(config_path, "r") as f:
    cfg = yaml.safe_load(f)

log_level = cfg.get("logging", {}).get("level", "INFO")
logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl")

os.makedirs(cfg["output"]["out_dir"], exist_ok=True)

def main():
    logger.info("Starting ETL pipeline")

    extractor = DataExtractor(cfg)
    raw_inventory, raw_movements = extractor.extract()

    inv_metrics = InventoryMetrics(cfg)
    inventory_summary = inv_metrics.run(raw_inventory, raw_movements)

    move_analytics = MovementAnalytics(cfg)
    movement_summary = move_analytics.run(raw_movements)

    wh_perf = WarehousePerformance(cfg)
    warehouse_summary = wh_perf.run(raw_inventory, raw_movements)

    fin_metrics = FinancialMetrics(cfg)
    financial_summary = fin_metrics.run(raw_inventory)

    loader = DataLoader(cfg)
    loader.save_parquet(inventory_summary, "inventory_summary")
    loader.save_parquet(movement_summary, "movement_summary")
    loader.save_parquet(warehouse_summary, "warehouse_summary")
    loader.save_parquet(financial_summary, "financial_summary")
    # also CSV if configured
    if cfg.get("output", {}).get("csv", False):
        loader.save_csv(inventory_summary, "inventory_summary")
        loader.save_csv(movement_summary, "movement_summary")

    reporter = ReportGenerator(cfg)
    reporter.generate_html_report({
        "inventory": inventory_summary,
        "movement": movement_summary,
        "warehouse": warehouse_summary,
        "financial": financial_summary,
    })

    logger.info("ETL run complete.")

if __name__ == "__main__":
    main()
