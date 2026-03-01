"""
main.py
───────
Runs the full ETL pipeline:
  Extract from GCS -> Transform -> Load to BigQuery
"""
import logging
from extract.extract import extract_from_gcs
from transform.transform import run_all_transforms
from load.load_to_gcs import upload_to_gcs
from load.load_to_bq import load_all_to_bq
from utils.config import (
    GCP_PROJECT_ID,
    GCS_BUCKET,
    BQ_DATASET,
    GCS_PATHS,
    BQ_TABLES,
)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    
    logger.info("━━━━━━━━━━━━━━━━━━━━  EXTRACT  ━━━━━━━━━━━━━━━━━━━━")
    raw = extract_from_gcs(GCS_BUCKET, GCS_PATHS)

    logger.info("━━━━━━━━━━━━━━━━━━━━  TRANSFORM  ━━━━━━━━━━━━━━━━━━")
    cleaned = run_all_transforms(raw)
    for name, df in cleaned.items():
        logger.info(f"  {name:30s}  {len(df):>8,} rows")

    logger.info("━━━━━━━━━━━━━━━━━━━━  LOAD -> GCS  ━━━━━━━━━━━━━━━━━")
    upload_to_gcs(cleaned, GCS_BUCKET)

    logger.info("━━━━━━━━━━━━━━━━━━━━  LOAD -> BIGQUERY  ━━━━━━━━━━━━")
    load_all_to_bq(cleaned, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLES, mode="replace")

    logger.info("━━━━━━━━━━━━━━━━━━━━  DONE  ━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    main()