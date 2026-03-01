"""
extract.py
"""
import logging
import pandas as pd
from google.cloud import storage
import io

logger = logging.getLogger(__name__)


def extract_from_gcs(bucket_name: str, gcs_paths: dict) -> dict[str, pd.DataFrame]:

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    frames = {}

    for name, blob_path in gcs_paths.items():
        logger.info(f"[EXTRACT] Downloading gs://{bucket_name}/{blob_path}")
        blob = bucket.blob(blob_path)
        data = blob.download_as_bytes()
        frames[name] = pd.read_parquet(io.BytesIO(data))
        logger.info(f"[EXTRACT] {name}: {frames[name].shape[0]:,} rows")

    return frames