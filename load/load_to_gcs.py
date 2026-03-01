"""
load_to_gcs.py

Uploads transformed DataFrames back to GCS as parquet files.
"""
import io
import logging
import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_to_gcs(
    frames: dict,
    bucket_name: str,
    prefix: str = "processed",
) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for name, df in frames.items():
        blob_path = f"{prefix}/{name}/{name}.parquet"
        buffer    = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        bucket.blob(blob_path).upload_from_file(
            buffer, content_type="application/octet-stream"
        )
        logger.info(f"[GCS] {name} gs://{bucket_name}/{blob_path}")