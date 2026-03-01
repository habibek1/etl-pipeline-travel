"""
etl_dag.py

DAG Flow:
    extract_raw
        ↓
    transform_all
        ↓
    upload_to_gcs
        ↓
    load_to_bigquery
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner":            "habibek",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}


def task_extract(**context):
    from extract.extract import extract_from_gcs
    from utils.config import GCS_BUCKET, GCS_PATHS
    import pickle

    raw = extract_from_gcs(GCS_BUCKET, GCS_PATHS)
    pickle.dump(raw, open("/tmp/etl_raw.pkl", "wb"))


def task_transform(**context):
    from transform.transform import run_all_transforms
    import pickle

    raw     = pickle.load(open("/tmp/etl_raw.pkl", "rb"))
    cleaned = run_all_transforms(raw)
    pickle.dump(cleaned, open("/tmp/etl_cleaned.pkl", "wb"))


def task_upload_gcs(**context):
    from load.load_to_gcs import upload_to_gcs
    from utils.config import GCS_BUCKET
    import pickle

    cleaned = pickle.load(open("/tmp/etl_cleaned.pkl", "rb"))
    upload_to_gcs(cleaned, GCS_BUCKET)


def task_load_bigquery(**context):
    from load.load_to_bq import load_all_to_bq
    from utils.config import GCP_PROJECT_ID, BQ_DATASET, BQ_TABLES
    import pickle

    cleaned = pickle.load(open("/tmp/etl_cleaned.pkl", "rb"))
    load_all_to_bq(cleaned, GCP_PROJECT_ID, BQ_DATASET, BQ_TABLES, mode="replace")


with DAG(
    dag_id          = "travel_etl_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "Travel ETL — Booking, Search, Provider, Airport → BigQuery",
    schedule        = "0 3 * * *",
    start_date      = datetime(2026, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["travel", "etl", "bigquery"],
) as dag:

    extract    = PythonOperator(task_id="extract_raw",      python_callable=task_extract)
    transform  = PythonOperator(task_id="transform_all",    python_callable=task_transform)
    upload_gcs = PythonOperator(task_id="upload_to_gcs",    python_callable=task_upload_gcs)
    load_bq    = PythonOperator(task_id="load_to_bigquery", python_callable=task_load_bigquery)

    extract >> transform >> upload_gcs >> load_bq