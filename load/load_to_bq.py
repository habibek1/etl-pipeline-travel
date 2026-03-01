"""
load_to_bq.py

two strategies:
  1. load_replace: full truncate and reload
  2. merge_upsert: MERGE statement for incremental updates
"""
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition

logger = logging.getLogger(__name__)

def load_replace(
    client:   bigquery.Client,
    df:       pd.DataFrame,
    table_id: str,
) -> None:
    logger.info(f"[BQ] Loading {table_id}  ({len(df):,} rows)")
    job_config = LoadJobConfig(
        write_disposition = WriteDisposition.WRITE_TRUNCATE,
        autodetect        = True,
    )
    client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    ).result()
    logger.info(f"[BQ] {table_id} loaded successfully")


def merge_upsert(
    client:        bigquery.Client,
    df:            pd.DataFrame,
    target_table:  str,
    staging_table: str,
    merge_key:     str,
) -> None:

    logger.info(f"[BQ MERGE] Loading staging -> {staging_table}")
    load_replace(client, df, staging_table)

    all_cols      = df.columns.tolist()
    update_cols   = [c for c in all_cols if c != merge_key]
    key_cond      = f"T.{merge_key} = S.{merge_key}"
    update_clause = ", ".join(f"T.{c} = S.{c}" for c in update_cols)
    insert_cols   = ", ".join(all_cols)
    insert_vals   = ", ".join(f"S.{c}" for c in all_cols)

    sql = f"""
        MERGE `{target_table}` T
        USING `{staging_table}` S
        ON {key_cond}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    logger.info(f"[BQ MERGE] Running MERGE -> {target_table}")
    client.query(sql).result()
    logger.info(f"[BQ MERGE] {target_table} upserted successfully")
    client.delete_table(staging_table, not_found_ok=True)
    logger.info(f"[BQ MERGE] Staging table dropped")


def load_all_to_bq(
    frames:     dict,
    project_id: str,
    dataset:    str,
    bq_tables:  dict,
    mode:       str = "replace",
) -> None:
    
    client = bigquery.Client(project=project_id)

    MERGE_KEYS = {
        "fact_booking":        "id",
        "fact_search":         "request_id",
        "fact_booking_search": "request_id",
    }

    for name, df in frames.items():
        table_id = bq_tables[name]
        is_dim   = name.startswith("dim_")

        if is_dim or mode == "replace":
            load_replace(client, df, table_id)
        else:
            staging  = f"{table_id}_staging"
            key      = MERGE_KEYS.get(name, df.columns[0])
            merge_upsert(client, df, table_id, staging, key)