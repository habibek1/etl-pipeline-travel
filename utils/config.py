"""
config.py
"""
import os

# ── GCP ───────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "etl-project-travel")
GCS_BUCKET     = os.getenv("GCS_BUCKET",     "travel-etl-hbb")
BQ_DATASET     = os.getenv("BQ_DATASET",     "travel_dwh")
BQ_LOCATION = "europe-west3"

# ── GCS Paths ─────────────────────────────────────────────────────────────────
GCS_PATHS = {
    "booking":  "raw/booking/booking.parquet",
    "search":   "raw/search/search.parquet",
    "provider": "raw/provider/provider.parquet",
    "airport":  "raw/airport/airports.parquet",
}

# ── BigQuery Table Names ──────────────────────────────────────────────────────
BQ_TABLES = {
    "dim_provider":        f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_provider",
    "dim_airport":         f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_airport",
    "fact_search":         f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_search",
    "fact_booking":        f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_booking",
    "fact_booking_search": f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_booking_search",
}