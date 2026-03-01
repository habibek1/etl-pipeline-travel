# etl-pipeline-travel
A simple ETL pipeline built for the travel industry. It takes raw booking and search data, cleans it up, and loads it into Google BigQuery so it can be used for analysis.

The pipeline reads 4 raw parquet files — bookings, searches, providers and airports — from a Google Cloud Storage bucket, applies a set of cleaning and transformation rules, and loads the results into 5 BigQuery tables.

## Data Flow
```
Raw parquet files → GCS bucket → Extract → Transform → Load back to GCS → Load to BigQuery
```

## The Tables

| Table | What it contains |
|---|---|
| `dim_provider` | The flight providers |
| `dim_airport` | Airport codes, cities and countries |
| `fact_search` | Every search a user made — 350,000 rows |
| `fact_booking` | Completed bookings after cleaning — 7,501 rows |
| `fact_booking_search` | Which search led to which booking |

## Cleaning Rules 

- Removed bookings where origin and destination are the same airport
- Derived direction_type from return_date, if there is no return date it is oneway, if there is one it is roundtrip
- Removed bookings with missing or invalid currency
- Removed bookings with zero or negative total price
- Kept only the latest record where duplicates existed
- Flattened nested passenger and price fields so BigQuery can read them

## Tech Stack

- Python and Pandas for extraction and transformation
- Google Cloud Storage for raw and processed file storage
- Google BigQuery as the data warehouse
- Apache Airflow for pipeline orchestration

## How to Run

Upload raw files to GCS first:
```bash
gsutil cp booking_10k.parquet  gs://your-bucket/raw/booking/booking.parquet
gsutil cp search_500k.parquet  gs://your-bucket/raw/search/search.parquet
gsutil cp provider.parquet     gs://your-bucket/raw/provider/provider.parquet
gsutil cp airports.parquet     gs://your-bucket/raw/airport/airports.parquet
```

Set your credentials first:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-key.json"
export GCP_PROJECT_ID="your-project-id"
export GCS_BUCKET="your-bucket-name"
export BQ_DATASET="travel_dwh"
```

Then run the pipeline:
```bash
python3 main.py
```

## Airflow DAG

An Airflow DAG is included in the `dags/` folder. It runs the same pipeline as `main.py` but as 4 separate tasks with automatic retries and scheduling:
```
extract_raw → transform_all → upload_to_gcs → load_to_bigquery
```

Scheduled to run daily at 03:00 UTC.

The DAG has been implemented but not deployed or tested in a live Airflow environment for this submission. It is ready to deploy to Cloud Composer or a local Airflow instance.

## Notes

- 25% of search records have null selected_currency. This is expected, users are not required to select a currency before searching so no rows were dropped for this reason.
- 2,499 bookings were removed due to missing or invalid currency.
- 0 bookings were removed for invalid totals, all bookings had valid positive prices.
- Bookings are almost evenly split between oneway (3,728) and roundtrip (3,773)
- Top 3 booking currencies are EUR (2,582), TRY (2,503) and USD (2,416)
- Most popular routes are AMS→FCO, CDG→FRA and AYT→MAD

- The pipeline runs in full replace mode by default. For daily incremental runs the mode can be switched to upsert which uses a BigQuery MERGE statement to update existing rows and insert new ones without reloading everything.
- Raw parquet files are manually uploaded to GCS before running the pipeline. The Airflow DAG handles the extract, transform and load steps from GCS to BigQuery.
- In production credentials would be managed via GCP Secret Manager rather than environment variables.