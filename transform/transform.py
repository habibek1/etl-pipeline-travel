"""
transform.py

All cleaning and transformation logic for the travel ETL pipeline.
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

VALID_CURRENCIES = {
    "USD", "EUR", "GBP", "TRY", "AED", "SAR", "QAR", "KWD",
    "BHD", "OMR", "EGP", "JPY", "CNY", "INR", "CAD", "AUD",
    "CHF", "SEK", "NOK", "DKK", "RUB", "THB", "SGD", "MYR",
}

#helpers

def _to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def _to_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _derive_direction_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    has_return = df["return_date"].notna() & (df["return_date"].astype(str).str.strip() != "")
    df["direction_type"] = np.where(has_return, "roundtrip", "oneway")
    return df


def _drop_same_origin_dest(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df[df["origin"].str.upper() != df["destination"].str.upper()].copy()
    dropped = before - len(df)
    if dropped:
        logger.warning(f"  Dropped {dropped} rows where origin == destination")
    return df


# Provider dimension

def transform_provider(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] provider")
    df = df.copy()
    df["id"]        = df["id"].astype("Int64")
    df["is_active"] = df["is_active"].astype("Int64")
    df = df.drop_duplicates(subset=["id"])
    logger.info(f"  -> {len(df):,} provider rows")
    return df

# Airport dimension

def transform_airport(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] airport")
    df = df.copy()
    df["airport_code"] = df["airport_code"].str.strip().str.upper()
    df = df.drop_duplicates(subset=["airport_code"])
    logger.info(f"  -> {len(df):,} airport rows")
    return df

# Search fact

def transform_search(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] search")
    df = df.copy()

    df["created_at"] = _to_timestamp(df["created_at"])
 
    df["departure_date"] = _to_date(df["departure_date"])
    df["return_date"]    = _to_date(df["return_date"])

    # direction_type from return_date 
    df = _derive_direction_type(df)

    df["origin"]      = df["origin"].str.strip().str.upper()
    df["destination"] = df["destination"].str.strip().str.upper()
    df = _drop_same_origin_dest(df)

    if "passenger_types" in df.columns:
        pt = pd.json_normalize(df["passenger_types"].tolist())
        pt.columns = [f"{c}_count" if c in ("adult","child","infant") else c for c in pt.columns]
        pt.index = df.index
        df = pd.concat([df.drop(columns=["passenger_types"]), pt], axis=1)

    if "price_detay" in df.columns:
        pd_ = pd.json_normalize(df["price_detay"].tolist())
        pd_.index = df.index
        rename_map = {
            "cheapest_price": "cheapest_price",
            "currency":       "price_currency",
            "flight_count":   "flight_count",
        }
        pd_ = pd_.rename(columns=rename_map)
        df = pd.concat([df.drop(columns=["price_detay"]), pd_], axis=1)

    # keep latest created_at
    df = df.sort_values("created_at", ascending=False).drop_duplicates(
        subset=["request_id"]
    )

    keep = [
        "request_id", "session_id", "user_id", "environment",
        "origin", "destination", "departure_date", "return_date",
        "direction_type", "locale", "market", "selected_currency",
        "adult_count", "child_count", "infant_count",
        "cheapest_price", "price_currency", "flight_count",
        "created_at",
    ]
    df = df[[c for c in keep if c in df.columns]]

    logger.info(f"  -> {len(df):,} search rows after transform")
    return df.reset_index(drop=True)

def transform_booking(df: pd.DataFrame, provider_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[TRANSFORM] booking")
    df = df.copy()

    df["created_at"] = _to_timestamp(df["created_at"])
    df["updated_at"] = _to_timestamp(df["updated_at"])

    df["departure_date"] = _to_date(df["departure_date"])
    df["return_date"]    = _to_date(df["return_date"])

    df = _derive_direction_type(df)

    df["origin"]      = df["origin"].str.strip().str.upper()
    df["destination"] = df["destination"].str.strip().str.upper()
    df = _drop_same_origin_dest(df)

    before = len(df)
    df = df[df["currency"].notna()]
    df = df[df["currency"].str.upper().isin(VALID_CURRENCIES)]
    logger.info(f"  Dropped {before - len(df):,} rows with invalid/null currency")

    before = len(df)
    df = df[df["total"].notna() & (df["total"] > 0)]
    logger.info(f"  Dropped {before - len(df):,} rows with total <= 0 or null")

    # latest updated_at 
    before = len(df)
    df = df.sort_values("updated_at", ascending=False).drop_duplicates(subset=["id"])
    logger.info(f"  Removed {before - len(df):,} duplicate booking rows")

    active_providers = provider_df[provider_df["is_active"] == 1][["id", "name"]].rename(
        columns={"id": "provider_id", "name": "provider_name"}
    )
    df = df.merge(active_providers, on="provider_id", how="left")

    keep = [
        "id", "request_id", "provider_id", "provider_name",
        "origin", "destination", "departure_date", "return_date",
        "direction_type", "currency", "total", "status_code",
        "created_at", "updated_at",
    ]
    df = df[[c for c in keep if c in df.columns]]

    logger.info(f"  -> {len(df):,} booking rows after transform")
    return df.reset_index(drop=True)

def build_booking_search(
    booking_df: pd.DataFrame,
    search_df:  pd.DataFrame,
) -> pd.DataFrame:

    logger.info("[TRANSFORM] building fact_booking_search")

    booking_sel = booking_df[[
        "id", "request_id", "provider_name",
        "origin", "destination", "departure_date", "return_date",
        "direction_type", "currency", "total",
        "created_at",
    ]].rename(columns={
        "id":          "booking_id",
        "origin":      "booking_origin",
        "destination": "booking_dest",
        "created_at":  "booking_created_at",
    })

    search_sel = search_df[[
        "request_id", "session_id", "user_id", "environment",
        "origin", "destination", "departure_date", "return_date",
        "direction_type", "selected_currency",
        "adult_count", "child_count", "infant_count",
        "cheapest_price", "created_at",
    ]].rename(columns={
        "origin":      "search_origin",
        "destination": "search_dest",
        "created_at":  "search_created_at",
    })

    joined = search_sel.merge(booking_sel, on="request_id", how="inner",
                              suffixes=("_search", "_booking"))

    joined["direction_type"] = joined.get(
        "direction_type_booking", joined.get("direction_type_search")
    )
    joined = joined.drop(
        columns=[c for c in joined.columns if c.endswith("_search") or c.endswith("_booking")
                 and c not in ("search_created_at", "booking_created_at")],
        errors="ignore"
    )

    # clean duplicated date columns from merge
    for col in ["departure_date_x", "departure_date_y", "return_date_x", "return_date_y"]:
        if col in joined.columns:
            joined = joined.drop(columns=[col])

    keep = [
        "request_id", "booking_id", "session_id", "user_id", "environment",
        "search_origin", "search_dest", "booking_origin", "booking_dest",
        "direction_type", "selected_currency",
        "total", "currency", "cheapest_price",
        "adult_count", "child_count", "infant_count",
        "provider_name", "search_created_at", "booking_created_at",
    ]
    joined = joined[[c for c in keep if c in joined.columns]]

    logger.info(f"  -> {len(joined):,} rows in fact_booking_search")
    return joined.reset_index(drop=True)


def run_all_transforms(raw: dict) -> dict:
    provider = transform_provider(raw["provider"])
    airport  = transform_airport(raw["airport"])
    search   = transform_search(raw["search"])
    booking  = transform_booking(raw["booking"], provider)
    bs_join  = build_booking_search(booking, search)

    return {
        "dim_provider":        provider,
        "dim_airport":         airport,
        "fact_search":         search,
        "fact_booking":        booking,
        "fact_booking_search": bs_join,
    }