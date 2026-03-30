# =============================================================
# Retail CDC Pipeline — Shared Extraction Logic
# =============================================================
# Common transformation functions used by both
# api_customers.py and api_products.py
# Keeps each script clean and focused on its own endpoint
# =============================================================

from ingestion.core.utils import flatten_dict, add_metadata
from ingestion.core.logger import get_logger

logger = get_logger("extract")


# -------------------------------------------------------------
# Transform raw FakeStoreAPI user record
# FakeStoreAPI returns nested objects like:
# {"name": {"firstname": "John", "lastname": "Doe"},
#  "address": {"city": "Sydney", ...}}
# We flatten and extract only what we need
# -------------------------------------------------------------
def transform_customer(raw: dict) -> dict:
    return {
        "first_name": raw.get("name", {}).get("firstname", ""),
        "last_name":  raw.get("name", {}).get("lastname", ""),
        "email":      raw.get("email", ""),
        "city":       raw.get("address", {}).get("city", ""),
        "country":    "United States"
    }


# -------------------------------------------------------------
# Transform raw FakeStoreAPI product record
# FakeStoreAPI returns:
# {"title": "...", "category": "...", "price": 29.99}
# We rename title to name to match our schema
# -------------------------------------------------------------
def transform_product(raw: dict) -> dict:
    return {
        "name":     raw.get("title", ""),
        "category": raw.get("category", ""),
        "price":    round(float(raw.get("price", 0.0)), 2)
    }


# -------------------------------------------------------------
# Apply metadata to a list of transformed records
# Adds _source and _ingested_at to every record
# So S3 and Snowflake always know where data came from
# -------------------------------------------------------------
def apply_metadata(records: list, source: str) -> list:
    return [add_metadata(record, source) for record in records]