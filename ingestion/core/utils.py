# =============================================================
# Retail CDC Pipeline — Shared Utilities
# =============================================================
# Helper functions used across all ingestion scripts
# Anything reusable that doesn't belong in config or logger
# =============================================================

import json
import os
from datetime import datetime
from ingestion.core.logger import get_logger

logger = get_logger("utils")


# -------------------------------------------------------------
# Convert a list of dictionaries to newline-delimited JSON
# Standard format for S3 data lake files
# Snowflake COPY INTO reads this natively line by line
# -------------------------------------------------------------
def to_ndjson(records: list) -> str:
    return "\n".join(json.dumps(record) for record in records)


# -------------------------------------------------------------
# Add pipeline metadata to every record before writing to S3
# So we always know when and where a record came from
# -------------------------------------------------------------
def add_metadata(record: dict, source: str) -> dict:
    record["_source"]      = source
    record["_ingested_at"] = datetime.utcnow().isoformat()
    return record


# -------------------------------------------------------------
# Safe JSON loader
# Returns None instead of crashing if JSON is malformed
# -------------------------------------------------------------
def safe_json_loads(raw: str) -> dict:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse JSON: {e}")
        return None


# -------------------------------------------------------------
# Generate a timestamped filename for S3 uploads
# Pattern: {table}_{YYYYMMDD_HHMMSS_ffffff}.json
# Microseconds ensure uniqueness across multiple runs per second
# -------------------------------------------------------------
def generate_filename(table: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    return f"{table}_{timestamp}.json"


# -------------------------------------------------------------
# Flatten a nested dictionary one level deep
# FakeStoreAPI returns nested objects like address.city
# This makes them flat for easy Postgres + Snowflake loading
#
# Example:
# {"name": {"firstname": "John"}} -> {"name_firstname": "John"}
# -------------------------------------------------------------
def flatten_dict(record: dict, parent_key: str = "", sep: str = "_") -> dict:
    items = {}
    for key, value in record.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep))
        else:
            items[new_key] = value
    return items