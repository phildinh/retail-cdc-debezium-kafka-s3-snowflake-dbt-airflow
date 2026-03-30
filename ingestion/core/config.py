# =============================================================
# Retail CDC Pipeline — Central Config
# =============================================================
# Single source of truth for all configuration values
# Every ingestion script imports from here
# Never reads .env directly — always goes through this file
# =============================================================

import os
from dotenv import load_dotenv

load_dotenv()


# -------------------------------------------------------------
# Postgres
# -------------------------------------------------------------
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB       = os.getenv("POSTGRES_DB")
POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# -------------------------------------------------------------
# Kafka
# -------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_ORDERS      = os.getenv("KAFKA_TOPIC_ORDERS", "postgres.public.orders")
KAFKA_TOPIC_ORDER_ITEMS = os.getenv("KAFKA_TOPIC_ORDER_ITEMS", "postgres.public.order_items")
KAFKA_CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "retail_cdc_consumer")


# -------------------------------------------------------------
# AWS S3
# -------------------------------------------------------------
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "ap-southeast-2")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")
S3_PREFIX             = os.getenv("S3_PREFIX", "raw")


# -------------------------------------------------------------
# Snowflake
# -------------------------------------------------------------
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_CDC")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "LOADER")


# -------------------------------------------------------------
# S3 path builder
# Partitions raw files by table and timestamp
# Pattern: raw/orders/2026/03/30/09/
# This matches our incremental load strategy in Snowflake
# -------------------------------------------------------------
def get_s3_key(table_name: str, filename: str) -> str:
    from datetime import datetime
    now = datetime.utcnow()
    return (
        f"{S3_PREFIX}/"
        f"{table_name}/"
        f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/"
        f"{filename}"
    )