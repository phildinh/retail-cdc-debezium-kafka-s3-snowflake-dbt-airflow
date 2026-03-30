# =============================================================
# Retail CDC Pipeline — Customer API Ingestion
# =============================================================
# Pulls 10 real customers from FakeStoreAPI
# Generates 490 synthetic customers
# Writes all 500 to S3 and Postgres in one run
# =============================================================

import boto3
import psycopg2
from ingestion.core.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    AWS_REGION, S3_BUCKET_NAME,
    get_s3_key
)
from ingestion.core.logger import get_logger
from ingestion.core.utils import to_ndjson, generate_filename, add_metadata
from ingestion.api.api_client import fetch
from ingestion.api.extract import transform_customer
from scripts.generate_customers import generate_customers

logger = get_logger("api_customers")


# -------------------------------------------------------------
# Block 1 — Fetch real customers from FakeStoreAPI
# -------------------------------------------------------------
def get_api_customers() -> list:
    raw_data  = fetch("/users")
    customers = [transform_customer(record) for record in raw_data]
    logger.info(f"Fetched {len(customers)} real customers from FakeStoreAPI")
    return customers


# -------------------------------------------------------------
# Block 2 — Generate synthetic customers
# Tops up to 500 total
# -------------------------------------------------------------
def get_synthetic_customers(count: int = 9900) -> list:
    customers = generate_customers(count)
    logger.info(f"Generated {len(customers)} synthetic customers")
    return customers


# -------------------------------------------------------------
# Block 3 — Write all customers to S3
# Combines real + synthetic into one file
# -------------------------------------------------------------
def write_to_s3(customers: list) -> str:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    filename = generate_filename("customers")
    s3_key   = get_s3_key("customers", filename)

    # Add metadata before writing to S3
    records_with_meta = [
        add_metadata(c, source="fakestoreapi+synthetic")
        for c in customers
    ]

    body = to_ndjson(records_with_meta)

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(f"Wrote {len(customers)} customers to s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key


# -------------------------------------------------------------
# Block 4 — Insert all customers into Postgres
# Skips if already loaded — idempotent
# -------------------------------------------------------------
def write_to_postgres(customers: list):
    conn   = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM customers")
    existing = cursor.fetchone()[0]

    if existing > 0:
        logger.info(f"Customers table already has {existing} rows — skipping Postgres insert")
        cursor.close()
        conn.close()
        return

    records = [
        (c["first_name"], c["last_name"], c["email"], c["city"], c["country"])
        for c in customers
    ]

    cursor.executemany("""
        INSERT INTO customers (first_name, last_name, email, city, country)
        VALUES (%s, %s, %s, %s, %s)
    """, records)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Inserted {len(records)} customers into Postgres")


# -------------------------------------------------------------
# Block 5 — Main
# Combines real + synthetic then writes to S3 + Postgres
# -------------------------------------------------------------
def run():
    logger.info("Starting customer ingestion...")

    api_customers       = get_api_customers()
    synthetic_customers = get_synthetic_customers(count=9900)
    all_customers       = api_customers + synthetic_customers

    logger.info(f"Total customers to load: {len(all_customers)}")

    write_to_s3(all_customers)
    write_to_postgres(all_customers)

    logger.info(f"Customer ingestion complete — {len(all_customers)} records")


if __name__ == "__main__":
    run()