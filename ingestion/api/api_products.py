# =============================================================
# Retail CDC Pipeline — Product API Ingestion
# =============================================================
# Pulls 20 real products from FakeStoreAPI
# Generates 80 synthetic products
# Writes all 100 to S3 and Postgres in one run
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
from ingestion.api.extract import transform_product
from scripts.generate_products import generate_products

logger = get_logger("api_products")


# -------------------------------------------------------------
# Block 1 — Fetch real products from FakeStoreAPI
# -------------------------------------------------------------
def get_api_products() -> list:
    raw_data = fetch("/products")
    products = [transform_product(record) for record in raw_data]
    logger.info(f"Fetched {len(products)} real products from FakeStoreAPI")
    return products


# -------------------------------------------------------------
# Block 2 — Generate synthetic products
# Tops up to 100 total
# -------------------------------------------------------------
def get_synthetic_products(count: int = 380) -> list:
    products = generate_products(count)
    logger.info(f"Generated {len(products)} synthetic products")
    return products


# -------------------------------------------------------------
# Block 3 — Write all products to S3
# -------------------------------------------------------------
def write_to_s3(products: list) -> str:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    filename = generate_filename("products")
    s3_key   = get_s3_key("products", filename)

    records_with_meta = [
        add_metadata(p, source="fakestoreapi+synthetic")
        for p in products
    ]

    body = to_ndjson(records_with_meta)

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    logger.info(f"Wrote {len(products)} products to s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key


# -------------------------------------------------------------
# Block 4 — Insert all products into Postgres
# -------------------------------------------------------------
def write_to_postgres(products: list):
    conn   = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM products")
    existing = cursor.fetchone()[0]

    if existing > 0:
        logger.info(f"Products table already has {existing} rows — skipping Postgres insert")
        cursor.close()
        conn.close()
        return

    records = [
        (p["name"], p["category"], p["price"])
        for p in products
    ]

    cursor.executemany("""
        INSERT INTO products (name, category, price)
        VALUES (%s, %s, %s)
    """, records)

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Inserted {len(records)} products into Postgres")


# -------------------------------------------------------------
# Block 5 — Main
# -------------------------------------------------------------
def run():
    logger.info("Starting product ingestion...")

    api_products       = get_api_products()
    synthetic_products = get_synthetic_products(count=380)
    all_products       = api_products + synthetic_products

    logger.info(f"Total products to load: {len(all_products)}")

    write_to_s3(all_products)
    write_to_postgres(all_products)

    logger.info(f"Product ingestion complete — {len(all_products)} records")


if __name__ == "__main__":
    run()