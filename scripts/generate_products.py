# =============================================================
# Retail CDC Pipeline — Product Seeder
# =============================================================
# Loads 100 products into Postgres before simulation starts
# 20 real products from FakeStoreAPI + 80 synthetic
# =============================================================

import os
import random
import requests
import psycopg2
from faker import Faker
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
fake = Faker()

# Realistic retail categories for synthetic products
CATEGORIES = [
    "electronics",
    "jewelery",
    "men's clothing",
    "women's clothing",
    "home & kitchen",
    "sports & outdoors",
    "beauty & personal care",
    "books"
]


# -------------------------------------------------------------
# Block 1 — Open connection to Postgres
# -------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


# -------------------------------------------------------------
# Block 2 — Pull real products from FakeStoreAPI
# Endpoint returns 20 products
# -------------------------------------------------------------
def fetch_api_products():
    url = "https://fakestoreapi.com/products"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    products = []
    for product in data:
        products.append({
            "name":     product["title"],
            "category": product["category"],
            "price":    round(float(product["price"]), 2)
        })

    print(f"[{datetime.now()}] Fetched {len(products)} products from FakeStoreAPI")
    return products


# -------------------------------------------------------------
# Block 3 — Generate synthetic products
# Uses fake.catch_phrase() for product names — sounds realistic
# Random category from our retail list
# Random price between $5 and $500
# -------------------------------------------------------------
def generate_synthetic_products(count=80):
    products = []

    for _ in range(count):
        products.append({
            "name":     fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price":    round(random.uniform(5.00, 500.00), 2)
        })

    print(f"[{datetime.now()}] Generated {len(products)} synthetic products")
    return products


# -------------------------------------------------------------
# Block 4 — Insert all products into Postgres
# Same idempotency check as seed_customers
# Skips insert if products already loaded
# -------------------------------------------------------------
def insert_products(conn, products):
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM products")
    existing = cursor.fetchone()[0]

    if existing > 0:
        print(f"[{datetime.now()}] Products table already has {existing} rows — skipping insert")
        cursor.close()
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

    print(f"[{datetime.now()}] Inserted {len(records)} products into Postgres")


# -------------------------------------------------------------
# Block 5 — Main
# -------------------------------------------------------------
def run():
    print(f"[{datetime.now()}] Starting product seeder...")

    conn = get_connection()

    api_products       = fetch_api_products()
    synthetic_products = generate_synthetic_products(count=80)

    all_products = api_products + synthetic_products

    insert_products(conn, all_products)
    conn.close()

    print(f"[{datetime.now()}] Product seeder complete — {len(all_products)} total")


if __name__ == "__main__":
    run()