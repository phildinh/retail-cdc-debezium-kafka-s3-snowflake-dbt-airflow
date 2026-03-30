# =============================================================
# Retail CDC Pipeline — Customer Seeder
# =============================================================
# Loads 500 customers into Postgres before simulation starts
# 10 real customers from FakeStoreAPI + 490 synthetic via Faker
# =============================================================

import os
import requests
import psycopg2
from faker import Faker
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
fake = Faker()


# -------------------------------------------------------------
# Block 1 — Open connection to Postgres
# Same pattern as simulate_transactions.py
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
# Block 2 — Pull real customers from FakeStoreAPI
# Endpoint returns 10 users
# We extract only the fields we need for our customers table
# -------------------------------------------------------------
def fetch_api_customers():
    url = "https://fakestoreapi.com/users"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    customers = []
    for user in data:
        customers.append({
            "first_name": user["name"]["firstname"],
            "last_name":  user["name"]["lastname"],
            "email":      user["email"],
            "city":       user["address"]["city"],
            "country":    "United States"
        })

    print(f"[{datetime.now()}] Fetched {len(customers)} customers from FakeStoreAPI")
    return customers


# -------------------------------------------------------------
# Block 3 — Generate synthetic customers using Faker
# Fills the remaining 490 slots to reach 500 total
# -------------------------------------------------------------
def generate_synthetic_customers(count=990):
    customers = []

    for _ in range(count):
        customers.append({
            "first_name": fake.first_name(),
            "last_name":  fake.last_name(),
            "email":      fake.email(),
            "city":       fake.city(),
            "country":    fake.country()
        })

    print(f"[{datetime.now()}] Generated {len(customers)} synthetic customers")
    return customers


# -------------------------------------------------------------
# Block 4 — Insert all customers into Postgres
# Uses executemany for efficiency — one round trip to Postgres
# instead of 500 individual INSERT calls
# -------------------------------------------------------------
def insert_customers(conn, customers):
    cursor = conn.cursor()

    # Check if customers already loaded — avoid duplicates on re-run
    cursor.execute("SELECT COUNT(*) FROM customers")
    existing = cursor.fetchone()[0]

    if existing > 0:
        print(f"[{datetime.now()}] Customers table already has {existing} rows — skipping insert")
        cursor.close()
        return

    # Build list of tuples for executemany
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

    print(f"[{datetime.now()}] Inserted {len(records)} customers into Postgres")


# -------------------------------------------------------------
# Block 5 — Main
# Combines API + synthetic then inserts
# -------------------------------------------------------------
def run():
    print(f"[{datetime.now()}] Starting customer seeder...")

    conn = get_connection()

    api_customers       = fetch_api_customers()
    synthetic_customers = generate_synthetic_customers(count=990)

    # Combine both lists into one
    all_customers = api_customers + synthetic_customers

    insert_customers(conn, all_customers)
    conn.close()

    print(f"[{datetime.now()}] Customer seeder complete — {len(all_customers)} total")


if __name__ == "__main__":
    run()