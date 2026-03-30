# =============================================================
# Retail CDC Pipeline — Transaction Simulator
# =============================================================
# Simulates a live retail store against Postgres
# Debezium captures every INSERT and UPDATE as a CDC event
# =============================================================

import os
import time
import random
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()


# -------------------------------------------------------------
# Block 1 — Open connection to Postgres
# Reads credentials from .env — never hardcoded
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
# Block 2 — Get real IDs from Postgres
# Queries actual customers + products tables
# So we never reference an ID that doesn't exist
# -------------------------------------------------------------
def get_real_ids(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT product_id FROM products")
    product_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()

    print(f"[{datetime.now()}] Loaded {len(customer_ids)} customers, {len(product_ids)} products")
    return customer_ids, product_ids


# -------------------------------------------------------------
# Block 3 — Insert a new order + order items
# One order header + 1 to 4 product lines
# RETURNING order_id gets the auto-generated ID back from Postgres
# so we can attach order_items to the correct order
# -------------------------------------------------------------
def insert_order(conn, customer_ids, product_ids):
    cursor = conn.cursor()

    # Pick a random customer for this order
    customer_id = random.choice(customer_ids)

    # Insert the order header — status starts as PENDING
    cursor.execute("""
        INSERT INTO orders (customer_id, status, created_at, updated_at)
        VALUES (%s, 'PENDING', NOW(), NOW())
        RETURNING order_id
    """, (customer_id,))

    # Get the auto-generated order_id back from Postgres
    order_id = cursor.fetchone()[0]

    # Insert 1 to 4 product lines for this order
    num_items = random.randint(1, 4)

    for _ in range(num_items):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5.00, 200.00), 2)

        cursor.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (order_id, product_id, quantity, unit_price))

    # Commit makes both the order and order_items permanent
    conn.commit()
    cursor.close()

    print(f"[{datetime.now()}] INSERT order_id={order_id} customer_id={customer_id} items={num_items}")
    return order_id


# -------------------------------------------------------------
# Block 4 — Update order status
# PENDING -> SHIPPED -> DELIVERED
# updated_at is handled automatically by the Postgres trigger
# Each update triggers a new CDC event in Debezium
# -------------------------------------------------------------
def update_order_status(conn, order_id, new_status):
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE orders
        SET status = %s
        WHERE order_id = %s
    """, (new_status, order_id))

    conn.commit()
    cursor.close()

    print(f"[{datetime.now()}] UPDATE order_id={order_id} status={new_status}")


# -------------------------------------------------------------
# Block 5 — Fetch open orders
# Gets orders that are not yet DELIVERED
# These are candidates for status progression this cycle
# Limit 20 keeps event volume manageable during testing
# -------------------------------------------------------------
def get_open_orders(conn):
    cursor = conn.cursor()

    cursor.execute("""
        SELECT order_id, status
        FROM orders
        WHERE status != 'DELIVERED'
        ORDER BY created_at ASC
        LIMIT 20
    """)

    rows = cursor.fetchall()
    cursor.close()
    return rows


# -------------------------------------------------------------
# Status progression map
# Dictionary that maps current status to next status
# .get() returns None for DELIVERED — so we never update it again
# -------------------------------------------------------------
STATUS_PROGRESSION = {
    "PENDING": "SHIPPED",
    "SHIPPED": "DELIVERED"
}


# -------------------------------------------------------------
# Block 6 — Main simulation loop
# Runs forever until you press Ctrl+C
# Each cycle:
#   - Inserts 1-3 new orders
#   - Progresses open orders with 40% probability
#   - Sleeps 2-5 seconds before next cycle
# -------------------------------------------------------------
def run_simulation():
    print(f"[{datetime.now()}] Starting transaction simulator...")

    # Open one connection and reuse it across all cycles
    conn = get_connection()

    # Load real IDs once at startup
    customer_ids, product_ids = get_real_ids(conn)

    # Safety check — if seed scripts haven't run yet, stop here
    if not customer_ids or not product_ids:
        print("ERROR: No customers or products found in Postgres.")
        print("Run seed_customers.py and seed_products.py first.")
        conn.close()
        return

    try:
        while True:

            # --- Insert new orders this cycle ---
            num_new_orders = random.randint(1, 3)
            for _ in range(num_new_orders):
                insert_order(conn, customer_ids, product_ids)

            # --- Progress existing open orders ---
            open_orders = get_open_orders(conn)

            for order_id, current_status in open_orders:
                # 40% chance this order progresses this cycle
                if random.random() < 0.4:
                    next_status = STATUS_PROGRESSION.get(current_status)
                    # next_status is None for DELIVERED — skip those
                    if next_status:
                        update_order_status(conn, order_id, next_status)

            # --- Sleep before next cycle ---
            sleep_seconds = random.uniform(2, 5)
            print(f"[{datetime.now()}] Sleeping {sleep_seconds:.1f}s before next cycle...")
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        # Runs when you press Ctrl+C
        print(f"\n[{datetime.now()}] Simulation stopped by user")

    finally:
        # Always closes the connection cleanly no matter what
        conn.close()


# -------------------------------------------------------------
# Entry point
# Only runs run_simulation() if you execute this file directly
# Won't run if another script imports this file
# -------------------------------------------------------------
if __name__ == "__main__":
    run_simulation()