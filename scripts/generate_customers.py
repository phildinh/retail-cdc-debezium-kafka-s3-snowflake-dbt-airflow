# =============================================================
# Retail CDC Pipeline — Synthetic Customer Generator
# =============================================================
# Generates realistic fake customers using Faker
# Called by api_customers.py to top up to 500 total
# =============================================================

from faker import Faker

fake = Faker()


def generate_customers(count: int = 490) -> list:
    customers = []

    for _ in range(count):
        customers.append({
            "first_name": fake.first_name(),
            "last_name":  fake.last_name(),
            "email":      fake.email(),
            "city":       fake.city(),
            "country":    fake.country()
        })

    return customers