# =============================================================
# Retail CDC Pipeline — Synthetic Product Generator
# =============================================================
# Generates realistic fake products
# Called by api_products.py to top up to 100 total
# =============================================================

import random
from faker import Faker

fake = Faker()

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


def generate_products(count: int = 80) -> list:
    products = []

    for _ in range(count):
        products.append({
            "name":     fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price":    round(random.uniform(5.00, 500.00), 2)
        })

    return products