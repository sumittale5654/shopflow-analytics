"""
ShopFlow Analytics — Data Generator
Generates realistic e-commerce data and seeds the PostgreSQL source database.
Run this once to populate your database before running the pipeline.
"""

import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from loguru import logger
from sqlalchemy import create_engine, text

from ingestion.config import DB_URL

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
NUM_CUSTOMERS = 2_000
NUM_PRODUCTS = 300
NUM_ORDERS = 15_000
DAYS_BACK = 365  # generate data for past year

CATEGORIES = {
    "Electronics":   ["Smartphones", "Laptops", "Headphones", "Tablets", "Cameras"],
    "Clothing":      ["Men's Wear", "Women's Wear", "Kids", "Sportswear", "Footwear"],
    "Home & Garden": ["Furniture", "Kitchen", "Bedding", "Garden", "Decor"],
    "Books":         ["Fiction", "Non-Fiction", "Education", "Comics", "Science"],
    "Sports":        ["Gym Equipment", "Outdoor", "Team Sports", "Yoga", "Cycling"],
}

BRANDS = ["TechPro", "StyleCo", "HomeEase", "ReadMore", "SportMax",
          "NovaBrand", "PrimeLine", "EcoChoice", "UrbanEdge", "GlobalMart"]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "upi", "net_banking"]
ORDER_STATUSES  = ["pending", "processing", "shipped", "delivered", "cancelled"]
STATUS_WEIGHTS  = [0.05, 0.10, 0.15, 0.60, 0.10]
DEVICE_TYPES    = ["desktop", "mobile", "tablet"]
BROWSERS        = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]
EVENT_TYPES     = ["page_view", "add_to_cart", "remove_from_cart", "purchase"]
EVENT_WEIGHTS   = [0.60, 0.25, 0.08, 0.07]
TIERS           = ["standard", "silver", "gold"]
TIER_WEIGHTS    = [0.70, 0.20, 0.10]


def random_date(start_days_ago: int = DAYS_BACK) -> datetime:
    return datetime.now() - timedelta(days=random.randint(0, start_days_ago))


# ── Generators ────────────────────────────────────────────────────────────────

def generate_customers() -> pd.DataFrame:
    logger.info(f"Generating {NUM_CUSTOMERS} customers...")
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        signup = random_date()
        rows.append({
            "customer_id":   i,
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "email":         fake.unique.email(),
            "phone":         fake.phone_number()[:20],
            "city":          fake.city(),
            "country":       fake.country(),
            "signup_date":   signup.date(),
            "customer_tier": random.choices(TIERS, TIER_WEIGHTS)[0],
            "created_at":    signup,
            "updated_at":    signup,
        })
    return pd.DataFrame(rows)


def generate_products() -> pd.DataFrame:
    logger.info(f"Generating {NUM_PRODUCTS} products...")
    rows = []
    pid = 1
    for category, subcats in CATEGORIES.items():
        per_cat = NUM_PRODUCTS // len(CATEGORIES)
        for _ in range(per_cat):
            cost = round(random.uniform(5, 500), 2)
            rows.append({
                "product_id":     pid,
                "product_name":   f"{random.choice(BRANDS)} {fake.word().capitalize()} {random.randint(100,999)}",
                "category":       category,
                "subcategory":    random.choice(subcats),
                "brand":          random.choice(BRANDS),
                "unit_price":     round(cost * random.uniform(1.3, 2.5), 2),
                "cost_price":     cost,
                "stock_quantity": random.randint(0, 1000),
                "is_active":      random.random() > 0.05,
                "created_at":     random_date(DAYS_BACK + 30),
                "updated_at":     random_date(),
            })
            pid += 1
    return pd.DataFrame(rows)


def generate_orders_and_items(
    customer_ids: list, product_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Generating {NUM_ORDERS} orders with items...")
    orders, items = [], []
    item_id = 1

    for oid in range(1, NUM_ORDERS + 1):
        cid = random.choice(customer_ids)
        order_date = random_date()
        discount = round(random.choices([0, 5, 10, 15, 20], [0.5, 0.2, 0.15, 0.1, 0.05])[0], 2)
        status = random.choices(ORDER_STATUSES, STATUS_WEIGHTS)[0]

        # 1-5 items per order
        num_items = random.randint(1, 5)
        selected = product_df.sample(num_items)
        order_total = 0.0

        for _, prod in selected.iterrows():
            qty = random.randint(1, 4)
            price = float(prod["unit_price"])
            disc = round(random.choices([0, 5, 10], [0.6, 0.25, 0.15])[0], 2)
            line_total = round(qty * price * (1 - disc / 100), 2)
            order_total += line_total
            items.append({
                "item_id":     item_id,
                "order_id":    oid,
                "product_id":  int(prod["product_id"]),
                "quantity":    qty,
                "unit_price":  price,
                "discount_pct": disc,
                "line_total":  line_total,
                "created_at":  order_date,
            })
            item_id += 1

        orders.append({
            "order_id":        oid,
            "customer_id":     cid,
            "order_date":      order_date,
            "status":          status,
            "payment_method":  random.choice(PAYMENT_METHODS),
            "shipping_city":   fake.city(),
            "shipping_country": fake.country(),
            "discount_pct":    discount,
            "total_amount":    round(order_total * (1 - discount / 100), 2),
            "created_at":      order_date,
            "updated_at":      order_date,
        })

    return pd.DataFrame(orders), pd.DataFrame(items)


def generate_clickstream(customer_ids: list, product_ids: list) -> pd.DataFrame:
    num_events = NUM_ORDERS * 8
    logger.info(f"Generating {num_events} clickstream events...")
    rows = []
    for _ in range(num_events):
        ts = random_date()
        cid = random.choice([None, None] + customer_ids)  # 33% anonymous
        rows.append({
            "session_id":      fake.uuid4()[:16],
            "customer_id":     cid,
            "product_id":      random.choice(product_ids + [None]),
            "event_type":      random.choices(EVENT_TYPES, EVENT_WEIGHTS)[0],
            "page_url":        f"/product/{random.choice(product_ids)}",
            "referrer":        random.choice(["google.com", "facebook.com", "direct", "email"]),
            "device_type":     random.choice(DEVICE_TYPES),
            "browser":         random.choice(BROWSERS),
            "event_timestamp": ts,
            "created_at":      ts,
        })
    return pd.DataFrame(rows)


# ── Loader ────────────────────────────────────────────────────────────────────

def load_to_postgres(df: pd.DataFrame, table: str, engine) -> None:
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=500)
    logger.success(f"  Loaded {len(df):,} rows → {table}")


def run():
    logger.info("=== ShopFlow Data Generator starting ===")
    engine = create_engine(DB_URL)

    # Truncate existing data for a clean seed
    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE clickstream, order_items, orders, products, customers RESTART IDENTITY CASCADE"
        ))

    customers = generate_customers()
    products  = generate_products()
    orders, items = generate_orders_and_items(
        customers["customer_id"].tolist(), products
    )
    clickstream = generate_clickstream(
        customers["customer_id"].tolist(),
        products["product_id"].tolist()
    )

    load_to_postgres(customers,   "customers",   engine)
    load_to_postgres(products,    "products",    engine)
    load_to_postgres(orders,      "orders",      engine)
    load_to_postgres(items,       "order_items", engine)
    load_to_postgres(clickstream, "clickstream", engine)

    # Reset watermarks
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE pipeline_watermark SET last_extracted = '1970-01-01 00:00:00'"
        ))

    logger.success("=== Data generation complete! ===")
    logger.info(f"  Customers : {len(customers):,}")
    logger.info(f"  Products  : {len(products):,}")
    logger.info(f"  Orders    : {len(orders):,}")
    logger.info(f"  Items     : {len(items):,}")
    logger.info(f"  Clickstream: {len(clickstream):,}")


if __name__ == "__main__":
    run()
