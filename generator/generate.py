"""
retail-lakehouse :: synthetic data generator

Populates the `retail` schema with realistic, internally-consistent
e-commerce data: customers, a category hierarchy, suppliers, products,
orders, order_items, and payments.

Designed to give downstream Spark / dbt enough surface area for:
  - SCD2 on customers/products (status & price changes)
  - Recursive CTEs on category hierarchy
  - AR-aging joins (orders without matching captured payments)
  - Window functions on revenue / cohort analysis

Usage:
    python generate.py                          # defaults
    python generate.py --orders 50000           # bigger run
    python generate.py --truncate               # wipe + regenerate
    python generate.py --seed 7                 # reproducible
"""

from __future__ import annotations

import argparse
import os
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from faker import Faker


# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------

DEFAULT_CUSTOMERS = 1_000
DEFAULT_PRODUCTS  = 200
DEFAULT_SUPPLIERS = 50
DEFAULT_ORDERS    = 10_000

ORDER_STATUS_WEIGHTS = [
    ("delivered", 70),
    ("shipped",   10),
    ("confirmed",  8),
    ("pending",    5),
    ("cancelled",  4),
    ("returned",   3),
]
PAYMENT_METHOD_WEIGHTS = [
    ("card",          60),
    ("bank_transfer", 20),
    ("paypal",        15),
    ("wallet",         5),
]
PAYMENT_STATUS_WEIGHTS = [
    ("captured",   92),
    ("refunded",    4),
    ("failed",      3),
    ("authorized", 1),
]
CUSTOMER_STATUS_WEIGHTS = [
    ("active",   80),
    ("churned",  15),
    ("inactive",  5),
]

# Q4 seasonal lift (Oct-Dec months get extra weight)
MONTH_WEIGHTS = {1:8, 2:7, 3:8, 4:8, 5:9, 6:9, 7:8, 8:8, 9:9, 10:12, 11:14, 12:15}

CATEGORY_TREE = {
    "Electronics":  ["Laptops", "Phones", "Audio", "Cameras"],
    "Home":         ["Furniture", "Kitchen", "Lighting"],
    "Apparel":      ["Men", "Women", "Kids"],
    "Sports":       ["Outdoor", "Fitness"],
    "Books":        ["Fiction", "Non-Fiction"],
}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def weighted_choice(choices: list[tuple[str, int]]) -> str:
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


def money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@lru_cache(maxsize=256)
def _month_candidates(start_ym: tuple[int, int], end_ym: tuple[int, int]) -> tuple[list, list]:
    """Cached: build (months, weights) for a year-month range. Called once per unique range."""
    sy, sm = start_ym
    ey, em = end_ym
    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(datetime(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    weights = [MONTH_WEIGHTS[dt.month] for dt in months]
    return months, weights


def random_datetime_between(start: datetime, end: datetime) -> datetime:
    """Pick a random datetime, weighted by month to add seasonality."""
    months, weights = _month_candidates(
        (start.year, start.month), (end.year, end.month)
    )
    month = random.choices(months, weights=weights, k=1)[0]
    next_month = datetime(month.year + 1, 1, 1) if month.month == 12 else datetime(month.year, month.month + 1, 1)
    day_span = (min(next_month, end) - max(month, start)).total_seconds()
    offset_sec = random.uniform(0, max(day_span, 1))
    return max(month, start) + timedelta(seconds=offset_sec)


# ----------------------------------------------------------------------
# Generators
# ----------------------------------------------------------------------

@dataclass
class GeneratedRow:
    pass


def gen_customers(n: int, faker: Faker) -> list[tuple]:
    rows = []
    today = datetime.now()
    earliest_signup = today - timedelta(days=365 * 3)
    seen_emails: set[str] = set()
    for _ in range(n):
        first = faker.first_name()
        last = faker.last_name()
        # Guarantee unique email
        while True:
            email = f"{first.lower()}.{last.lower()}.{random.randint(1, 99999)}@{faker.free_email_domain()}"
            if email not in seen_emails:
                seen_emails.add(email)
                break
        signup_dt = random_datetime_between(earliest_signup, today)
        status = weighted_choice(CUSTOMER_STATUS_WEIGHTS)
        rows.append((
            first, last, email,
            faker.phone_number()[:40],
            faker.country()[:60],
            faker.city()[:80],
            faker.postcode()[:20],
            signup_dt.date(),
            status,
            signup_dt,
            signup_dt,
        ))
    return rows


def gen_categories() -> tuple[list[tuple], list[tuple]]:
    """Returns (parents, children) — load parents first, then children with FK."""
    parents = [(name, None) for name in CATEGORY_TREE.keys()]
    children: list[tuple[str, str]] = []
    for parent_name, child_names in CATEGORY_TREE.items():
        for child in child_names:
            children.append((child, parent_name))
    return parents, children


def gen_suppliers(n: int, faker: Faker) -> list[tuple]:
    rows = []
    for _ in range(n):
        company = faker.company()[:160]
        rows.append((
            company,
            faker.country()[:60],
            f"orders@{faker.domain_name()}"[:160],
        ))
    return rows


def gen_products(
    n: int, faker: Faker, category_ids: list[int], supplier_ids: list[int]
) -> list[tuple]:
    rows = []
    used_skus: set[str] = set()
    for _ in range(n):
        # Unique SKU
        while True:
            sku = f"SKU-{random.randint(100000, 999999)}"
            if sku not in used_skus:
                used_skus.add(sku)
                break
        cost = round(random.uniform(2, 400), 2)
        markup = random.uniform(1.2, 2.5)
        unit_price = round(cost * markup, 2)
        rows.append((
            sku,
            faker.catch_phrase()[:200],
            random.choice(category_ids),
            random.choice(supplier_ids),
            unit_price,
            cost,
            random.random() > 0.05,  # ~5% inactive
        ))
    return rows


def gen_orders(
    n: int,
    customer_ids: list[int],
    customer_signup: dict[int, date],
) -> list[tuple]:
    rows = []
    today = datetime.now()
    earliest = today - timedelta(days=365 * 2)
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        signup = customer_signup[customer_id]
        # Order date must be >= signup
        signup_dt = datetime.combine(signup, datetime.min.time())
        order_start = max(signup_dt, earliest)
        if order_start >= today:
            continue
        order_dt = random_datetime_between(order_start, today)
        status = weighted_choice(ORDER_STATUS_WEIGHTS)
        rows.append((
            customer_id,
            order_dt,
            status,
            "USD",
            0,  # total_amount filled after items
            order_dt,
            order_dt,
        ))
    return rows


def gen_order_items(
    order_ids: list[int], products: list[tuple[int, Decimal]]
) -> tuple[list[tuple], dict[int, Decimal]]:
    """Returns (item_rows, order_totals)."""
    items: list[tuple] = []
    totals: dict[int, Decimal] = {}
    for order_id in order_ids:
        line_count = random.choices([1, 2, 3, 4, 5], weights=[35, 30, 18, 10, 7], k=1)[0]
        chosen_products = random.sample(products, k=min(line_count, len(products)))
        order_total = Decimal("0.00")
        for product_id, unit_price in chosen_products:
            qty = random.choices([1, 2, 3, 5, 10], weights=[60, 20, 10, 7, 3], k=1)[0]
            discount = random.choices([0, 0, 0, 5, 10, 20], k=1)[0]
            gross = unit_price * qty
            line_amount = money(gross * (Decimal(100 - discount) / Decimal(100)))
            order_total += line_amount
            items.append((order_id, product_id, qty, unit_price, discount, line_amount))
        totals[order_id] = order_total
    return items, totals


def gen_payments(
    orders: list[tuple[int, datetime, str, Decimal]],
) -> list[tuple]:
    """
    orders: list of (order_id, order_date, status, total_amount)
    ~95% of non-cancelled orders get a payment.
    ~3% of payments arrive late (>30 days) -> good for AR-aging models.
    """
    payments = []
    for order_id, order_date, status, total in orders:
        if status == "cancelled":
            continue
        if random.random() > 0.95:
            continue  # leave unpaid -> AR aging candidate
        # Payment delay distribution
        delay_days = random.choices(
            [0, 1, 3, 7, 14, 30, 45, 90],
            weights=[40, 20, 15, 10, 7, 5, 2, 1],
            k=1,
        )[0]
        pay_dt = order_date + timedelta(days=delay_days, seconds=random.randint(0, 86400))
        method = weighted_choice(PAYMENT_METHOD_WEIGHTS)
        pay_status = weighted_choice(PAYMENT_STATUS_WEIGHTS)
        # Refunds happen on returned orders more often
        if status == "returned" and random.random() < 0.6:
            pay_status = "refunded"
        amount = total if pay_status != "failed" else money(total * Decimal("0"))
        payments.append((
            order_id,
            pay_dt,
            amount,
            method,
            pay_status,
            f"PAY-{uuid.uuid4().hex[:16].upper()}",
        ))
    return payments


# ----------------------------------------------------------------------
# DB I/O
# ----------------------------------------------------------------------

def connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def truncate_all(cur):
    cur.execute("""
        TRUNCATE TABLE
            retail.payments,
            retail.order_items,
            retail.orders,
            retail.products,
            retail.suppliers,
            retail.categories,
            retail.customers
        RESTART IDENTITY CASCADE;
    """)


def insert_returning_ids(cur, sql: str, rows: list[tuple]) -> list[int]:
    result = psycopg2.extras.execute_values(cur, sql, rows, fetch=True, page_size=1000)
    return [r[0] for r in result]


def insert_many(cur, sql: str, rows: list[tuple]) -> None:
    psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="retail-lakehouse data generator")
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS)
    parser.add_argument("--products",  type=int, default=DEFAULT_PRODUCTS)
    parser.add_argument("--suppliers", type=int, default=DEFAULT_SUPPLIERS)
    parser.add_argument("--orders",    type=int, default=DEFAULT_ORDERS)
    parser.add_argument("--truncate",  action="store_true", help="Wipe tables before loading")
    parser.add_argument("--seed",      type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    faker = Faker()
    Faker.seed(args.seed)

    print(f"[1/7] Connecting to Postgres at {os.getenv('PG_HOST','localhost')}:{os.getenv('PG_PORT','5432')}")
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    if args.truncate:
        print("[--] Truncating retail.* tables")
        truncate_all(cur)

    # --- Customers ---
    print(f"[2/7] Generating {args.customers} customers")
    customer_rows = gen_customers(args.customers, faker)
    customer_ids = insert_returning_ids(
        cur,
        """
        INSERT INTO retail.customers
            (first_name, last_name, email, phone, country, city, postal_code,
             signup_date, status, created_at, updated_at)
        VALUES %s
        RETURNING customer_id, signup_date
        """,
        customer_rows,
    )
    # Re-fetch (id, signup) since execute_values RETURNING gives one column at a time
    cur.execute("SELECT customer_id, signup_date FROM retail.customers ORDER BY customer_id")
    customer_signup = {cid: sdate for cid, sdate in cur.fetchall()}
    customer_ids = list(customer_signup.keys())

    # --- Categories (parents then children) ---
    print("[3/7] Generating category hierarchy")
    parents, children = gen_categories()
    parent_ids = insert_returning_ids(
        cur,
        "INSERT INTO retail.categories (category_name, parent_category_id) VALUES %s RETURNING category_id",
        parents,
    )
    parent_map = dict(zip(CATEGORY_TREE.keys(), parent_ids))
    child_rows = [(name, parent_map[parent]) for name, parent in children]
    child_ids = insert_returning_ids(
        cur,
        "INSERT INTO retail.categories (category_name, parent_category_id) VALUES %s RETURNING category_id",
        child_rows,
    )
    all_category_ids = parent_ids + child_ids

    # --- Suppliers ---
    print(f"[4/7] Generating {args.suppliers} suppliers")
    supplier_rows = gen_suppliers(args.suppliers, faker)
    supplier_ids = insert_returning_ids(
        cur,
        "INSERT INTO retail.suppliers (supplier_name, country, contact_email) VALUES %s RETURNING supplier_id",
        supplier_rows,
    )

    # --- Products ---
    print(f"[5/7] Generating {args.products} products")
    product_rows = gen_products(args.products, faker, all_category_ids, supplier_ids)
    insert_many(
        cur,
        """
        INSERT INTO retail.products
            (sku, product_name, category_id, supplier_id, unit_price, cost_price, active)
        VALUES %s
        """,
        product_rows,
    )
    cur.execute("SELECT product_id, unit_price FROM retail.products")
    products = [(pid, price) for pid, price in cur.fetchall()]

    # --- Orders ---
    print(f"[6/7] Generating ~{args.orders} orders")
    order_rows = gen_orders(args.orders, customer_ids, customer_signup)
    insert_many(
        cur,
        """
        INSERT INTO retail.orders
            (customer_id, order_date, status, currency, total_amount, created_at, updated_at)
        VALUES %s
        """,
        order_rows,
    )
    cur.execute("SELECT order_id, order_date, status FROM retail.orders ORDER BY order_id")
    order_meta = cur.fetchall()
    order_ids = [r[0] for r in order_meta]

    # --- Order items ---
    print("[7/7] Generating order items + payments")
    item_rows, order_totals = gen_order_items(order_ids, products)
    insert_many(
        cur,
        """
        INSERT INTO retail.order_items
            (order_id, product_id, quantity, unit_price, discount_pct, line_amount)
        VALUES %s
        """,
        item_rows,
    )

    # Backfill order totals
    print("      Updating order totals")
    psycopg2.extras.execute_values(
        cur,
        """
        UPDATE retail.orders AS o
        SET total_amount = v.total
        FROM (VALUES %s) AS v(order_id, total)
        WHERE o.order_id = v.order_id
        """,
        [(oid, total) for oid, total in order_totals.items()],
        page_size=1000,
    )

    # Payments need (order_id, order_date, status, total)
    orders_for_pay = [
        (oid, odate, ostatus, order_totals.get(oid, Decimal("0.00")))
        for oid, odate, ostatus in order_meta
    ]
    payment_rows = gen_payments(orders_for_pay)
    insert_many(
        cur,
        """
        INSERT INTO retail.payments
            (order_id, payment_date, amount, method, status, reference_id)
        VALUES %s
        """,
        payment_rows,
    )

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM retail.customers")
    n_cust = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM retail.orders")
    n_ord = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM retail.order_items")
    n_items = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM retail.payments")
    n_pay = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM retail.products")
    n_prod = cur.fetchone()[0]

    print()
    print("Done.")
    print(f"  customers:   {n_cust:>8}")
    print(f"  products:    {n_prod:>8}")
    print(f"  orders:      {n_ord:>8}")
    print(f"  order_items: {n_items:>8}")
    print(f"  payments:    {n_pay:>8}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
