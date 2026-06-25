"""
Generates synthetic e-commerce data (customers, products, orders, order_items)
to simulate a daily extract dropped by an upstream operational system.

Run:
    python src/generate_sample_data.py --num-customers 500 --num-orders 5000
"""
import argparse
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()

CATEGORIES = ["Electronics", "Home & Kitchen", "Apparel", "Books", "Sports", "Toys", "Beauty"]


def generate_customers(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        rows.append(
            {
                "customer_id": str(uuid.uuid4()),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "country": fake.country(),
                "signup_date": fake.date_between(start_date="-3y", end_date="-1d"),
            }
        )
    return pd.DataFrame(rows)


def generate_products(n: int = 200) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        rows.append(
            {
                "product_id": str(uuid.uuid4()),
                "product_name": fake.word().title() + " " + fake.word().title(),
                "category": random.choice(CATEGORIES),
                "unit_price": round(random.uniform(5, 500), 2),
            }
        )
    return pd.DataFrame(rows)


def generate_orders(customers: pd.DataFrame, products: pd.DataFrame, n: int):
    order_rows, item_rows = [], []
    customer_ids = customers["customer_id"].tolist()
    product_records = products.to_dict("records")

    for _ in range(n):
        order_id = str(uuid.uuid4())
        order_date = datetime.now() - timedelta(days=random.randint(0, 90))
        customer_id = random.choice(customer_ids)

        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "order_status": random.choices(
                    ["completed", "cancelled", "returned"], weights=[0.88, 0.07, 0.05]
                )[0],
            }
        )

        for _ in range(random.randint(1, 5)):
            product = random.choice(product_records)
            qty = random.randint(1, 4)
            item_rows.append(
                {
                    "order_item_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": qty,
                    "unit_price": product["unit_price"],
                }
            )

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-customers", type=int, default=500)
    parser.add_argument("--num-products", type=int, default=200)
    parser.add_argument("--num-orders", type=int, default=5000)
    parser.add_argument("--out-dir", type=str, default="data/raw")
    args = parser.parse_args()

    customers = generate_customers(args.num_customers)
    products = generate_products(args.num_products)
    orders, order_items = generate_orders(customers, products, args.num_orders)

    customers.to_csv(f"{args.out_dir}/customers.csv", index=False)
    products.to_csv(f"{args.out_dir}/products.csv", index=False)
    orders.to_csv(f"{args.out_dir}/orders.csv", index=False)
    order_items.to_csv(f"{args.out_dir}/order_items.csv", index=False)

    print(
        f"Generated {len(customers)} customers, {len(products)} products, "
        f"{len(orders)} orders, {len(order_items)} order_items -> {args.out_dir}/"
    )


if __name__ == "__main__":
    main()
