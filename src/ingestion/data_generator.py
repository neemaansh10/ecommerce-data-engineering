import os
import random
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any
from src.utils.logger import logger

def generate_customers(num_records: int = 100) -> List[Dict[str, Any]]:
    """
    Generates synthetic customer dataset with realistic metadata.
    """
    cities = [("New York", "USA"), ("London", "UK"), ("Toronto", "Canada"),
              ("Berlin", "Germany"), ("Tokyo", "Japan"), ("Sydney", "Australia"),
              ("Paris", "France"), ("Mumbai", "India"), ("Singapore", "Singapore")]

    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "Ansh", "Priya", "Carlos", "Yuki"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Neema", "Patel", "Sato", "Chen"]

    customers = []
    base_date = datetime(2025, 1, 1)

    for i in range(1, num_records + 1):
        fname = random.choice(first_names)
        lname = random.choice(last_names)
        city, country = random.choice(cities)
        signup_dt = base_date + timedelta(days=random.randint(0, 500))

        cust = {
            "customer_id": f"CUST{i:05d}",
            "name": f"{fname} {lname}",
            "email": f"{fname.lower()}.{lname.lower()}{i}@example.com",
            "city": city,
            "country": country,
            "signup_date": signup_dt.strftime("%Y-%m-%d")
        }
        customers.append(cust)

    # Add 2 invalid dirty records for Data Quality testing (e.g. null email/name)
    customers.append({
        "customer_id": "CUST99999",
        "name": "",
        "email": "invalid_email_at_test.com",
        "city": "Unknown",
        "country": "USA",
        "signup_date": "2026-01-01"
    })

    return customers


def generate_products(num_records: int = 30) -> List[Dict[str, Any]]:
    """
    Generates synthetic product catalog across realistic categories.
    """
    categories = {
        "Electronics": [("Wireless Headphones", 149.99), ("Smartphone", 799.00), ("Laptop", 1299.50), ("Smart Watch", 199.99), ("4K Monitor", 349.00)],
        "Apparel": [("Cotton T-Shirt", 24.99), ("Denim Jeans", 59.99), ("Winter Jacket", 120.00), ("Running Shoes", 89.95), ("Leather Belt", 29.50)],
        "Home & Kitchen": [("Coffee Maker", 89.99), ("Air Fryer", 119.00), ("Blender", 49.99), ("Desk Lamp", 35.00), ("Vacuum Cleaner", 210.00)],
        "Books": [("Data Engineering Cookbook", 45.00), ("Designing Data-Intensive Applications", 55.00), ("Python Crash Course", 30.00), ("Spark in Action", 48.50)]
    }

    products = []
    prod_counter = 1

    for cat, items in categories.items():
        for pname, base_price in items:
            products.append({
                "product_id": f"PROD{prod_counter:04d}",
                "product_name": pname,
                "category": cat,
                "price": base_price
            })
            prod_counter += 1

    return products


def generate_orders_batch(batch_date_str: str, num_records: int = 150, max_cust_id: int = 100, max_prod_id: int = 19) -> List[Dict[str, Any]]:
    """
    Generates a daily batch of synthetic order transactions.
    Includes valid transactions as well as dirty/edge case records (null IDs, negative quantity, invalid prices, duplicate order IDs).
    """
    statuses = ["Completed", "Completed", "Completed", "Shipped", "Pending", "Cancelled"]
    orders = []

    for i in range(1, num_records + 1):
        cust_num = random.randint(1, max_cust_id)
        prod_num = random.randint(1, max_prod_id)
        qty = random.randint(1, 5)
        # Random timestamp during batch day
        dt = datetime.strptime(batch_date_str, "%Y-%m-%d") + timedelta(hours=random.randint(8, 20), minutes=random.randint(0, 59))
        price = random.choice([24.99, 59.99, 89.99, 149.99, 199.99, 799.00, 1299.50])

        order = {
            "order_id": f"ORD_{batch_date_str.replace('-', '')}_{i:04d}",
            "customer_id": f"CUST{cust_num:05d}",
            "product_id": f"PROD{prod_num:04d}",
            "order_date": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": qty,
            "price": price,
            "order_status": random.choice(statuses)
        }
        orders.append(order)

    # Introduce synthetic DIRTY data records into the batch to demonstrate Data Validation / Spark cleaning:
    # 1. Null Customer ID
    orders.append({
        "order_id": f"ORD_{batch_date_str.replace('-', '')}_DIRTY_01",
        "customer_id": "", # NULL customer ID
        "product_id": "PROD0001",
        "order_date": f"{batch_date_str} 10:00:00",
        "quantity": 2,
        "price": 149.99,
        "order_status": "Completed"
    })
    # 2. Negative quantity
    orders.append({
        "order_id": f"ORD_{batch_date_str.replace('-', '')}_DIRTY_02",
        "customer_id": "CUST00001",
        "product_id": "PROD0002",
        "order_date": f"{batch_date_str} 11:30:00",
        "quantity": -5, # Invalid quantity <= 0
        "price": 59.99,
        "order_status": "Completed"
    })
    # 3. Duplicate order ID
    if len(orders) > 0:
        dup_order = dict(orders[0])
        dup_order["quantity"] = 10 # Duplicate key check test
        orders.append(dup_order)

    return orders


def write_csv(filepath: str, data: List[Dict[str, Any]], fieldnames: List[str]):
    """
    Utility function to write a list of dictionaries to a CSV file.
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Generated raw CSV dataset: {filepath} ({len(data)} records)")


def generate_all_raw_data(base_dir: str = "data/raw", batch_dates: List[str] = None):
    """
    Generates all raw datasets: customers, products, and multi-day order partitions.
    """
    if batch_dates is None:
        batch_dates = ["2026-09-01", "2026-09-02", "2026-09-03"]

    logger.info("Generating raw datasets for E-Commerce Data Engineering Pipeline...")

    # 1. Customers
    cust_data = generate_customers(num_records=100)
    write_csv(
        os.path.join(base_dir, "customers", "customers.csv"),
        cust_data,
        ["customer_id", "name", "email", "city", "country", "signup_date"]
    )

    # 2. Products
    prod_data = generate_products()
    write_csv(
        os.path.join(base_dir, "products", "products.csv"),
        prod_data,
        ["product_id", "product_name", "category", "price"]
    )

    # 3. Orders (Partitioned by Date)
    for b_date in batch_dates:
        ord_data = generate_orders_batch(batch_date_str=b_date, num_records=120)
        write_csv(
            os.path.join(base_dir, "orders", b_date, "orders.csv"),
            ord_data,
            ["order_id", "customer_id", "product_id", "order_date", "quantity", "price", "order_status"]
        )

    logger.info("Raw data generation complete!")


if __name__ == "__main__":
    generate_all_raw_data()
