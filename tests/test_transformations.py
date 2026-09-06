import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.transformation.spark_transformations import (
    transform_orders, transform_customers, transform_products, CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA, ORDERS_SCHEMA
)

def test_transform_orders_calculation_and_filtering(spark):
    """
    Tests PySpark order transformation logic:
    - Verifies total_amount = quantity * price calculation
    - Verifies invalid/dirty records (null customer_id, quantity <= 0) are filtered out
    - Verifies duplicate order_id records are dropped
    """
    raw_data = [
        # Valid Order 1: qty 2 * price 50.0 = total 100.0
        ("ORD001", "CUST001", "PROD001", "2026-09-01 10:00:00", 2, 50.00, "Completed"),
        # Valid Order 2: qty 3 * price 20.0 = total 60.0
        ("ORD002", "CUST002", "PROD002", "2026-09-01 11:30:00", 3, 20.00, "Shipped"),
        # Dirty Record 1: Null customer_id (Should be filtered out)
        ("ORD003", None, "PROD001", "2026-09-01 12:00:00", 1, 50.00, "Completed"),
        # Dirty Record 2: Invalid quantity -5 (Should be filtered out)
        ("ORD004", "CUST003", "PROD001", "2026-09-01 13:00:00", -5, 50.00, "Completed"),
        # Dirty Record 3: Duplicate order_id of ORD001 (Should be dropped)
        ("ORD001", "CUST001", "PROD001", "2026-09-01 10:00:00", 10, 50.00, "Completed")
    ]

    raw_df = spark.createDataFrame(raw_data, schema=ORDERS_SCHEMA)
    transformed_df = transform_orders(raw_df)

    rows = transformed_df.collect()
    
    # Only ORD001 and ORD002 should survive filtering and deduplication
    assert len(rows) == 2
    
    ord1 = [r for r in rows if r["order_id"] == "ORD001"][0]
    assert ord1["total_amount"] == 100.00
    assert ord1["date_id"] == 20260901

    ord2 = [r for r in rows if r["order_id"] == "ORD002"][0]
    assert ord2["total_amount"] == 60.00


def test_transform_customers_deduplication(spark):
    """Tests deduplication and null handling on customer dataset."""
    raw_data = [
        ("CUST001", "John Smith", "john@example.com", "New York", "USA", "2026-01-01"),
        ("CUST001", "John Smith Duplicate", "john@example.com", "New York", "USA", "2026-01-01"),
        ("", "Invalid Cust", "test@example.com", "London", "UK", "2026-01-01")
    ]

    raw_df = spark.createDataFrame(raw_data, schema=CUSTOMERS_SCHEMA)
    transformed_df = transform_customers(raw_df)

    rows = transformed_df.collect()
    assert len(rows) == 1
    assert rows[0]["customer_id"] == "CUST001"


def test_transform_products_price_filter(spark):
    """Tests product filtering for non-positive prices."""
    raw_data = [
        ("PROD001", "Valid Laptop", "Electronics", 999.99),
        ("PROD002", "Zero Price Item", "Electronics", 0.0),
        ("PROD003", "Negative Price Item", "Electronics", -15.0)
    ]

    raw_df = spark.createDataFrame(raw_data, schema=PRODUCTS_SCHEMA)
    transformed_df = transform_products(raw_df)

    rows = transformed_df.collect()
    assert len(rows) == 1
    assert rows[0]["product_id"] == "PROD001"
    assert rows[0]["price"] == 999.99
