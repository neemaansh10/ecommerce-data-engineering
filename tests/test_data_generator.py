import pytest
from src.ingestion.data_generator import generate_customers, generate_products, generate_orders_batch

def test_generate_customers():
    customers = generate_customers(num_records=50)
    assert len(customers) >= 50
    assert "customer_id" in customers[0]
    assert "email" in customers[0]

def test_generate_products():
    products = generate_products()
    assert len(products) > 0
    assert "product_id" in products[0]
    assert "price" in products[0]
    assert isinstance(products[0]["price"], (int, float))

def test_generate_orders_batch():
    orders = generate_orders_batch("2026-09-01", num_records=30)
    assert len(orders) >= 30
    assert "order_id" in orders[0]
    assert "quantity" in orders[0]
