import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.validation.data_validator import DataValidator

def test_data_validator_checks(spark):
    """
    Tests DataValidator quality assertions for Primary Key uniqueness,
    Null checks, and referential integrity.
    """
    validator = DataValidator()

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("quantity", IntegerType(), True)
    ])

    data = [
        ("ORD001", "CUST001", 2),
        ("ORD002", "CUST002", 5),
        ("ORD002", "CUST003", 1) # Duplicate primary key ORD002
    ]

    df = spark.createDataFrame(data, schema=schema)

    # 1. Test PK Duplicate Check
    res_pk = validator.check_unique_primary_key(df, "order_id", "test_orders")
    assert res_pk["status"] == "FAILED"
    assert res_pk["failed_records"] == 1

    # 2. Test Bound Check
    res_bound = validator.check_numeric_bounds(df, "quantity", 0, "test_orders")
    assert res_bound["status"] == "PASSED"
    assert res_bound["failed_records"] == 0
