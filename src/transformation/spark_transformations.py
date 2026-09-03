import os
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, expr, when, round as spark_round, to_date, to_timestamp,
    year, month, dayofmonth, quarter, date_format, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
)

from src.utils.logger import logger

def get_spark_session(app_name: str = "ECommercePipeline") -> SparkSession:
    """
    Creates or retrieves an active SparkSession with optimized local configuration.
    Raises a clean, informative error if Java Runtime (JDK/JRE) is missing.
    """
    try:
        return (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4") # Optimized for local mode execution
            .config("spark.driver.memory", "2g")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as e:
        logger.error(
            "\n" + "=" * 80 +
            "\n[MISSING JAVA RUNTIME DETECTED]" +
            "\nPySpark requires Java Runtime Environment (JRE/JDK 8, 11, or 17+) to launch its JVM Gateway." +
            "\nPlease install Java on your machine to execute PySpark transformations." +
            "\nFor macOS: brew install openjdk@17 (or download Java JDK from oracle/adoptium)." +
            "\n" + "=" * 80
        )
        raise e



# Define Explicit Schemas for Raw CSV files
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True)
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_status", StringType(), True)
])


def transform_customers(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms raw customer DataFrame for dim_customer table:
    1. Filter out records with null customer_id or empty customer_id
    2. Drop duplicate customer_id records (keeping first)
    3. Standardize signup_date to DateType
    4. Fill missing optional fields with default values
    """
    logger.info("Transforming Customer dataset...")
    
    clean_df = (
        df.filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
        .dropDuplicates(["customer_id"])
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("name", when(col("name").isNull() | (col("name") == ""), "Unknown").otherwise(col("name")))
        .withColumn("email", when(col("email").isNull(), "unspecified@example.com").otherwise(col("email")))
        .select(
            col("customer_id"),
            col("name"),
            col("email"),
            col("city"),
            col("country"),
            col("signup_date")
        )
    )
    return clean_df


def transform_products(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms raw product DataFrame for dim_product table:
    1. Filter null product_id or invalid price (<= 0)
    2. Drop duplicate product_id records
    3. Round price to 2 decimal places
    """
    logger.info("Transforming Product dataset...")
    
    clean_df = (
        df.filter(col("product_id").isNotNull() & (col("product_id") != ""))
        .filter(col("price").isNotNull() & (col("price") > 0))
        .dropDuplicates(["product_id"])
        .withColumn("price", spark_round(col("price"), 2))
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("price")
        )
    )
    return clean_df


def transform_orders(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms raw order transactions for fact_sales table:
    1. Clean invalid records: null customer_id, null product_id, quantity <= 0, price <= 0
    2. Deduplicate by order_id
    3. Convert order_date string to TimestampType
    4. Calculate total_amount = round(quantity * price, 2)
    5. Generate date_id formatted as integer YYYYMMDD for Star Schema dimension linking
    """
    logger.info("Transforming Orders dataset...")

    # Filter out dirty/invalid records
    filtered_df = df.filter(
        col("order_id").isNotNull() & (col("order_id") != "") &
        col("customer_id").isNotNull() & (col("customer_id") != "") &
        col("product_id").isNotNull() & (col("product_id") != "") &
        col("quantity").isNotNull() & (col("quantity") > 0) &
        col("price").isNotNull() & (col("price") > 0)
    ).dropDuplicates(["order_id"])

    # Transform timestamps & calculate totals
    transformed_df = (
        filtered_df
        .withColumn("order_timestamp", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("order_date_parsed", to_date(col("order_timestamp")))
        # Derive date_id as YYYYMMDD integer for dimension key join
        .withColumn("date_id", date_format(col("order_date_parsed"), "yyyyMMdd").cast(IntegerType()))
        # Business calculation rule: total_amount = quantity * price
        .withColumn("total_amount", spark_round(col("quantity") * col("price"), 2))
        .withColumn("order_status", when(col("order_status").isNull(), "Pending").otherwise(col("order_status")))
        .select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("date_id"),
            col("order_timestamp").alias("order_date"),
            col("quantity"),
            col("price").alias("unit_price"),
            col("total_amount"),
            col("order_status")
        )
    )
    return transformed_df


def generate_dim_date(spark: SparkSession, start_date: str = "2025-01-01", end_date: str = "2027-12-31") -> DataFrame:
    """
    Generates a comprehensive Date Dimension table (dim_date) for Star Schema analysis.
    """
    logger.info(f"Generating dim_date dimension from {start_date} to {end_date}...")
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    dates = []
    curr = start_dt
    while curr <= end_dt:
        dates.append((
            int(curr.strftime("%Y%m%d")),
            curr.strftime("%Y-%m-%d"),
            curr.day,
            curr.month,
            (curr.month - 1) // 3 + 1,
            curr.year,
            curr.strftime("%A")
        ))
        curr += timedelta(days=1)

    schema = StructType([
        StructField("date_id", IntegerType(), False),
        StructField("full_date", StringType(), False),
        StructField("day", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("day_of_week", StringType(), False)
    ])

    return spark.createDataFrame(dates, schema=schema)
