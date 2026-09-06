import os
import argparse
import sys
from pathlib import Path

from src.utils.logger import logger
from src.utils.config_loader import load_config
from src.ingestion.data_generator import generate_all_raw_data
from src.ingestion.s3_ingestor import S3Ingestor
from src.transformation.spark_transformations import (
    get_spark_session, CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA, ORDERS_SCHEMA,
    transform_customers, transform_products, transform_orders, generate_dim_date
)
from src.validation.data_validator import DataValidator
from src.warehouse.postgres_loader import PostgresWarehouseLoader


def run_pipeline(generate_data: bool = False, batch_date: str = None, sync_s3: bool = False):
    """
    Main orchestrator function for the E-Commerce Data Engineering Pipeline.
    Executes:
    1. Data Generation (Optional)
    2. S3 Data Lake Ingestion (Optional/Fallback)
    3. PySpark Transformations & Parquet Conversion
    4. Data Quality Validation Checks
    5. PostgreSQL Data Warehouse Star Schema Loading
    """
    logger.info("================================================================================")
    logger.info("           STARTING E-COMMERCE DATA ENGINEERING PIPELINE EXECUTION              ")
    logger.info("================================================================================")

    config = load_config()
    raw_dir = config.get("paths", {}).get("raw_data_dir", "data/raw")
    processed_dir = config.get("paths", {}).get("processed_data_dir", "data/processed")

    # Step 1: Generate Synthetic Raw Data if requested or missing
    if generate_data or not os.path.exists(raw_dir):
        logger.info("Step 1: Generating Raw CSV Datasets...")
        generate_all_raw_data(base_dir=raw_dir)

    # Step 2: Ingest Raw Data to AWS S3 (If credentials present)
    if sync_s3:
        logger.info("Step 2: Syncing Raw CSV Data to AWS S3 Bucket...")
        s3_ingestor = S3Ingestor(config)
        s3_ingestor.sync_raw_data_to_s3(local_raw_dir=raw_dir)

    # Step 3: Initialize PySpark Session
    logger.info("Step 3: Initializing PySpark Transformation Engine...")
    spark = get_spark_session("ECommercePipelineRunner")

    # Transform Customer Dimension
    cust_raw_path = os.path.join(raw_dir, "customers", "customers.csv")
    cust_out_path = os.path.join(processed_dir, "dim_customer")
    if os.path.exists(cust_raw_path):
        raw_cust_df = spark.read.option("header", "true").schema(CUSTOMERS_SCHEMA).csv(cust_raw_path)
        clean_cust_df = transform_customers(raw_cust_df)
        clean_cust_df.write.mode("overwrite").parquet(cust_out_path)
        logger.info(f"Saved dim_customer Parquet dataset ({clean_cust_df.count()} records).")

    # Transform Product Dimension
    prod_raw_path = os.path.join(raw_dir, "products", "products.csv")
    prod_out_path = os.path.join(processed_dir, "dim_product")
    if os.path.exists(prod_raw_path):
        raw_prod_df = spark.read.option("header", "true").schema(PRODUCTS_SCHEMA).csv(prod_raw_path)
        clean_prod_df = transform_products(raw_prod_df)
        clean_prod_df.write.mode("overwrite").parquet(prod_out_path)
        logger.info(f"Saved dim_product Parquet dataset ({clean_prod_df.count()} records).")

    # Transform Date Dimension
    date_out_path = os.path.join(processed_dir, "dim_date")
    clean_date_df = generate_dim_date(spark, "2025-01-01", "2027-12-31")
    clean_date_df.write.mode("overwrite").parquet(date_out_path)

    # Transform Orders Fact Table (Supports Incremental Batch Date)
    orders_out_path = os.path.join(processed_dir, "fact_sales")
    if batch_date:
        ord_raw_path = os.path.join(raw_dir, "orders", batch_date, "orders.csv")
        logger.info(f"Step 3.1: Running INCREMENTAL batch transformation for date {batch_date} from {ord_raw_path}")
        write_mode = "append"
    else:
        ord_raw_path = os.path.join(raw_dir, "orders", "*", "orders.csv")
        logger.info(f"Step 3.1: Running FULL batch transformation for all partitions under {ord_raw_path}")
        write_mode = "overwrite"

    raw_ord_df = spark.read.option("header", "true").schema(ORDERS_SCHEMA).csv(ord_raw_path)
    clean_ord_df = transform_orders(raw_ord_df)
    clean_ord_df.write.mode(write_mode).partitionBy("date_id").parquet(orders_out_path)
    logger.info(f"Saved fact_sales Parquet dataset ({clean_ord_df.count()} records).")

    # Step 4: Run Data Quality & Validation Checks
    logger.info("Step 4: Executing Data Quality & Validation Checks...")
    validator = DataValidator()
    
    # Read back processed datasets for DQ verification
    proc_cust = spark.read.parquet(cust_out_path)
    proc_prod = spark.read.parquet(prod_out_path)
    proc_fact = spark.read.parquet(orders_out_path)

    validator.check_unique_primary_key(proc_cust, "customer_id", "dim_customer")
    validator.check_unique_primary_key(proc_prod, "product_id", "dim_product")
    validator.check_unique_primary_key(proc_fact, "order_id", "fact_sales")

    validator.check_non_null_columns(proc_fact, ["order_id", "customer_id", "product_id", "date_id", "total_amount"], "fact_sales")
    validator.check_numeric_bounds(proc_fact, "quantity", 0, "fact_sales")
    validator.check_numeric_bounds(proc_fact, "total_amount", 0.0, "fact_sales")

    validator.check_referential_integrity(proc_fact, proc_cust, "customer_id", "customer_id", "fact_sales", "dim_customer")
    validator.check_referential_integrity(proc_fact, proc_prod, "product_id", "product_id", "fact_sales", "dim_product")

    validator.print_validation_report()

    # Step 5: Load Transformed Parquet Datasets into PostgreSQL Data Warehouse
    logger.info("Step 5: Ingesting Processed Parquet Data into PostgreSQL Star Schema...")
    loader = PostgresWarehouseLoader(config)
    loader.load_all_processed_data(processed_dir=processed_dir)

    logger.info("================================================================================")
    logger.info("         E-COMMERCE DATA ENGINEERING PIPELINE COMPLETED SUCCESSFULLY!           ")
    logger.info("================================================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Data Engineering Pipeline Runner")
    parser.add_argument("--generate-data", action="store_true", help="Generate synthetic raw datasets before running pipeline")
    parser.add_argument("--batch-date", type=str, default=None, help="Process specific order batch date (e.g. 2026-09-01) for incremental loading")
    parser.add_argument("--sync-s3", action="store_true", help="Sync raw CSV files to AWS S3 bucket if credentials are configured")

    args = parser.parse_args()
    run_pipeline(generate_data=args.generate_data, batch_date=args.batch_date, sync_s3=args.sync_s3)
