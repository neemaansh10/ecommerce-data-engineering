import os
import sys
from pathlib import Path
from src.transformation.spark_transformations import get_spark_session, ORDERS_SCHEMA, transform_orders, generate_dim_date
from src.utils.logger import logger

def run_transform_orders(raw_orders_dir: str = "data/raw/orders", output_fact_path: str = "data/processed/fact_sales", output_date_dim_path: str = "data/processed/dim_date", batch_date: str = None):
    """
    Standalone script to transform order transaction batches into fact_sales Parquet dataset.
    Supports INCREMENTAL LOADING by accepting a specific batch_date (e.g., '2026-09-01').
    """
    spark = get_spark_session("TransformOrdersJob")

    if batch_date:
        input_path = os.path.join(raw_orders_dir, batch_date, "orders.csv")
        logger.info(f"Processing INCREMENTAL batch for date: {batch_date} at path: {input_path}")
    else:
        # Process all available partitions
        input_path = os.path.join(raw_orders_dir, "*", "orders.csv")
        logger.info(f"Processing ALL batches under: {input_path}")

    if not list(Path(raw_orders_dir).glob("**/*.csv")):
        logger.error(f"No raw order CSV files found under {raw_orders_dir}")
        return

    raw_df = spark.read.option("header", "true").schema(ORDERS_SCHEMA).csv(input_path)
    processed_df = transform_orders(raw_df)

    logger.info(f"Writing fact_sales dataset to Parquet at: {output_fact_path}")
    # Save partitioned by date_id for efficient analytical queries
    processed_df.write.mode("append" if batch_date else "overwrite").partitionBy("date_id").parquet(output_fact_path)
    logger.info(f"fact_sales transformation complete. Transformed clean order records: {processed_df.count()}")

    # Ensure dim_date table is also updated/generated
    if not os.path.exists(output_date_dim_path):
        dim_date_df = generate_dim_date(spark)
        logger.info(f"Writing dim_date dimension table to Parquet at: {output_date_dim_path}")
        dim_date_df.write.mode("overwrite").parquet(output_date_dim_path)


if __name__ == "__main__":
    b_date = sys.argv[1] if len(sys.argv) > 1 else None
    run_transform_orders(batch_date=b_date)
