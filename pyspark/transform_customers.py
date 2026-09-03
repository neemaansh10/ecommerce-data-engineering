import os
from src.transformation.spark_transformations import get_spark_session, CUSTOMERS_SCHEMA, transform_customers
from src.utils.logger import logger

def run_transform_customers(raw_path: str = "data/raw/customers/customers.csv", output_path: str = "data/processed/dim_customer"):
    """
    Standalone script to read raw customers CSV, execute PySpark transformation, 
    and save processed output as Parquet.
    """
    spark = get_spark_session("TransformCustomersJob")
    logger.info(f"Reading raw customers dataset from: {raw_path}")

    if not os.path.exists(raw_path):
        logger.error(f"File not found: {raw_path}")
        return

    raw_df = spark.read.option("header", "true").schema(CUSTOMERS_SCHEMA).csv(raw_path)
    processed_df = transform_customers(raw_df)

    logger.info(f"Writing transformed dim_customer dataset to Parquet at: {output_path}")
    processed_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"dim_customer processing complete. Total clean records: {processed_df.count()}")


if __name__ == "__main__":
    run_transform_customers()
