import os
from src.transformation.spark_transformations import get_spark_session, PRODUCTS_SCHEMA, transform_products
from src.utils.logger import logger

def run_transform_products(raw_path: str = "data/raw/products/products.csv", output_path: str = "data/processed/dim_product"):
    """
    Standalone script to read raw products CSV, execute PySpark transformation, 
    and save processed output as Parquet.
    """
    spark = get_spark_session("TransformProductsJob")
    logger.info(f"Reading raw products dataset from: {raw_path}")

    if not os.path.exists(raw_path):
        logger.error(f"File not found: {raw_path}")
        return

    raw_df = spark.read.option("header", "true").schema(PRODUCTS_SCHEMA).csv(raw_path)
    processed_df = transform_products(raw_df)

    logger.info(f"Writing transformed dim_product dataset to Parquet at: {output_path}")
    processed_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"dim_product processing complete. Total clean records: {processed_df.count()}")


if __name__ == "__main__":
    run_transform_products()
