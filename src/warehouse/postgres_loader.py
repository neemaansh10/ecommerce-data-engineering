import os
from pathlib import Path
import pandas as pd
from typing import Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from src.utils.logger import logger
from src.utils.config_loader import load_config


class PostgresWarehouseLoader:
    """
    Data Warehouse loader class that ingests PySpark Parquet datasets 
    into PostgreSQL Star Schema tables.
    Includes fallback capabilities for local SQLite demo database if PostgreSQL server is not running.
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or load_config()
        self.env = self.config.get("env", {})

        self.host = self.env.get("POSTGRES_HOST", "localhost")
        self.port = self.env.get("POSTGRES_PORT", 5432)
        self.db = self.env.get("POSTGRES_DB", "ecommerce_dw")
        self.user = self.env.get("POSTGRES_USER", "postgres")
        self.password = self.env.get("POSTGRES_PASSWORD", "postgres")

        self.engine = None
        self.is_fallback_sqlite = False
        self._init_db_connection()

    def _init_db_connection(self):
        """Initializes database engine connection."""
        connection_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        try:
            engine = create_engine(connection_url, connect_args={"connect_timeout": 3})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.engine = engine
            logger.info(f"Connected successfully to PostgreSQL Data Warehouse at {self.host}:{self.port}/{self.db}")
        except Exception as e:
            logger.warning(f"Could not connect to PostgreSQL ({e}). Using local SQLite fallback data warehouse for local testing.")
            # Fallback to local SQLite database in data/warehouse.db
            os.makedirs("data", exist_ok=True)
            self.engine = create_engine("sqlite:///data/warehouse.db")
            self.is_fallback_sqlite = True
            logger.info("Initialized local SQLite Data Warehouse engine at data/warehouse.db")

    def execute_ddl_script(self, ddl_path: str = "sql/create_tables.sql"):
        """Executes table creation DDL SQL script."""
        path = Path(ddl_path)
        if not path.exists():
            logger.error(f"DDL script file not found at: {ddl_path}")
            return

        logger.info(f"Executing DDL script from {ddl_path}...")
        with open(path, "r") as f:
            sql_script = f.read()

        if self.is_fallback_sqlite:
            # Adjust PostgreSQL NUMERIC/VARCHAR syntax for SQLite compatibility if in fallback mode
            sql_script = sql_script.replace("NUMERIC(10, 2)", "REAL")
            sql_script = sql_script.replace("NUMERIC(12, 2)", "REAL")
            sql_script = sql_script.replace("TIMESTAMP", "TEXT")
            sql_script = sql_script.replace("DATE", "TEXT")

        try:
            with self.engine.connect() as conn:
                for statement in sql_script.split(";"):
                    stmt = statement.strip()
                    if stmt:
                        conn.execute(text(stmt))
                conn.commit()
            logger.info("Successfully executed DDL table creation script.")
        except Exception as e:
            logger.error(f"Error executing DDL script: {e}")

    def load_parquet_to_table(self, parquet_path: str, table_name: str, if_exists: str = "append") -> int:
        """
        Reads processed Parquet file and loads records into the target Data Warehouse table.
        """
        p_path = Path(parquet_path)
        if not p_path.exists():
            logger.warning(f"Parquet directory not found: {parquet_path}. Skipping load for table '{table_name}'.")
            return 0

        try:
            logger.info(f"Loading Parquet dataset from {parquet_path} into table '{table_name}'...")
            df = pd.read_parquet(parquet_path)
            
            if df.empty:
                logger.warning(f"Parquet dataset at {parquet_path} is empty.")
                return 0

            # Write DataFrame to SQL table
            df.to_sql(table_name, con=self.engine, if_exists=if_exists, index=False, method="multi", chunksize=1000)
            rec_count = len(df)
            logger.info(f"Successfully loaded {rec_count} records into '{table_name}'.")
            return rec_count
        except Exception as e:
            logger.error(f"Failed to load Parquet data to table '{table_name}': {e}")
            return 0

    def load_all_processed_data(self, processed_dir: str = "data/processed"):
        """Loads all transformed datasets into PostgreSQL Star Schema tables."""
        self.execute_ddl_script("sql/create_tables.sql")

        # Load dimensions first, then fact table
        self.load_parquet_to_table(os.path.join(processed_dir, "dim_customer"), "dim_customer", if_exists="append")
        self.load_parquet_to_table(os.path.join(processed_dir, "dim_product"), "dim_product", if_exists="append")
        self.load_parquet_to_table(os.path.join(processed_dir, "dim_date"), "dim_date", if_exists="append")
        self.load_parquet_to_table(os.path.join(processed_dir, "fact_sales"), "fact_sales", if_exists="append")

        logger.info("Data Warehouse loading process completed!")


if __name__ == "__main__":
    loader = PostgresWarehouseLoader()
    loader.load_all_processed_data()
