import os
from pathlib import Path
from src.utils.logger import logger
from src.utils.config_loader import load_config

try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class S3Ingestor:
    """
    Ingestion helper to sync local raw datasets to an AWS S3 Data Lake bucket.
    Supports logical S3 key organization:
    s3://<bucket>/ecommerce/raw/customers/customers.csv
    s3://<bucket>/ecommerce/raw/products/products.csv
    s3://<bucket>/ecommerce/raw/orders/<batch_date>/orders.csv
    """

    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.env = self.config.get("env", {})
        self.bucket_name = self.env.get("S3_BUCKET") or self.config.get("s3", {}).get("bucket_name")
        self.aws_access_key = self.env.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = self.env.get("AWS_SECRET_ACCESS_KEY")
        self.region = self.env.get("AWS_REGION", "us-east-1")
        self.raw_prefix = self.config.get("s3", {}).get("raw_prefix", "ecommerce/raw")

        self.s3_client = None
        self._init_s3_client()

    def _init_s3_client(self):
        """Initializes boto3 S3 client if credentials are available."""
        if not BOTO3_AVAILABLE:
            logger.warning("boto3 package not installed. S3 upload functionality is disabled.")
            return

        if self.aws_access_key and self.aws_secret_key:
            try:
                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key,
                    region_name=self.region
                )
                logger.info(f"Initialized S3 Client for region {self.region}")
            except Exception as e:
                logger.warning(f"Failed to initialize S3 client: {e}")
        else:
            logger.info("No AWS Credentials provided in environment. Operating in LOCAL Data Lake mode.")

    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """
        Uploads a single file to AWS S3.
        """
        if not self.s3_client or not self.bucket_name:
            logger.info(f"[Local Fallback] Skipping S3 upload for {local_path} -> s3://{self.bucket_name}/{s3_key}")
            return False

        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded {local_path} to s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"S3 ClientError uploading {local_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading to S3: {e}")
            return False

    def sync_raw_data_to_s3(self, local_raw_dir: str = "data/raw"):
        """
        Walks local_raw_dir and uploads all raw CSVs to S3 with matching logical key structure.
        """
        raw_path = Path(local_raw_dir)
        if not raw_path.exists():
            logger.warning(f"Raw directory {local_raw_dir} does not exist. Nothing to upload.")
            return

        for file_path in raw_path.glob("**/*.csv"):
            rel_path = file_path.relative_to(raw_path)
            s3_key = f"{self.raw_prefix}/{rel_path.as_posix()}"
            self.upload_file(str(file_path), s3_key)


if __name__ == "__main__":
    ingestor = S3Ingestor()
    ingestor.sync_raw_data_to_s3()
