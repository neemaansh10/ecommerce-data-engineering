import os
import yaml
from pathlib import Path
from typing import Any, Dict
from dotenv import load_dotenv

# Automatically load environment variables from .env if present
load_dotenv()

def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Loads YAML configuration file and merges with environment variables.
    """
    if config_path is None:
        base_dir = Path(__file__).resolve().parent.parent.parent
        config_path = base_dir / "config" / "config.yaml"

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Inject environment variables into config dictionary for easy access
    config["env"] = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "S3_BUCKET": os.getenv("S3_BUCKET", config.get("s3", {}).get("bucket_name", "")),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": int(os.getenv("POSTGRES_PORT", 5432)),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "ecommerce_dw"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }

    return config
