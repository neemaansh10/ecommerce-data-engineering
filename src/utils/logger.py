import logging
import os
import sys
from datetime import datetime

def setup_logger(name: str = "ecommerce_pipeline", log_dir: str = "logs") -> logging.Logger:
    """
    Configures and returns a structured logger with both Stream (console) 
    and File handlers for production logging.
    """
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicating handlers if setup_logger is called multiple times
    if logger.hasHandlers():
        return logger

    # Log format: Timestamp - LogLevel - LoggerName - Message
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler
    log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(os.path.join(log_dir, log_filename))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

# Default logger instance
logger = setup_logger()
