# =============================================================
# Retail CDC Pipeline — Central Logger
# =============================================================
# Single logger used across all ingestion scripts
# Writes to both console and a log file simultaneously
# =============================================================

import logging
import os
from datetime import datetime

# -------------------------------------------------------------
# Log file location
# Creates a logs/ folder in the project root
# One log file per day — keeps things organised
# -------------------------------------------------------------
LOG_DIR  = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{datetime.utcnow().strftime('%Y%m%d')}.log")


def get_logger(name: str) -> logging.Logger:
    # Create logs directory if it doesn't exist
    os.makedirs(LOG_DIR, exist_ok=True)

    # Get or create logger with the given name
    # Name tells you which script the log came from
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # -------------------------------------------------------------
    # Format — what each log line looks like
    # 2026-03-30 09:00:00 | INFO     | api_customers | Fetched 10 customers
    # -------------------------------------------------------------
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # -------------------------------------------------------------
    # Console handler — prints to terminal
    # -------------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # -------------------------------------------------------------
    # File handler — writes to logs/pipeline_YYYYMMDD.log
    # -------------------------------------------------------------
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger