# =============================================================
# Retail CDC Pipeline — Base API Client
# =============================================================
# Shared HTTP client used by all API ingestion scripts
# Uses tenacity for production-grade retry logic
# =============================================================

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)
import logging
from ingestion.core.logger import get_logger

logger = get_logger("api_client")

# -------------------------------------------------------------
# FakeStoreAPI base URL
# All endpoints are built from this
# -------------------------------------------------------------
BASE_URL        = "https://fakestoreapi.com"
REQUEST_TIMEOUT = 10


# -------------------------------------------------------------
# Tenacity retry decorator
# Retries automatically on Timeout or ConnectionError
# Does NOT retry on HTTP errors like 404 or 500
#
# stop_after_attempt(3)        — give up after 3 tries
# wait_exponential(min=2, max=10) — wait 2s, then 4s, then 8s
#                                   between retries (exponential backoff)
# retry_if_exception_type      — only retry on these specific errors
# before_sleep_log             — logs a message before each retry
# after_log                    — logs a message after final attempt
# -------------------------------------------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError
    )),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    after=after_log(logger, logging.ERROR)
)
def fetch(endpoint: str) -> list:
    url = f"{BASE_URL}{endpoint}"
    logger.info(f"Calling {url}")

    response = requests.get(url, timeout=REQUEST_TIMEOUT)

    # -------------------------------------------------------------
    # raise_for_status() checks the HTTP status code
    # 4xx or 5xx raises an exception immediately
    # Tenacity does NOT retry these — they won't fix themselves
    # -------------------------------------------------------------
    response.raise_for_status()

    data = response.json()
    logger.info(f"Received {len(data)} records from {endpoint}")
    return data