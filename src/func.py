import os
import datetime
from datetime import timezone
from logging_setup import get_logger

logger = get_logger('func')

def get_utc_timestamp():
    test_mode = os.getenv("PREDICTIVESITEOUTAGE_TEST_MODE", "false").lower() == "true"

    if test_mode:
        utc_timestamp = 1719705540
        logger.info("Test mode enabled. Using predefined UTC timestamp.")
    else:
        dt = datetime.datetime.now(timezone.utc)
        utc_time = dt.replace(tzinfo=timezone.utc)
        utc_timestamp = round(utc_time.timestamp())
        logger.info("Test mode disabled. Using current UTC timestamp.")

    rounded_utc_timestamp = utc_timestamp
    while rounded_utc_timestamp % 300 != 0:
        rounded_utc_timestamp -= 1

    logger.info(f"Rounded UTC timestamp: {rounded_utc_timestamp}")
    return rounded_utc_timestamp
