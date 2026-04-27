from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
import logging

logger = logging.getLogger(__name__)


def db_retry(func):
    """
    Retry decorator for database operations.
    Retries up to 3 times with exponential backoff (2s, 4s, 8s).
    Useful when wrapping flaky connections in experiments.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def kafka_retry(func):
    """
    Retry decorator for Kafka operations.
    Retries up to 5 times with exponential backoff (1s, 2s, 4s, 8s, 16s).
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)
