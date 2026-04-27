import logging
import os


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger with consistent format across all modules.
    Log level is controlled by LOG_LEVEL env var (default INFO).
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return logging.getLogger(name)
