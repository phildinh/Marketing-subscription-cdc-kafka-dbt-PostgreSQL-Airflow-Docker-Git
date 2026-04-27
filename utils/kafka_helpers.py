import os
import socket
from utils.logging_config import get_logger

logger = get_logger(__name__)


def check_kafka_reachable(
    host: str = None,
    port: int = None,
    timeout: int = 10,
) -> bool:
    """
    Returns True if Kafka broker is reachable, False otherwise.
    Used by health checks in Airflow DAG and experiments.
    """
    host = host or os.getenv("KAFKA_HOST", "localhost")
    port = port or int(os.getenv("KAFKA_PORT", 9092))

    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        logger.info(f"Kafka reachable at {host}:{port}")
        return True
    except Exception as e:
        logger.error(f"Kafka not reachable at {host}:{port} — {e}")
        return False


def get_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
