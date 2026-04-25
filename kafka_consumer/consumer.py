# =============================================================
# consumer.py
# Reads CDC events from Kafka topics
# Parses JSON payload
# Writes to raw.cdc_events in analytical_db
# Uses manual offset commit for reliability
# =============================================================

import json
import logging
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# -------------------------------------------------------------
# Logging
# -------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# Topics to subscribe to
# One per OLTP source table
# -------------------------------------------------------------
TOPICS = [
    "cdc_subscription.public.users",
    "cdc_subscription.public.plans",
    "cdc_subscription.public.subscriptions",
    "cdc_subscription.public.payments",
    "cdc_subscription.public.events",
]


# =============================================================
# Database connection â€” analytical_db
# =============================================================
def get_analytical_connection():
    return psycopg2.connect(
        host=os.getenv("ANALYTICAL_DB_HOST", "localhost"),
        port=int(os.getenv("ANALYTICAL_DB_PORT", 5432)),
        dbname=os.getenv("ANALYTICAL_DB_NAME", "analytical_db"),
        user=os.getenv("ANALYTICAL_DB_USER", "postgres"),
        password=os.getenv("ANALYTICAL_DB_PASSWORD")
    )


# =============================================================
# Parse a raw Kafka message into a structured dict
# ready to insert into raw.cdc_events
# =============================================================
def parse_message(msg):
    """
    Takes a raw Kafka message and returns a clean dict.

    Debezium with the unwrap transform gives us a flat JSON
    payload with op, table, ts_ms at the top level.

    We separate those metadata fields from the actual row data
    and store them in structured columns.
    """
    try:
        # Decode the raw bytes into a Python dict
        payload = json.loads(msg.value.decode("utf-8"))

        # Handle Kafka Connect schema wrapper if present
        if "schema" in payload and "payload" in payload:
            payload = payload["payload"]

        op = payload.pop("__op", None) or payload.pop("op", None)
        source_table = payload.pop("__table", None) or payload.pop("table", None)
        ts_ms = payload.pop("__ts_ms", None) or payload.pop("ts_ms", None)

        # Whatever remains is the actual row data
        # On an INSERT â€” this is the full new row
        # On an UPDATE â€” this is the new state of the row
        # On a DELETE  â€” this will be mostly empty after unwrap
        after = payload if op != "d" else None
        before = None  # unwrap transform gives us current state only

        return {
            "topic":        msg.topic,
            "partition":    msg.partition,
            "offset_value": msg.offset,
            "op":           op,
            "source_table": source_table,
            "before":       json.dumps(before),
            "after":        json.dumps(after),
            "ts_ms":        ts_ms,
            "consumed_at":  datetime.now(),
        }

    except Exception as e:
        logger.error(f"Failed to parse message: {e} | raw: {msg.value}")
        return None


# =============================================================
# Write a batch of parsed events to raw.cdc_events
# Uses execute_values for efficient bulk insert
# =============================================================
def write_to_analytical(conn, events):
    """
    Inserts a list of parsed event dicts into raw.cdc_events.
    Uses execute_values to insert the whole batch in one query.
    """
    if not events:
        return

    rows = [
        (
            event["topic"],
            event["partition"],
            event["offset_value"],
            event["op"],
            event["source_table"],
            event["before"],
            event["after"],
            event["ts_ms"],
            event["consumed_at"],
        )
        for event in events
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO raw.cdc_events (
                topic,
                partition,
                offset_value,
                op,
                source_table,
                before,
                after,
                ts_ms,
                consumed_at
            ) VALUES %s
            """,
            rows
        )
        conn.commit()

    logger.info(f"Wrote {len(rows)} events to raw.cdc_events")


# =============================================================
# Main consumer loop
# =============================================================
def run():
    logger.info("Starting Kafka consumer")

    # ----------------------------------------------------------
    # Connect to analytical_db
    # ----------------------------------------------------------
    conn = get_analytical_connection()
    logger.info("Connected to analytical_db")

    # ----------------------------------------------------------
    # Create Kafka consumer
    #
    # enable_auto_commit=False â€” we commit manually after writing
    # to Postgres so we never lose an event if the write fails
    #
    # auto_offset_reset="earliest" â€” if this consumer has never
    # run before, start from the very first event in the topic
    #
    # group_id â€” Kafka uses this to track which offsets this
    # consumer group has processed. Same group_id = resume
    # from last committed offset on restart
    # ----------------------------------------------------------
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="cdc-analytical-consumer",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=None,  # we handle deserialization manually
        max_poll_records=100,     # process up to 100 messages per batch
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    logger.info(f"Subscribed to topics: {TOPICS}")

    # ----------------------------------------------------------
    # Main poll loop
    # ----------------------------------------------------------
    try:
        while True:
            # Poll Kafka for new messages
            # timeout_ms=1000 means wait up to 1 second for messages
            records = consumer.poll(timeout_ms=1000)

            if not records:
                logger.debug("No new messages â€” waiting")
                continue

            # records is a dict: {TopicPartition: [messages]}
            # We flatten it into a single list of parsed events
            parsed_events = []

            for topic_partition, messages in records.items():
                for msg in messages:
                    parsed = parse_message(msg)

                    if parsed:
                        logger.info(
                            f"Received: topic={msg.topic} "
                            f"partition={msg.partition} "
                            f"offset={msg.offset} "
                            f"op={parsed['op']}"
                        )
                        parsed_events.append(parsed)

            # Write the whole batch to analytical_db in one query
            write_to_analytical(conn, parsed_events)

            # Only commit offsets AFTER successful write to Postgres
            # If write_to_analytical raises an exception we never
            # reach this line â€” Kafka replays from last committed offset
            consumer.commit()
            logger.info(f"Committed offsets for {len(parsed_events)} events")

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")

    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer and DB connection closed cleanly")


# =============================================================
# Entry point
# =============================================================
if __name__ == "__main__":
    run()
