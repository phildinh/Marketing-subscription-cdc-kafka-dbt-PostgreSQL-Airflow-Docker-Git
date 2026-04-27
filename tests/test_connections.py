"""
Integration tests for database and Kafka connectivity.
Requires local PostgreSQL and Kafka to be running.
Run with: pytest tests/test_connections.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import psycopg2
from utils.db_connections import get_oltp_connection, get_analytical_connection
from utils.kafka_helpers import check_kafka_reachable


# ── PostgreSQL ────────────────────────────────────────────────────────────────

class TestOltpConnection:

    def test_connects_successfully(self):
        conn = get_oltp_connection()
        assert conn is not None
        conn.close()

    def test_required_tables_exist(self):
        conn = get_oltp_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('users', 'plans', 'subscriptions', 'payments', 'events')
            """)
            tables = {row[0] for row in cur.fetchall()}
        conn.close()
        assert tables == {"users", "plans", "subscriptions", "payments", "events"}

    def test_wal_level_is_logical(self):
        """Debezium requires wal_level=logical to function."""
        conn = get_oltp_connection()
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level;")
            wal_level = cur.fetchone()[0]
        conn.close()
        assert wal_level == "logical", (
            f"wal_level is '{wal_level}' — must be 'logical' for Debezium. "
            "Run: ALTER SYSTEM SET wal_level = logical; then restart PostgreSQL."
        )


class TestAnalyticalConnection:

    def test_connects_successfully(self):
        conn = get_analytical_connection()
        assert conn is not None
        conn.close()

    def test_required_schemas_exist(self):
        conn = get_analytical_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name IN ('raw', 'staging', 'marts', 'snapshots')
            """)
            schemas = {row[0] for row in cur.fetchall()}
        conn.close()
        assert "raw" in schemas, "raw schema missing — run sql/init_analytical.sql"
        assert "staging" in schemas, "staging schema missing"
        assert "marts" in schemas, "marts schema missing"

    def test_cdc_events_table_exists(self):
        conn = get_analytical_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'cdc_events'
            """)
            columns = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {"id", "topic", "partition", "offset_value", "op", "source_table", "before", "after", "ts_ms", "consumed_at"}
        assert expected.issubset(columns), f"Missing columns: {expected - columns}"


# ── Kafka ─────────────────────────────────────────────────────────────────────

class TestKafkaConnection:

    def test_kafka_reachable(self):
        assert check_kafka_reachable(), "Kafka not reachable — is Docker running?"
