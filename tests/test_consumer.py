"""
Tests for kafka_consumer/consumer.py
Focuses on parse_message — the pure transformation logic.
No Kafka broker or database required.
"""
import json
import pytest
from unittest.mock import MagicMock


# ── helpers ──────────────────────────────────────────────────────────────────

def make_message(payload: dict, topic="cdc_subscription.public.users", partition=0, offset=1):
    """Creates a mock Kafka message with the given payload."""
    msg = MagicMock()
    msg.value = json.dumps(payload).encode("utf-8")
    msg.topic = topic
    msg.partition = partition
    msg.offset = offset
    return msg


# Import after helpers so we can patch if needed
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from kafka_consumer.consumer import parse_message


# ── tests ─────────────────────────────────────────────────────────────────────

class TestParseMessage:

    def test_flat_insert_message(self):
        """Standard flat CDC message — op, table, ts_ms at top level."""
        payload = {
            "user_id": 1,
            "email": "phil@test.com",
            "name": "Phil Dinh",
            "plan_id": 1,
            "created_at": 1767225600000000,
            "op": "c",
            "table": "users",
            "ts_ms": 1745580000000,
        }
        msg = make_message(payload)
        result = parse_message(msg)

        assert result is not None
        assert result["op"] == "c"
        assert result["source_table"] == "users"
        assert result["ts_ms"] == 1745580000000
        assert result["topic"] == "cdc_subscription.public.users"
        assert result["offset_value"] == 1

    def test_schema_wrapper_message(self):
        """Kafka Connect schema wrapper — payload nested under 'payload' key."""
        wrapped = {
            "schema": {"type": "struct", "name": "cdc_subscription.public.users.Value"},
            "payload": {
                "user_id": 1,
                "email": "phil@test.com",
                "op": "c",
                "table": "users",
                "ts_ms": 1745580000000,
            }
        }
        msg = make_message(wrapped)
        result = parse_message(msg)

        assert result is not None
        assert result["op"] == "c"
        assert result["source_table"] == "users"

    def test_double_underscore_prefix(self):
        """Debezium default prefix __ on add.fields."""
        payload = {
            "user_id": 1,
            "__op": "u",
            "__table": "users",
            "__ts_ms": 1745580000000,
        }
        msg = make_message(payload)
        result = parse_message(msg)

        assert result is not None
        assert result["op"] == "u"
        assert result["source_table"] == "users"

    def test_delete_message(self):
        """Delete event — after should be None."""
        payload = {
            "user_id": 1,
            "op": "d",
            "table": "users",
            "ts_ms": 1745580000000,
            "__deleted": "true",
        }
        msg = make_message(payload)
        result = parse_message(msg)

        assert result is not None
        assert result["op"] == "d"
        assert json.loads(result["after"]) is None

    def test_snapshot_read_message(self):
        """Debezium initial snapshot — op=r."""
        payload = {
            "user_id": 1,
            "op": "r",
            "table": "users",
            "ts_ms": 1745580000000,
        }
        msg = make_message(payload)
        result = parse_message(msg)

        assert result is not None
        assert result["op"] == "r"

    def test_invalid_json_returns_none(self):
        """Malformed message should return None without raising."""
        msg = MagicMock()
        msg.value = b"not valid json {"
        msg.topic = "cdc_subscription.public.users"
        msg.partition = 0
        msg.offset = 1

        result = parse_message(msg)
        assert result is None

    def test_after_contains_row_data(self):
        """after column should contain the row data minus CDC metadata fields."""
        payload = {
            "user_id": 42,
            "email": "test@test.com",
            "op": "c",
            "table": "users",
            "ts_ms": 1745580000000,
        }
        msg = make_message(payload)
        result = parse_message(msg)

        after = json.loads(result["after"])
        assert after["user_id"] == 42
        assert after["email"] == "test@test.com"
        assert "op" not in after
        assert "table" not in after
        assert "ts_ms" not in after
