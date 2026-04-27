# =============================================================
# alerts.py
# Centralised Slack alerting module
# Called by Airflow DAG and the Kafka consumer
# =============================================================

import os
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# Core send function
# All other functions call this
# -------------------------------------------------------------
def send_slack_message(text: str, emoji: str = ""):
    """
    Sends a message to Slack via webhook.
    Silently skips if SLACK_WEBHOOK_URL is not configured.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping alert")
        return

    payload = {"text": f"{emoji} {text}".strip()}

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Slack alert sent: {text[:50]}")
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")


# =============================================================
# Pipeline lifecycle alerts
# Called from Airflow DAG callbacks
# =============================================================

def alert_pipeline_started(dag_id: str, execution_date: str):
    send_slack_message(
        text=(
            f"*CDC Pipeline started*\n"
            f"DAG: `{dag_id}`\n"
            f"Time: `{execution_date}`"
        ),
        emoji=":large_blue_circle:"
    )


def alert_pipeline_succeeded(dag_id: str, execution_date: str, duration_seconds: float):
    send_slack_message(
        text=(
            f"*CDC Pipeline succeeded*\n"
            f"DAG: `{dag_id}`\n"
            f"Time: `{execution_date}`\n"
            f"Duration: `{round(duration_seconds, 1)}s`"
        ),
        emoji=":white_check_mark:"
    )


def alert_pipeline_failed(dag_id: str, task_id: str, execution_date: str):
    send_slack_message(
        text=(
            f"*CDC Pipeline failed*\n"
            f"DAG: `{dag_id}`\n"
            f"Failed task: `{task_id}`\n"
            f"Time: `{execution_date}`\n"
            f"Action: Check Airflow logs immediately"
        ),
        emoji=":red_circle:"
    )


# =============================================================
# Data quality alerts
# Called from dbt test results or consumer
# =============================================================

def alert_dbt_tests_passed(test_count: int):
    send_slack_message(
        text=(
            f"*dbt tests passed*\n"
            f"Tests run: `{test_count}`\n"
            f"All data quality checks green"
        ),
        emoji=":large_green_circle:"
    )


def alert_dbt_tests_failed(failed_tests: list):
    failed_list = "\n".join([f"  - `{t}`" for t in failed_tests])
    send_slack_message(
        text=(
            f"*dbt tests failed*\n"
            f"Failed tests:\n{failed_list}\n"
            f"Action: Check analytical_db for data quality issues"
        ),
        emoji=":warning:"
    )


# =============================================================
# Schema change alert
# Called from consumer when an unexpected column appears
# =============================================================

def alert_schema_change(table: str, new_columns: list):
    columns_list = ", ".join([f"`{c}`" for c in new_columns])
    send_slack_message(
        text=(
            f"*Schema change detected*\n"
            f"Table: `{table}`\n"
            f"New columns: {columns_list}\n"
            f"Action: Review Debezium connector and dbt staging models"
        ),
        emoji=":warning:"
    )


# =============================================================
# No events alert
# Called when consumer detects silence for 30+ minutes
# =============================================================

def alert_no_events(minutes_silent: int):
    send_slack_message(
        text=(
            f"*No CDC events received*\n"
            f"Silent for: `{minutes_silent} minutes`\n"
            f"Action: Check Debezium connector and Kafka consumer"
        ),
        emoji=":double_vertical_bar:"
    )


# =============================================================
# Consumer stats alert
# Called after each successful consumer batch
# =============================================================

def alert_consumer_stats(events_processed: int, tables: dict):
    table_breakdown = "\n".join(
        [f"  - `{table}`: {count} events" for table, count in tables.items()]
    )
    send_slack_message(
        text=(
            f"*Consumer batch complete*\n"
            f"Total events: `{events_processed}`\n"
            f"Breakdown:\n{table_breakdown}"
        ),
        emoji=":inbox_tray:"
    )