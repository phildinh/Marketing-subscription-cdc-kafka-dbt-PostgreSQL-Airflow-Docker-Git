# =============================================================
# cdc_pipeline_dag.py
# Orchestrates the dbt transformation layer
# Runs every 30 minutes
# Checks Kafka and Debezium health before transforming
# Sends Slack alerts on success and failure
# =============================================================

import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# -------------------------------------------------------------
# Default arguments
# Applied to every task unless overridden
# -------------------------------------------------------------
default_args = {
    "owner": "phil",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------
with DAG(
    dag_id="cdc_pipeline",
    description="CDC pipeline — health checks, dbt run, dbt test, Slack alerts",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["cdc", "dbt", "kafka", "debezium"],
) as dag:

    # ==========================================================
    # TASK 1 — Check Kafka health
    # Uses Docker service name 'kafka', not localhost
    # ==========================================================
    def check_kafka():
        import socket
        host = os.getenv("KAFKA_HOST", "kafka")
        port = int(os.getenv("KAFKA_PORT", 9092))

        try:
            sock = socket.create_connection((host, port), timeout=10)
            sock.close()
            print(f"Kafka is reachable at {host}:{port}")
        except Exception as e:
            raise Exception(f"Kafka health check failed: {e}")

    check_kafka_health = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka,
    )

    # ==========================================================
    # TASK 2 — Check Debezium health
    # ==========================================================
    def check_debezium():
        url = os.getenv(
            "DEBEZIUM_URL",
            "http://debezium:8083/connectors/cdc-subscription-connector/status"
        )

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            status = response.json()

            connector_state = status["connector"]["state"]
            task_state = status["tasks"][0]["state"]

            print(f"Connector state: {connector_state}")
            print(f"Task state: {task_state}")

            if connector_state != "RUNNING" or task_state != "RUNNING":
                raise Exception(
                    f"Debezium not healthy — "
                    f"connector: {connector_state}, task: {task_state}"
                )

            print("Debezium is healthy")

        except Exception as e:
            raise Exception(f"Debezium health check failed: {e}")

    check_debezium_health = PythonOperator(
        task_id="check_debezium_health",
        python_callable=check_debezium,
    )

    # ==========================================================
    # TASK 3 — dbt run staging
    # append_env=True keeps PATH and other system vars intact
    # ==========================================================
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --select staging",
        append_env=True,
    )

    # ==========================================================
    # TASK 4 — dbt run marts
    # ==========================================================
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --select marts",
        append_env=True,
    )

    # ==========================================================
    # TASK 5 — dbt snapshot
    # ==========================================================
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt && dbt snapshot",
        append_env=True,
    )

    # ==========================================================
    # TASK 6 — dbt test
    # ==========================================================
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test",
        append_env=True,
    )

    # ==========================================================
    # TASK 7 — Slack success alert
    # ==========================================================
    def notify_success(**context):
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            print("No Slack webhook configured — skipping")
            return

        run_id = context["run_id"]
        execution_date = context["execution_date"]

        payload = {
            "text": (
                f":white_check_mark: *CDC Pipeline succeeded*\n"
                f"Run: `{run_id}`\n"
                f"Time: `{execution_date}`"
            )
        }

        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Slack success alert sent")

    slack_success = PythonOperator(
        task_id="slack_notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ==========================================================
    # TASK 8 — Slack failure alert
    # ==========================================================
    def notify_failure(**context):
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            print("No Slack webhook configured — skipping")
            return

        run_id = context["run_id"]
        execution_date = context["execution_date"]
        task_id = context["task_instance"].task_id

        payload = {
            "text": (
                f":red_circle: *CDC Pipeline failed*\n"
                f"Failed task: `{task_id}`\n"
                f"Run: `{run_id}`\n"
                f"Time: `{execution_date}`"
            )
        }

        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Slack failure alert sent")

    slack_failure = PythonOperator(
        task_id="slack_notify_failure",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ==========================================================
    # Task dependencies
    # ==========================================================
    check_kafka_health >> check_debezium_health
    check_debezium_health >> dbt_run_staging
    dbt_run_staging >> dbt_run_marts
    dbt_run_marts >> dbt_snapshot
    dbt_snapshot >> dbt_test
    dbt_test >> [slack_success, slack_failure]
