# =============================================================
# generator.py
# Generates synthetic subscription data and inserts into oltp_db
# Simulates realistic business events: signups, upgrades,
# cancellations, payments, and clickstream activity
# =============================================================

import os
import random
import time
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import faker
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from schemas import (
    PLANS,
    EVENT_TYPES,
    PAYMENT_STATUSES,
    EVENT_PROPERTIES,
    User,
    Subscription,
    Payment,
    Event,
)

# -------------------------------------------------------------
# Logging
# -------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# Faker instance
# Generates realistic names, emails
# -------------------------------------------------------------
fake = faker.Faker()


# =============================================================
# Database connection
# =============================================================
def get_connection():
    return psycopg2.connect(
        host=os.getenv("OLTP_DB_HOST", "localhost"),
        port=int(os.getenv("OLTP_DB_PORT", 5432)),
        dbname=os.getenv("OLTP_DB_NAME"),
        user=os.getenv("OLTP_DB_USER"),
        password=os.getenv("OLTP_DB_PASSWORD")
    )


# =============================================================
# Seed plans
# Inserts the fixed plan tiers once at startup
# Only inserts if plans table is empty
# =============================================================
def seed_plans(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM plans;")
        count = cur.fetchone()[0]

        if count > 0:
            logger.info(f"Plans already seeded ({count} rows) — skipping")
            return

        for plan in PLANS:
            cur.execute(
                """
                INSERT INTO plans (name, price, billing_cycle)
                VALUES (%s, %s, %s)
                """,
                (plan["name"], plan["price"], plan["billing_cycle"])
            )

        conn.commit()
        logger.info(f"Seeded {len(PLANS)} plans")


# =============================================================
# Get all plan IDs from the database
# =============================================================
def get_plan_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT plan_id, price FROM plans;")
        return cur.fetchall()  # list of (plan_id, price) tuples


# =============================================================
# Create a new user
# =============================================================
def create_user(conn, plan_ids):
    plan_id, _ = random.choice(plan_ids)
    user = User(
        email=fake.unique.email(),
        name=fake.name(),
        plan_id=plan_id,
        created_at=datetime.now()
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (email, name, plan_id, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING user_id
            """,
            (user.email, user.name, user.plan_id, user.created_at)
        )
        user_id = cur.fetchone()[0]
        conn.commit()

    logger.info(f"Created user {user_id} — {user.email}")
    return user_id, plan_id


# =============================================================
# Create a subscription for a user
# =============================================================
def create_subscription(conn, user_id, plan_id):
    subscription = Subscription(
        user_id=user_id,
        plan_id=plan_id,
        status="active",
        started_at=datetime.now()
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO subscriptions (user_id, plan_id, status, started_at)
            VALUES (%s, %s, %s, %s)
            RETURNING subscription_id
            """,
            (
                subscription.user_id,
                subscription.plan_id,
                subscription.status,
                subscription.started_at
            )
        )
        subscription_id = cur.fetchone()[0]
        conn.commit()

    logger.info(f"Created subscription {subscription_id} for user {user_id}")
    return subscription_id


# =============================================================
# Create a payment for a subscription
# =============================================================
def create_payment(conn, subscription_id, amount):
    status = random.choices(
        ["success", "failed", "pending"],
        weights=[80, 15, 5]  # 80% success, 15% failed, 5% pending
    )[0]

    payment = Payment(
        subscription_id=subscription_id,
        amount=amount,
        status=status,
        created_at=datetime.now()
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments (subscription_id, amount, status, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING payment_id
            """,
            (
                payment.subscription_id,
                payment.amount,
                payment.status,
                payment.created_at
            )
        )
        payment_id = cur.fetchone()[0]
        conn.commit()

    logger.info(f"Payment {payment_id} — {status} — ${amount}")
    return payment_id, status


# =============================================================
# Retry a failed payment
# Inserts a new payment row for the same subscription
# This is Experiment 4 — duplicate event / double count scenario
# =============================================================
def retry_payment(conn, subscription_id, amount):
    logger.info(f"Retrying payment for subscription {subscription_id}")
    return create_payment(conn, subscription_id, amount)


# =============================================================
# Upgrade or downgrade a subscription
# Updates the subscription row — fires CDC UPDATE event
# =============================================================
def change_plan(conn, subscription_id, new_plan_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE subscriptions
            SET plan_id = %s
            WHERE subscription_id = %s
            """,
            (new_plan_id, subscription_id)
        )
        conn.commit()

    logger.info(f"Subscription {subscription_id} changed to plan {new_plan_id}")


# =============================================================
# Cancel a subscription
# Updates status and sets ended_at — fires CDC UPDATE event
# =============================================================
def cancel_subscription(conn, subscription_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE subscriptions
            SET status = 'cancelled', ended_at = %s
            WHERE subscription_id = %s
            """,
            (datetime.now(), subscription_id)
        )
        conn.commit()

    logger.info(f"Subscription {subscription_id} cancelled")


# =============================================================
# Generate a clickstream event for a user
# =============================================================
def create_event(conn, user_id):
    event_type = random.choice(EVENT_TYPES)
    prop_options = EVENT_PROPERTIES.get(event_type, {})

    # Build a random properties dict from the options
    properties = {
        key: random.choice(values)
        for key, values in prop_options.items()
    }

    event = Event(
        user_id=user_id,
        event_type=event_type,
        properties=properties,
        created_at=datetime.now()
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (user_id, event_type, properties, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (
                event.user_id,
                event.event_type,
                json.dumps(event.properties),
                event.created_at
            )
        )
        conn.commit()


# =============================================================
# Main simulation loop
# Runs continuously — simulates realistic business activity
# =============================================================
def run(batch_size=10, sleep_seconds=5):
    """
    batch_size    — number of new users created per cycle
    sleep_seconds — pause between cycles
    """
    logger.info("Starting data generator")
    conn = get_connection()

    # Seed plans first
    seed_plans(conn)
    plan_ids = get_plan_ids(conn)

    # Track active subscriptions for updates
    active_subscriptions = []  # list of (subscription_id, plan_id, amount)

    cycle = 0

    while True:
        cycle += 1
        logger.info(f"--- Cycle {cycle} ---")

        # 1. Create new users and their subscriptions
        for _ in range(batch_size):
            user_id, plan_id = create_user(conn, plan_ids)
            subscription_id = create_subscription(conn, user_id, plan_id)

            # Get the price for this plan
            amount = next(
                price for pid, price in plan_ids if pid == plan_id
            )

            # Initial payment
            payment_id, status = create_payment(conn, subscription_id, amount)

            # Retry if payment failed
            if status == "failed":
                retry_payment(conn, subscription_id, amount)

            # Track for future updates
            active_subscriptions.append((subscription_id, plan_id, amount))

            # Fire some clickstream events for this user
            for _ in range(random.randint(1, 5)):
                create_event(conn, user_id)

        # 2. Simulate activity on existing subscriptions
        if active_subscriptions:

            # Upgrade or downgrade 10% of active subscriptions
            upgrade_sample = random.sample(
                active_subscriptions,
                max(1, len(active_subscriptions) // 10)
            )
            for subscription_id, _, _ in upgrade_sample:
                new_plan_id, _ = random.choice(plan_ids)
                change_plan(conn, subscription_id, new_plan_id)

            # Cancel 5% of active subscriptions
            cancel_sample = random.sample(
                active_subscriptions,
                max(1, len(active_subscriptions) // 20)
            )
            for subscription_id, _, _ in cancel_sample:
                cancel_subscription(conn, subscription_id)
                active_subscriptions = [
                    s for s in active_subscriptions
                    if s[0] != subscription_id
                ]

        logger.info(f"Cycle {cycle} complete — sleeping {sleep_seconds}s")
        time.sleep(sleep_seconds)


# =============================================================
# Entry point
# =============================================================
if __name__ == "__main__":
    run(batch_size=10, sleep_seconds=5)