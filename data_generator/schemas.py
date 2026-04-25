# =============================================================
# schemas.py
# Defines data shapes for synthetic data generation
# Used by generator.py to insert realistic data into oltp_db
# =============================================================

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


# -------------------------------------------------------------
# Plans
# Subscription tiers — seeded once at startup
# -------------------------------------------------------------
PLANS = [
    {"name": "starter",  "price": Decimal("9.99"),  "billing_cycle": "monthly"},
    {"name": "growth",   "price": Decimal("29.99"), "billing_cycle": "monthly"},
    {"name": "pro",      "price": Decimal("79.99"), "billing_cycle": "monthly"},
    {"name": "starter",  "price": Decimal("99.99"), "billing_cycle": "annual"},
    {"name": "growth",   "price": Decimal("299.99"),"billing_cycle": "annual"},
    {"name": "pro",      "price": Decimal("799.99"),"billing_cycle": "annual"},
]


# -------------------------------------------------------------
# Event types
# Clickstream events that fire against the events table
# -------------------------------------------------------------
EVENT_TYPES = [
    "page_view",
    "button_click",
    "feature_used",
    "plan_viewed",
    "checkout_started",
    "checkout_abandoned",
    "support_ticket_opened",
    "login",
    "logout",
]


# -------------------------------------------------------------
# Subscription statuses
# Valid values matching the CHECK constraint in oltp schema
# -------------------------------------------------------------
SUBSCRIPTION_STATUSES = ["active", "cancelled", "paused", "expired"]


# -------------------------------------------------------------
# Payment statuses
# Valid values matching the CHECK constraint in oltp schema
# -------------------------------------------------------------
PAYMENT_STATUSES = ["success", "failed", "refunded", "pending"]


# -------------------------------------------------------------
# Dataclasses
# Typed shapes for each table — generator uses these
# to build rows before inserting into Postgres
# -------------------------------------------------------------

@dataclass
class User:
    email: str
    name: str
    plan_id: int
    created_at: datetime


@dataclass
class Subscription:
    user_id: int
    plan_id: int
    status: str
    started_at: datetime
    ended_at: Optional[datetime] = None


@dataclass
class Payment:
    subscription_id: int
    amount: Decimal
    status: str
    created_at: datetime


@dataclass
class Event:
    user_id: int
    event_type: str
    properties: dict
    created_at: datetime


# -------------------------------------------------------------
# Event properties
# Realistic JSON payloads per event type
# Flattened by dbt in stg_events.sql later
# -------------------------------------------------------------
EVENT_PROPERTIES = {
    "page_view": {
        "page": ["home", "pricing", "features", "blog", "docs"],
        "referrer": ["google", "direct", "twitter", "linkedin", None],
    },
    "button_click": {
        "button": ["upgrade_now", "start_trial", "contact_sales", "view_pricing"],
        "page": ["home", "pricing", "features"],
    },
    "feature_used": {
        "feature": ["export_csv", "api_access", "custom_report", "team_invite"],
        "duration_seconds": [10, 30, 60, 120, 300],
    },
    "plan_viewed": {
        "plan": ["starter", "growth", "pro"],
        "billing_cycle": ["monthly", "annual"],
    },
    "checkout_started": {
        "plan": ["starter", "growth", "pro"],
        "billing_cycle": ["monthly", "annual"],
    },
    "checkout_abandoned": {
        "plan": ["starter", "growth", "pro"],
        "step": ["payment", "confirmation", "address"],
    },
    "support_ticket_opened": {
        "category": ["billing", "technical", "account", "feature_request"],
        "priority": ["low", "medium", "high"],
    },
    "login": {
        "method": ["email", "google", "github"],
        "device": ["desktop", "mobile", "tablet"],
    },
    "logout": {
        "device": ["desktop", "mobile", "tablet"],
    },
}