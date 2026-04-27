"""
Tests for data_generator/schemas.py and generator business logic.
No database required — tests the domain constants and dataclasses.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from decimal import Decimal
from datetime import datetime
from data_generator.schemas import (
    PLANS,
    EVENT_TYPES,
    SUBSCRIPTION_STATUSES,
    PAYMENT_STATUSES,
    EVENT_PROPERTIES,
    User,
    Subscription,
    Payment,
    Event,
)


class TestPlans:

    def test_plans_not_empty(self):
        assert len(PLANS) > 0

    def test_all_plans_have_required_fields(self):
        for plan in PLANS:
            assert "name" in plan
            assert "price" in plan
            assert "billing_cycle" in plan

    def test_billing_cycle_values(self):
        valid = {"monthly", "annual"}
        for plan in PLANS:
            assert plan["billing_cycle"] in valid

    def test_price_is_positive(self):
        for plan in PLANS:
            assert plan["price"] > 0

    def test_annual_price_greater_than_monthly(self):
        """Annual plan for same tier should cost more than monthly."""
        for name in ["starter", "growth", "pro"]:
            monthly = next(p["price"] for p in PLANS if p["name"] == name and p["billing_cycle"] == "monthly")
            annual = next(p["price"] for p in PLANS if p["name"] == name and p["billing_cycle"] == "annual")
            assert annual > monthly


class TestEventTypes:

    def test_event_types_not_empty(self):
        assert len(EVENT_TYPES) > 0

    def test_all_event_types_have_properties(self):
        """Every event type should have a properties definition."""
        for event_type in EVENT_TYPES:
            assert event_type in EVENT_PROPERTIES, f"{event_type} missing from EVENT_PROPERTIES"

    def test_properties_values_are_lists(self):
        for event_type, props in EVENT_PROPERTIES.items():
            for key, values in props.items():
                assert isinstance(values, list), f"{event_type}.{key} should be a list"
                assert len(values) > 0


class TestStatusConstants:

    def test_subscription_statuses(self):
        assert "active" in SUBSCRIPTION_STATUSES
        assert "cancelled" in SUBSCRIPTION_STATUSES

    def test_payment_statuses(self):
        assert "success" in PAYMENT_STATUSES
        assert "failed" in PAYMENT_STATUSES
        assert "refunded" in PAYMENT_STATUSES
        assert "pending" in PAYMENT_STATUSES


class TestDataclasses:

    def test_user_creation(self):
        user = User(
            email="phil@test.com",
            name="Phil Dinh",
            plan_id=1,
            created_at=datetime.now(),
        )
        assert user.email == "phil@test.com"
        assert user.plan_id == 1

    def test_subscription_creation(self):
        sub = Subscription(
            user_id=1,
            plan_id=1,
            status="active",
            started_at=datetime.now(),
        )
        assert sub.status == "active"
        assert sub.ended_at is None

    def test_payment_creation(self):
        payment = Payment(
            subscription_id=1,
            amount=Decimal("9.99"),
            status="success",
            created_at=datetime.now(),
        )
        assert payment.amount == Decimal("9.99")

    def test_event_creation(self):
        event = Event(
            user_id=1,
            event_type="page_view",
            properties={"page": "pricing"},
            created_at=datetime.now(),
        )
        assert event.event_type == "page_view"
        assert event.properties["page"] == "pricing"
